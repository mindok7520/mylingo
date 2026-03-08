use crate::PublishCommand;
use crate::migrate::{migrate_content_db, open_db};
use anyhow::{Context, Result, bail};
use linguaforge_core::sources::{SourceDefinition, SourceRegistry};
use linguaforge_core::staging::{
    StageManifest, StagedExample, StagedExampleLink, StagedKanji, StagedLexeme,
};
use linguaforge_core::workspace::WorkspaceLayout;
use rusqlite::{OptionalExtension, Transaction, params};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;

#[derive(Debug, Default, Clone, Copy)]
struct PublishStats {
    inserted_lexemes: u64,
    merged_lexemes: u64,
    inserted_senses: u64,
    linked_tags: u64,
    inserted_kanji: u64,
    inserted_examples: u64,
    linked_examples: u64,
}

#[derive(Default)]
struct LookupCache {
    language_ids: HashMap<String, i64>,
    pos_ids: HashMap<String, i64>,
    domain_ids: HashMap<String, i64>,
    register_ids: HashMap<String, i64>,
    tag_ids: HashMap<String, i64>,
    example_ids_by_source_ref: HashMap<String, i64>,
    lexeme_ids_by_key: HashMap<String, Vec<i64>>,
}

pub async fn run_publish(workspace: &WorkspaceLayout, command: PublishCommand) -> Result<()> {
    let registry = SourceRegistry::load_from_path(&workspace.registry_path())?;
    let targets = if command.all || command.sources.is_empty() {
        registry.enabled_sources().collect::<Vec<_>>()
    } else {
        registry.select(&command.sources)?
    };

    if targets.is_empty() {
        bail!("no sources selected");
    }

    migrate_content_db(workspace)?;
    let mut conn = open_db(&workspace.content_db_path())?;

    let mut total = PublishStats::default();
    let mut published_kanji = false;
    for source in targets {
        let manifest_path = workspace
            .staging_source_dir(&source.id, &source.version)
            .join("manifest.json");
        if !manifest_path.exists() {
            println!(
                "skipped {}: no staging manifest at {}",
                source.id,
                manifest_path.display()
            );
            continue;
        }

        let stats = publish_source(&mut conn, source, &manifest_path)?;
        total.inserted_lexemes += stats.inserted_lexemes;
        total.merged_lexemes += stats.merged_lexemes;
        total.inserted_senses += stats.inserted_senses;
        total.linked_tags += stats.linked_tags;
        total.inserted_kanji += stats.inserted_kanji;
        total.inserted_examples += stats.inserted_examples;
        total.linked_examples += stats.linked_examples;
        published_kanji |= stats.inserted_kanji > 0 || source.id == "kanjidic2";
        println!(
            "published {}: +{} lexemes, {} merged, +{} senses, +{} tags, +{} kanji, +{} examples, +{} links",
            source.id,
            stats.inserted_lexemes,
            stats.merged_lexemes,
            stats.inserted_senses,
            stats.linked_tags,
            stats.inserted_kanji,
            stats.inserted_examples,
            stats.linked_examples
        );
    }

    if published_kanji {
        rebuild_kanji_lexeme_links(&mut conn)?;
        println!("rebuilt kanji-lexeme links");
    }

    if !command.no_search_rebuild {
        rebuild_lexeme_search(&mut conn)?;
        println!("rebuilt lexeme search index");
    }

    println!(
        "publish total: +{} lexemes, {} merged, +{} senses, +{} tags, +{} kanji, +{} examples, +{} links",
        total.inserted_lexemes,
        total.merged_lexemes,
        total.inserted_senses,
        total.linked_tags,
        total.inserted_kanji,
        total.inserted_examples,
        total.linked_examples
    );

    Ok(())
}

fn publish_source(
    conn: &mut rusqlite::Connection,
    source: &SourceDefinition,
    manifest_path: &Path,
) -> Result<PublishStats> {
    let manifest = load_stage_manifest(manifest_path)?;
    let tx = conn.transaction()?;
    let source_db_id = ensure_data_source(&tx, source)?;
    let mut cache = LookupCache::default();
    let mut stats = PublishStats::default();

    for output in &manifest.outputs {
        let path = Path::new(&output.path);
        let file = File::open(path)
            .with_context(|| format!("failed to open staging output: {}", path.display()))?;
        let reader = BufReader::new(file);

        match output.kind.as_str() {
            "lexeme_jsonl" => {
                for line in reader.lines() {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }

                    let staged: StagedLexeme = serde_json::from_str(&line).with_context(|| {
                        format!(
                            "failed to parse staged lexeme JSON line from {}",
                            path.display()
                        )
                    })?;
                    if staged.record_kind == "lexeme_metadata" {
                        publish_lexeme_metadata(
                            &tx,
                            &mut cache,
                            source_db_id,
                            &staged,
                            &mut stats,
                        )?;
                    } else {
                        publish_lexeme(&tx, &mut cache, source_db_id, &staged, &mut stats)?;
                    }
                }
            }
            "kanji_jsonl" => {
                for line in reader.lines() {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }

                    let staged: StagedKanji = serde_json::from_str(&line).with_context(|| {
                        format!(
                            "failed to parse staged kanji JSON line from {}",
                            path.display()
                        )
                    })?;
                    publish_kanji(&tx, &mut cache, source_db_id, &staged, &mut stats)?;
                }
            }
            "example_jsonl" => {
                for line in reader.lines() {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }

                    let staged: StagedExample = serde_json::from_str(&line).with_context(|| {
                        format!(
                            "failed to parse staged example JSON line from {}",
                            path.display()
                        )
                    })?;
                    publish_example(&tx, &mut cache, source_db_id, &staged, &mut stats)?;
                }
            }
            "example_link_jsonl" => {
                for line in reader.lines() {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }

                    let staged: StagedExampleLink =
                        serde_json::from_str(&line).with_context(|| {
                            format!(
                                "failed to parse staged example link JSON line from {}",
                                path.display()
                            )
                        })?;
                    publish_example_link(&tx, &mut cache, &staged, &mut stats)?;
                }
            }
            _ => {}
        }
    }

    tx.commit()?;
    Ok(stats)
}

fn publish_lexeme(
    tx: &Transaction<'_>,
    cache: &mut LookupCache,
    source_db_id: i64,
    staged: &StagedLexeme,
    stats: &mut PublishStats,
) -> Result<()> {
    let language_id = lookup_language_id(tx, cache, &staged.language)?;
    let pos_code = staged.primary_pos.as_deref().unwrap_or("unknown");
    let pos_id = lookup_pos_id(tx, cache, pos_code)?;
    let register_id = match staged.register.as_deref() {
        Some(code) => Some(lookup_register_id(tx, cache, code)?),
        None => None,
    };

    let lexeme_quality = compute_lexeme_quality(staged);
    let existing = tx
        .query_row(
            "
            SELECT id, reading, pronunciation, frequency_rank, cefr_level, jlpt_level, register_id, quality_score
            FROM lexemes
            WHERE language_id = ?1 AND lemma_normalized = ?2 AND primary_pos_id = ?3
            ",
            params![language_id, staged.lemma_normalized, pos_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, f64>(7)?,
                ))
            },
        )
        .optional()?;

    let lexeme_id = if let Some((
        lexeme_id,
        current_reading,
        current_pronunciation,
        current_frequency,
        current_cefr,
        current_jlpt,
        current_register_id,
        current_quality,
    )) = existing
    {
        let merged_frequency =
            min_optional_i64(current_frequency, staged.frequency_rank.map(i64::from));
        let merged_quality = current_quality.max(lexeme_quality);

        tx.execute(
            "
            UPDATE lexemes
            SET lemma = ?2,
                display_form = ?3,
                reading = COALESCE(reading, ?4),
                pronunciation = COALESCE(pronunciation, ?5),
                frequency_rank = ?6,
                cefr_level = COALESCE(cefr_level, ?7),
                jlpt_level = COALESCE(jlpt_level, ?8),
                register_id = COALESCE(register_id, ?9),
                quality_score = ?10,
                is_ai_enriched = is_ai_enriched,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?1
            ",
            params![
                lexeme_id,
                staged.lemma,
                staged.display_form,
                staged.reading.as_deref().or(current_reading.as_deref()),
                staged
                    .pronunciation
                    .as_deref()
                    .or(current_pronunciation.as_deref()),
                merged_frequency,
                staged.cefr_level.as_deref().or(current_cefr.as_deref()),
                staged.jlpt_level.map(i64::from).or(current_jlpt),
                register_id.or(current_register_id),
                merged_quality,
            ],
        )?;
        stats.merged_lexemes += 1;
        lexeme_id
    } else {
        tx.execute(
            "
            INSERT INTO lexemes (
                language_id,
                lemma,
                lemma_normalized,
                display_form,
                reading,
                pronunciation,
                primary_pos_id,
                frequency_rank,
                difficulty_level,
                cefr_level,
                jlpt_level,
                register_id,
                quality_score,
                is_ai_enriched,
                is_verified
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9, ?10, ?11, ?12, 0, 0)
            ",
            params![
                language_id,
                staged.lemma,
                staged.lemma_normalized,
                staged.display_form,
                staged.reading,
                staged.pronunciation,
                pos_id,
                staged.frequency_rank.map(i64::from),
                staged.cefr_level,
                staged.jlpt_level.map(i64::from),
                register_id,
                lexeme_quality,
            ],
        )?;
        stats.inserted_lexemes += 1;
        tx.last_insert_rowid()
    };

    if staged.gloss_en.is_some() || staged.gloss_ko.is_some() {
        let domain_id = match staged.domain.as_deref() {
            Some(code) => Some(lookup_domain_id(tx, cache, code)?),
            None => None,
        };
        let (sense_id, inserted_new_sense) =
            ensure_sense(tx, lexeme_id, staged, domain_id, register_id)?;
        tx.execute(
            "
            INSERT OR IGNORE INTO sense_sources (sense_id, source_id, source_ref, priority)
            VALUES (?1, ?2, ?3, 100)
            ",
            params![sense_id, source_db_id, staged_source_ref(staged)],
        )?;

        if inserted_new_sense {
            stats.inserted_senses += 1;
        }
    }

    for tag in &staged.tags {
        let tag_id = ensure_tag_id(tx, cache, tag)?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO lexeme_tag_map (lexeme_id, tag_id) VALUES (?1, ?2)",
            params![lexeme_id, tag_id],
        )?;
        stats.linked_tags += inserted as u64;
    }

    Ok(())
}

fn publish_lexeme_metadata(
    tx: &Transaction<'_>,
    cache: &mut LookupCache,
    source_db_id: i64,
    staged: &StagedLexeme,
    stats: &mut PublishStats,
) -> Result<()> {
    let language_id = lookup_language_id(tx, cache, &staged.language)?;
    let register_id = match staged.register.as_deref() {
        Some(code) => Some(lookup_register_id(tx, cache, code)?),
        None => None,
    };

    let mut stmt = tx.prepare(
        "
        SELECT id, frequency_rank, cefr_level, jlpt_level, register_id, quality_score
        FROM lexemes
        WHERE language_id = ?1 AND lemma_normalized = ?2
        ",
    )?;
    let rows = stmt.query_map(params![language_id, staged.lemma_normalized], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<i64>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, f64>(5)?,
        ))
    })?;

    let mut matched_ids = Vec::new();
    for row in rows {
        let (
            lexeme_id,
            current_frequency,
            current_cefr,
            current_jlpt,
            current_register_id,
            current_quality,
        ) = row?;
        tx.execute(
            "
            UPDATE lexemes
            SET frequency_rank = ?2,
                cefr_level = COALESCE(cefr_level, ?3),
                jlpt_level = COALESCE(jlpt_level, ?4),
                register_id = COALESCE(register_id, ?5),
                quality_score = ?6,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?1
            ",
            params![
                lexeme_id,
                min_optional_i64(current_frequency, staged.frequency_rank.map(i64::from)),
                staged.cefr_level.as_deref().or(current_cefr.as_deref()),
                staged.jlpt_level.map(i64::from).or(current_jlpt),
                register_id.or(current_register_id),
                current_quality.max(compute_lexeme_quality(staged)),
            ],
        )?;

        for tag in &staged.tags {
            let tag_id = ensure_tag_id(tx, cache, tag)?;
            let inserted = tx.execute(
                "INSERT OR IGNORE INTO lexeme_tag_map (lexeme_id, tag_id) VALUES (?1, ?2)",
                params![lexeme_id, tag_id],
            )?;
            stats.linked_tags += inserted as u64;
        }

        matched_ids.push(lexeme_id);
    }

    if matched_ids.is_empty() {
        publish_lexeme(tx, cache, source_db_id, staged, stats)?;
    } else {
        stats.merged_lexemes += matched_ids.len() as u64;
    }

    Ok(())
}

fn publish_kanji(
    tx: &Transaction<'_>,
    cache: &mut LookupCache,
    source_db_id: i64,
    staged: &StagedKanji,
    stats: &mut PublishStats,
) -> Result<()> {
    let en_language_id = lookup_language_id(tx, cache, "en")?;
    let existing_id = tx
        .query_row(
            "SELECT id FROM kanji WHERE character = ?1",
            params![staged.character],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;

    let kanji_id = if let Some(kanji_id) = existing_id {
        tx.execute(
            "
            UPDATE kanji
            SET stroke_count = COALESCE(stroke_count, ?2),
                grade = COALESCE(grade, ?3),
                jlpt_level = COALESCE(jlpt_level, ?4),
                frequency_rank = COALESCE(frequency_rank, ?5),
                radical = COALESCE(radical, ?6),
                svg_path = COALESCE(svg_path, ?7)
            WHERE id = ?1
            ",
            params![
                kanji_id,
                staged.stroke_count.map(i64::from),
                staged.grade.map(i64::from),
                staged.jlpt_level.map(i64::from),
                staged.frequency_rank.map(i64::from),
                staged.radical,
                staged.svg_path,
            ],
        )?;
        kanji_id
    } else {
        tx.execute(
            "
            INSERT INTO kanji (character, stroke_count, grade, jlpt_level, frequency_rank, radical, svg_path)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ",
            params![
                staged.character,
                staged.stroke_count.map(i64::from),
                staged.grade.map(i64::from),
                staged.jlpt_level.map(i64::from),
                staged.frequency_rank.map(i64::from),
                staged.radical,
                staged.svg_path,
            ],
        )?;
        stats.inserted_kanji += 1;
        tx.last_insert_rowid()
    };

    for reading in &staged.readings_on {
        tx.execute(
            "INSERT OR IGNORE INTO kanji_readings (kanji_id, reading_type, reading_text) VALUES (?1, 'on', ?2)",
            params![kanji_id, reading],
        )?;
    }
    for reading in &staged.readings_kun {
        tx.execute(
            "INSERT OR IGNORE INTO kanji_readings (kanji_id, reading_type, reading_text) VALUES (?1, 'kun', ?2)",
            params![kanji_id, reading],
        )?;
    }
    for reading in &staged.readings_nanori {
        tx.execute(
            "INSERT OR IGNORE INTO kanji_readings (kanji_id, reading_type, reading_text) VALUES (?1, 'nanori', ?2)",
            params![kanji_id, reading],
        )?;
    }
    for meaning in &staged.meanings_en {
        tx.execute(
            "INSERT OR IGNORE INTO kanji_meanings (kanji_id, language_id, meaning_text) VALUES (?1, ?2, ?3)",
            params![kanji_id, en_language_id, meaning],
        )?;
    }

    let _ = source_db_id;
    Ok(())
}

fn publish_example(
    tx: &Transaction<'_>,
    cache: &mut LookupCache,
    source_db_id: i64,
    staged: &StagedExample,
    stats: &mut PublishStats,
) -> Result<()> {
    let language_id = lookup_language_id(tx, cache, &staged.language)?;
    let domain_id = match staged.domain.as_deref() {
        Some(code) => Some(lookup_domain_id(tx, cache, code)?),
        None => None,
    };

    let existing_id = tx
        .query_row(
            "SELECT id FROM examples WHERE sentence_normalized = ?1",
            params![staged.sentence_normalized],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;

    let example_id = if let Some(example_id) = existing_id {
        tx.execute(
            "
            UPDATE examples
            SET sentence_reading = COALESCE(sentence_reading, ?2),
                difficulty_level = COALESCE(difficulty_level, ?3),
                domain_id = COALESCE(domain_id, ?4),
                quality_score = MAX(quality_score, ?5)
            WHERE id = ?1
            ",
            params![
                example_id,
                staged.sentence_reading,
                staged.difficulty_level.map(i64::from),
                domain_id,
                compute_example_quality(staged),
            ],
        )?;
        example_id
    } else {
        tx.execute(
            "
            INSERT INTO examples (language_id, sentence, sentence_normalized, sentence_reading, difficulty_level, domain_id, quality_score)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ",
            params![
                language_id,
                staged.sentence,
                staged.sentence_normalized,
                staged.sentence_reading,
                staged.difficulty_level.map(i64::from),
                domain_id,
                compute_example_quality(staged),
            ],
        )?;
        stats.inserted_examples += 1;
        tx.last_insert_rowid()
    };

    tx.execute(
        "INSERT OR IGNORE INTO example_sources (example_id, source_id, source_ref) VALUES (?1, ?2, ?3)",
        params![example_id, source_db_id, staged.source_ref],
    )?;

    if let (Some(language_code), Some(text)) = (
        staged.translation_language.as_deref(),
        staged.translation_text.as_deref(),
    ) {
        let translation_language_id = lookup_language_id(tx, cache, language_code)?;
        tx.execute(
            "
            INSERT OR IGNORE INTO example_translations (example_id, language_id, translation_text, is_primary)
            VALUES (?1, ?2, ?3, 1)
            ",
            params![example_id, translation_language_id, text],
        )?;
    }

    if let Some(source_ref) = staged.source_ref.as_ref() {
        cache
            .example_ids_by_source_ref
            .insert(source_ref.clone(), example_id);
    }

    Ok(())
}

fn publish_example_link(
    tx: &Transaction<'_>,
    cache: &mut LookupCache,
    staged: &StagedExampleLink,
    stats: &mut PublishStats,
) -> Result<()> {
    let example_id = lookup_example_id_by_source_ref(tx, cache, &staged.example_source_ref)?;
    let Some(example_id) = example_id else {
        return Ok(());
    };

    let lexeme_ids =
        lookup_matching_lexeme_ids(tx, cache, &staged.lexeme_language, &staged.lemma_normalized)?;

    for lexeme_id in lexeme_ids {
        let inserted = tx.execute(
            "
            INSERT OR IGNORE INTO lexeme_examples (lexeme_id, example_id, highlight_start, highlight_end, match_score)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ",
            params![
                lexeme_id,
                example_id,
                staged.highlight_start.map(i64::from),
                staged.highlight_end.map(i64::from),
                staged.match_score,
            ],
        )?;
        stats.linked_examples += inserted as u64;
    }

    Ok(())
}

fn ensure_sense(
    tx: &Transaction<'_>,
    lexeme_id: i64,
    staged: &StagedLexeme,
    domain_id: Option<i64>,
    register_id: Option<i64>,
) -> Result<(i64, bool)> {
    let gloss_en = staged.gloss_en.as_deref().unwrap_or("");
    let gloss_ko = staged.gloss_ko.as_deref().unwrap_or("");

    if let Some(existing_id) = tx
        .query_row(
            "
            SELECT id
            FROM lexeme_senses
            WHERE lexeme_id = ?1
              AND COALESCE(gloss_en, '') = ?2
              AND COALESCE(gloss_ko, '') = ?3
            ",
            params![lexeme_id, gloss_en, gloss_ko],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
    {
        tx.execute(
            "
            UPDATE lexeme_senses
            SET domain_id = COALESCE(domain_id, ?2),
                register_id = COALESCE(register_id, ?3),
                quality_score = MAX(quality_score, ?4)
            WHERE id = ?1
            ",
            params![
                existing_id,
                domain_id,
                register_id,
                compute_sense_quality(staged)
            ],
        )?;
        return Ok((existing_id, false));
    }

    tx.execute(
        "
        INSERT INTO lexeme_senses (
            lexeme_id,
            sense_order,
            gloss_ko,
            gloss_en,
            gloss_detail,
            domain_id,
            register_id,
            quality_score
        )
        SELECT ?1,
               COALESCE(MAX(sense_order), 0) + 1,
               ?2,
               ?3,
               NULL,
               ?4,
               ?5,
               ?6
        FROM lexeme_senses
        WHERE lexeme_id = ?1
        ",
        params![
            lexeme_id,
            staged.gloss_ko,
            staged.gloss_en,
            domain_id,
            register_id,
            compute_sense_quality(staged),
        ],
    )?;
    Ok((tx.last_insert_rowid(), true))
}

fn ensure_data_source(tx: &Transaction<'_>, source: &SourceDefinition) -> Result<i64> {
    tx.execute(
        "
        INSERT INTO data_sources (source_key, name, version, homepage_url, license, imported_at)
        VALUES (?1, ?2, ?3, ?4, ?5, CURRENT_TIMESTAMP)
        ON CONFLICT(source_key) DO UPDATE SET
            name = excluded.name,
            version = excluded.version,
            homepage_url = excluded.homepage_url,
            license = excluded.license,
            imported_at = CURRENT_TIMESTAMP
        ",
        params![
            source.id,
            source.name,
            source.version,
            source.homepage,
            source.license,
        ],
    )?;

    let id = tx.query_row(
        "SELECT id FROM data_sources WHERE source_key = ?1",
        params![source.id],
        |row| row.get(0),
    )?;
    Ok(id)
}

fn lookup_language_id(tx: &Transaction<'_>, cache: &mut LookupCache, code: &str) -> Result<i64> {
    lookup_lookup_table_id(tx, &mut cache.language_ids, "languages", "code", code)
}

fn lookup_pos_id(tx: &Transaction<'_>, cache: &mut LookupCache, code: &str) -> Result<i64> {
    lookup_lookup_table_id(tx, &mut cache.pos_ids, "pos_tags", "code", code)
        .or_else(|_| lookup_lookup_table_id(tx, &mut cache.pos_ids, "pos_tags", "code", "unknown"))
}

fn lookup_domain_id(tx: &Transaction<'_>, cache: &mut LookupCache, code: &str) -> Result<i64> {
    lookup_lookup_table_id(tx, &mut cache.domain_ids, "domains", "code", code)
}

fn lookup_register_id(tx: &Transaction<'_>, cache: &mut LookupCache, code: &str) -> Result<i64> {
    lookup_lookup_table_id(tx, &mut cache.register_ids, "registers", "code", code)
}

fn ensure_tag_id(tx: &Transaction<'_>, cache: &mut LookupCache, code: &str) -> Result<i64> {
    if let Some(id) = cache.tag_ids.get(code) {
        return Ok(*id);
    }

    tx.execute(
        "
        INSERT INTO tags (code, display_name)
        VALUES (?1, ?2)
        ON CONFLICT(code) DO UPDATE SET display_name = excluded.display_name
        ",
        params![code, humanize_code(code)],
    )?;

    let id = tx.query_row(
        "SELECT id FROM tags WHERE code = ?1",
        params![code],
        |row| row.get(0),
    )?;
    cache.tag_ids.insert(code.to_string(), id);
    Ok(id)
}

fn lookup_example_id_by_source_ref(
    tx: &Transaction<'_>,
    cache: &mut LookupCache,
    source_ref: &str,
) -> Result<Option<i64>> {
    if let Some(id) = cache.example_ids_by_source_ref.get(source_ref) {
        return Ok(Some(*id));
    }

    let id = tx
        .query_row(
            "
            SELECT e.id
            FROM examples e
            JOIN example_sources s ON s.example_id = e.id
            WHERE s.source_ref = ?1
            LIMIT 1
            ",
            params![source_ref],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;

    if let Some(id) = id {
        cache
            .example_ids_by_source_ref
            .insert(source_ref.to_string(), id);
    }

    Ok(id)
}

fn lookup_matching_lexeme_ids(
    tx: &Transaction<'_>,
    cache: &mut LookupCache,
    language_code: &str,
    lemma_normalized: &str,
) -> Result<Vec<i64>> {
    let cache_key = format!("{language_code}:{lemma_normalized}");
    if let Some(ids) = cache.lexeme_ids_by_key.get(&cache_key) {
        return Ok(ids.clone());
    }

    let language_id = lookup_language_id(tx, cache, language_code)?;
    let ids = {
        let mut stmt =
            tx.prepare("SELECT id FROM lexemes WHERE language_id = ?1 AND lemma_normalized = ?2")?;
        stmt.query_map(params![language_id, lemma_normalized], |row| {
            row.get::<_, i64>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
    };

    cache.lexeme_ids_by_key.insert(cache_key, ids.clone());
    Ok(ids)
}

fn lookup_lookup_table_id(
    tx: &Transaction<'_>,
    cache: &mut HashMap<String, i64>,
    table: &str,
    column: &str,
    code: &str,
) -> Result<i64> {
    if let Some(id) = cache.get(code) {
        return Ok(*id);
    }

    let sql = format!("SELECT id FROM {table} WHERE {column} = ?1");
    let id = tx
        .query_row(&sql, params![code], |row| row.get(0))
        .with_context(|| format!("missing lookup value '{}' in {}", code, table))?;
    cache.insert(code.to_string(), id);
    Ok(id)
}

fn rebuild_lexeme_search(conn: &mut rusqlite::Connection) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM lexeme_search", [])?;
    tx.execute(
        "
        INSERT INTO lexeme_search (lexeme_id, surface, reading, gloss_ko, gloss_en)
        SELECT
            l.id,
            l.display_form,
            COALESCE(l.reading, ''),
            COALESCE(GROUP_CONCAT(DISTINCT s.gloss_ko), ''),
            COALESCE(GROUP_CONCAT(DISTINCT s.gloss_en), '')
        FROM lexemes l
        LEFT JOIN lexeme_senses s ON s.lexeme_id = l.id
        GROUP BY l.id
        ",
        [],
    )?;
    tx.commit()?;
    Ok(())
}

fn rebuild_kanji_lexeme_links(conn: &mut rusqlite::Connection) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM kanji_lexemes", [])?;

    let ja_language_id: i64 =
        tx.query_row("SELECT id FROM languages WHERE code = 'ja'", [], |row| {
            row.get(0)
        })?;

    let kanji_pairs = {
        let mut kanji_stmt = tx.prepare("SELECT id, character FROM kanji")?;
        kanji_stmt
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    let kanji_map: HashMap<char, i64> = kanji_pairs
        .into_iter()
        .filter_map(|(id, text)| text.chars().next().map(|ch| (ch, id)))
        .collect();

    let lexemes = {
        let mut lexeme_stmt =
            tx.prepare("SELECT id, display_form FROM lexemes WHERE language_id = ?1")?;
        lexeme_stmt
            .query_map(params![ja_language_id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };

    for (lexeme_id, display_form) in lexemes {
        for (index, ch) in display_form.chars().enumerate() {
            if let Some(kanji_id) = kanji_map.get(&ch) {
                tx.execute(
                    "INSERT OR IGNORE INTO kanji_lexemes (kanji_id, lexeme_id, position_index) VALUES (?1, ?2, ?3)",
                    params![kanji_id, lexeme_id, index as i64],
                )?;
            }
        }
    }

    tx.commit()?;
    Ok(())
}

fn load_stage_manifest(path: &Path) -> Result<StageManifest> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read stage manifest: {}", path.display()))?;
    let manifest = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse stage manifest: {}", path.display()))?;
    Ok(manifest)
}

fn staged_source_ref(staged: &StagedLexeme) -> Option<String> {
    staged.source_ref.clone().or_else(|| {
        staged
            .metadata
            .get("ent_seq")
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .or_else(|| {
                staged
                    .metadata
                    .get("line_number")
                    .and_then(|value| value.as_u64().map(|number| number.to_string()))
            })
    })
}

fn compute_lexeme_quality(staged: &StagedLexeme) -> f64 {
    let mut score = 0.05_f64;
    if staged.reading.is_some() {
        score += 0.15;
    }
    if staged.pronunciation.is_some() {
        score += 0.05;
    }
    if staged.gloss_en.is_some() || staged.gloss_ko.is_some() {
        score += 0.3;
    }
    if staged.frequency_rank.is_some() {
        score += 0.15;
    }
    if staged.cefr_level.is_some() || staged.jlpt_level.is_some() {
        score += 0.15;
    }
    if !staged.tags.is_empty() {
        score += 0.1;
    }
    if staged.domain.is_some() {
        score += 0.05;
    }
    score.min(1.0)
}

fn compute_sense_quality(staged: &StagedLexeme) -> f64 {
    let mut score = 0.1_f64;
    if staged.gloss_en.is_some() {
        score += 0.35;
    }
    if staged.gloss_ko.is_some() {
        score += 0.25;
    }
    if staged.domain.is_some() {
        score += 0.1;
    }
    if staged.register.is_some() {
        score += 0.1;
    }
    score.min(1.0)
}

fn compute_example_quality(staged: &StagedExample) -> f64 {
    let mut score = 0.15_f64;
    if !staged.sentence.is_empty() {
        score += 0.35;
    }
    if staged.translation_text.is_some() {
        score += 0.25;
    }
    if staged.domain.is_some() {
        score += 0.1;
    }
    if staged.difficulty_level.is_some() {
        score += 0.05;
    }
    score.min(1.0)
}

fn min_optional_i64(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn humanize_code(code: &str) -> String {
    code.split(['-', '_'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}
