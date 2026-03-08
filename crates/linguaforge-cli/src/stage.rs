use crate::StageCommand;
use anyhow::{Context, Result, bail};
use csv::ReaderBuilder;
use linguaforge_core::sources::{ExtractMode, SourceDefinition, SourceRegistry};
use linguaforge_core::staging::{
    StageManifest, StageOutput, StagedExample, StagedExampleLink, StagedKanji, StagedLexeme,
};
use linguaforge_core::workspace::WorkspaceLayout;
use quick_xml::Reader;
use quick_xml::events::Event;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tracing::info;
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Default)]
struct JmdictEntry {
    ent_seq: String,
    kanji_forms: Vec<String>,
    reading_forms: Vec<String>,
    priority_codes: Vec<String>,
    senses: Vec<JmdictSense>,
}

#[derive(Debug, Default, Clone)]
struct JmdictSense {
    pos_codes: Vec<String>,
    field_codes: Vec<String>,
    misc_codes: Vec<String>,
    glosses: Vec<String>,
}

#[derive(Debug, Default)]
struct KanjidicCharacter {
    literal: String,
    stroke_count: Option<u32>,
    grade: Option<u32>,
    jlpt_level: Option<u8>,
    frequency_rank: Option<u32>,
    radical: Option<String>,
    readings_on: Vec<String>,
    readings_kun: Vec<String>,
    readings_nanori: Vec<String>,
    meanings_en: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct TatoebaExample {
    id: String,
    text: String,
    translation: String,
    #[serde(default)]
    words: Vec<TatoebaWord>,
}

#[derive(Debug, Deserialize)]
struct TatoebaWord {
    headword: String,
    #[serde(default)]
    reading: Option<String>,
    #[serde(default, rename = "surfaceForm")]
    surface_form: Option<String>,
}

pub async fn run_stage(workspace: &WorkspaceLayout, command: StageCommand) -> Result<()> {
    let registry = SourceRegistry::load_from_path(&workspace.registry_path())?;
    let targets = if command.all || command.sources.is_empty() {
        registry.enabled_sources().collect::<Vec<_>>()
    } else {
        registry.select(&command.sources)?
    };

    if targets.is_empty() {
        bail!("no sources selected");
    }

    for source in targets {
        match stage_source(workspace, source, command.force)? {
            Some(manifest) => println!(
                "staged {} -> {} output(s)",
                manifest.source_id,
                manifest.outputs.len()
            ),
            None => println!("stage not implemented yet for {}", source.id),
        }
    }

    Ok(())
}

fn stage_source(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    force: bool,
) -> Result<Option<StageManifest>> {
    match source.id.as_str() {
        "google-10000" => stage_google_10000(workspace, source, force).map(Some),
        "ngsl" => stage_ngsl(workspace, source, force).map(Some),
        "jmdict" => stage_jmdict(workspace, source, force).map(Some),
        "kanjidic2" => stage_kanjidic2(workspace, source, force).map(Some),
        "jlpt-vocabulary" => stage_jlpt_vocabulary(workspace, source, force).map(Some),
        "oxford-word-list" => stage_oxford_word_list(workspace, source, force).map(Some),
        "tatoeba-jpn-eng" => stage_tatoeba_examples(workspace, source, force).map(Some),
        _ => Ok(None),
    }
}

fn stage_google_10000(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    force: bool,
) -> Result<StageManifest> {
    stage_english_word_list(
        workspace,
        source,
        force,
        EnglishListSpec {
            parser: EnglishListParser::PlainText,
            list_tag: "google-10000",
            core_tag: "frequency-list",
            cefr_from_rank: false,
        },
    )
}

fn stage_ngsl(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    force: bool,
) -> Result<StageManifest> {
    stage_english_word_list(
        workspace,
        source,
        force,
        EnglishListSpec {
            parser: EnglishListParser::CsvHeadwordDefinition,
            list_tag: "ngsl",
            core_tag: "core-list",
            cefr_from_rank: true,
        },
    )
}

fn stage_jmdict(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    force: bool,
) -> Result<StageManifest> {
    info!(source = %source.id, "staging source");

    let input_path = resolved_asset_path(workspace, source, 0)?;
    if !input_path.exists() {
        bail!(
            "raw asset missing for '{}'. run `cargo run -p linguaforge-cli -- fetch {}` first",
            source.id,
            source.id
        );
    }

    let output_dir = workspace.staging_source_dir(&source.id, &source.version);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create staging dir: {}", output_dir.display()))?;
    let output_path = output_dir.join("lexemes.jsonl");

    if let Some(cached) = load_cached_manifest(&output_dir, &output_path, force)? {
        return Ok(cached);
    }

    let file = File::open(&input_path)
        .with_context(|| format!("failed to open JMdict XML: {}", input_path.display()))?;
    let mut reader = Reader::from_reader(BufReader::new(file));
    reader.config_mut().trim_text(true);

    let output = File::create(&output_path)
        .with_context(|| format!("failed to create staging output: {}", output_path.display()))?;
    let mut writer = BufWriter::new(output);

    let mut records = 0_u64;
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) if event.name().as_ref() == b"entry" => {
                let entry = parse_jmdict_entry(&mut reader)?;
                for staged in jmdict_entry_to_lexemes(source, entry) {
                    serde_json::to_writer(&mut writer, &staged)?;
                    writer.write_all(b"\n")?;
                    records += 1;
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    writer.flush()?;
    write_stage_manifest(source, &output_path, records)
}

fn stage_kanjidic2(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    force: bool,
) -> Result<StageManifest> {
    info!(source = %source.id, "staging source");

    let input_path = resolved_asset_path(workspace, source, 0)?;
    if !input_path.exists() {
        bail!(
            "raw asset missing for '{}'. run `cargo run -p linguaforge-cli -- fetch {}` first",
            source.id,
            source.id
        );
    }

    let output_dir = workspace.staging_source_dir(&source.id, &source.version);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create staging dir: {}", output_dir.display()))?;
    let output_path = output_dir.join("kanji.jsonl");

    if let Some(cached) = load_cached_manifest(&output_dir, &output_path, force)? {
        return Ok(cached);
    }

    let file = File::open(&input_path)
        .with_context(|| format!("failed to open KANJIDIC2 XML: {}", input_path.display()))?;
    let mut reader = Reader::from_reader(BufReader::new(file));
    reader.config_mut().trim_text(true);

    let output = File::create(&output_path)
        .with_context(|| format!("failed to create staging output: {}", output_path.display()))?;
    let mut writer = BufWriter::new(output);
    let mut records = 0_u64;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) if event.name().as_ref() == b"character" => {
                let staged =
                    kanjidic_character_to_staged(source, parse_kanjidic_character(&mut reader)?);
                serde_json::to_writer(&mut writer, &staged)?;
                writer.write_all(b"\n")?;
                records += 1;
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    writer.flush()?;
    write_stage_manifest_with_outputs(
        source,
        vec![StageOutput {
            kind: "kanji_jsonl".to_string(),
            path: output_path.display().to_string(),
            records,
        }],
    )
}

fn stage_jlpt_vocabulary(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    force: bool,
) -> Result<StageManifest> {
    info!(source = %source.id, "staging source");

    let archive_root = resolved_asset_path(workspace, source, 0)?;
    let input_path = archive_root
        .join("JLPT_Vocabulary-main")
        .join("data")
        .join("results")
        .join("JLPTWords.csv");
    if !input_path.exists() {
        bail!("JLPT vocabulary CSV missing at {}", input_path.display());
    }

    let output_dir = workspace.staging_source_dir(&source.id, &source.version);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create staging dir: {}", output_dir.display()))?;
    let output_path = output_dir.join("lexeme_metadata.jsonl");

    if let Some(cached) = load_cached_manifest(&output_dir, &output_path, force)? {
        return Ok(cached);
    }

    let file = File::open(&input_path)
        .with_context(|| format!("failed to open JLPT CSV: {}", input_path.display()))?;
    let mut csv_reader = ReaderBuilder::new().from_reader(file);
    let output = File::create(&output_path)
        .with_context(|| format!("failed to create staging output: {}", output_path.display()))?;
    let mut writer = BufWriter::new(output);
    let mut records = 0_u64;

    for (index, record) in csv_reader.records().enumerate() {
        let record = record?;
        let lemma = record.get(0).unwrap_or("").trim();
        let level_text = record.get(1).unwrap_or("").trim();
        if lemma.is_empty() || level_text.is_empty() {
            continue;
        }

        let jlpt_level = level_text.trim_start_matches('N').parse::<u8>().ok();
        let staged = StagedLexeme {
            source_id: source.id.clone(),
            source_version: source.version.clone(),
            language: source.language.clone(),
            record_kind: "lexeme_metadata".to_string(),
            source_ref: Some((index + 2).to_string()),
            lemma: lemma.to_string(),
            lemma_normalized: normalize_japanese_lemma(lemma),
            display_form: lemma.to_string(),
            reading: None,
            pronunciation: None,
            primary_pos: Some("unknown".to_string()),
            gloss_en: None,
            gloss_ko: None,
            frequency_rank: None,
            cefr_level: None,
            jlpt_level,
            domain: Some("general".to_string()),
            register: Some("neutral".to_string()),
            tags: vec![
                "list:jlpt-vocabulary".to_string(),
                format!("jlpt:{level_text}"),
            ],
            metadata: json!({
                "row_number": index + 2,
                "raw_level": level_text,
            }),
        };

        serde_json::to_writer(&mut writer, &staged)?;
        writer.write_all(b"\n")?;
        records += 1;
    }

    writer.flush()?;
    write_stage_manifest_with_outputs(
        source,
        vec![StageOutput {
            kind: "lexeme_jsonl".to_string(),
            path: output_path.display().to_string(),
            records,
        }],
    )
}

fn stage_oxford_word_list(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    force: bool,
) -> Result<StageManifest> {
    info!(source = %source.id, "staging source");

    let archive_root = resolved_asset_path(workspace, source, 0)?;
    let base = archive_root.join("English-Vocabulary-Word-List-master");
    let files = [
        (base.join("Oxford 3000.txt"), "oxford-3000"),
        (base.join("Oxford 5000.txt"), "oxford-5000"),
    ];

    let output_dir = workspace.staging_source_dir(&source.id, &source.version);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create staging dir: {}", output_dir.display()))?;
    let output_path = output_dir.join("lexeme_metadata.jsonl");

    if let Some(cached) = load_cached_manifest(&output_dir, &output_path, force)? {
        return Ok(cached);
    }

    let output = File::create(&output_path)
        .with_context(|| format!("failed to create staging output: {}", output_path.display()))?;
    let mut writer = BufWriter::new(output);
    let mut seen = HashSet::new();
    let mut records = 0_u64;

    for (path, list_tag) in files {
        let file = File::open(&path)
            .with_context(|| format!("failed to open Oxford list: {}", path.display()))?;
        let reader = BufReader::new(file);

        for (index, line) in reader.lines().enumerate() {
            let lemma = line?;
            let lemma = lemma.trim();
            if lemma.is_empty() {
                continue;
            }

            let normalized = normalize_english_lemma(lemma);
            if normalized.is_empty() {
                continue;
            }

            let key = format!("{list_tag}:{normalized}");
            if !seen.insert(key) {
                continue;
            }

            let staged = StagedLexeme {
                source_id: source.id.clone(),
                source_version: source.version.clone(),
                language: source.language.clone(),
                record_kind: "lexeme_metadata".to_string(),
                source_ref: Some(format!("{list_tag}:{}", index + 1)),
                lemma: lemma.to_string(),
                lemma_normalized: normalized,
                display_form: lemma.to_string(),
                reading: None,
                pronunciation: None,
                primary_pos: Some("unknown".to_string()),
                gloss_en: None,
                gloss_ko: None,
                frequency_rank: None,
                cefr_level: None,
                jlpt_level: None,
                domain: Some("general".to_string()),
                register: Some("neutral".to_string()),
                tags: vec![format!("list:{list_tag}"), "list:oxford".to_string()],
                metadata: json!({
                    "file": path.file_name().and_then(|name| name.to_str()).unwrap_or_default(),
                    "line_number": index + 1,
                }),
            };

            serde_json::to_writer(&mut writer, &staged)?;
            writer.write_all(b"\n")?;
            records += 1;
        }
    }

    writer.flush()?;
    write_stage_manifest_with_outputs(
        source,
        vec![StageOutput {
            kind: "lexeme_jsonl".to_string(),
            path: output_path.display().to_string(),
            records,
        }],
    )
}

fn stage_tatoeba_examples(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    force: bool,
) -> Result<StageManifest> {
    info!(source = %source.id, "staging source");

    let input_path = resolved_asset_path(workspace, source, 0)?.join("jpn-eng-examples.json");
    if !input_path.exists() {
        bail!("Tatoeba JSON missing at {}", input_path.display());
    }

    let output_dir = workspace.staging_source_dir(&source.id, &source.version);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create staging dir: {}", output_dir.display()))?;
    let examples_path = output_dir.join("examples.jsonl");
    let links_path = output_dir.join("example_links.jsonl");

    if !force && examples_path.exists() && links_path.exists() {
        let manifest_path = output_dir.join("manifest.json");
        if manifest_path.exists() {
            let manifest_raw = fs::read_to_string(&manifest_path).with_context(|| {
                format!("failed to read stage manifest: {}", manifest_path.display())
            })?;
            let manifest: StageManifest =
                serde_json::from_str(&manifest_raw).with_context(|| {
                    format!(
                        "failed to parse stage manifest: {}",
                        manifest_path.display()
                    )
                })?;
            return Ok(manifest);
        }
    }

    let file = File::open(&input_path)
        .with_context(|| format!("failed to open Tatoeba JSON: {}", input_path.display()))?;
    let examples: Vec<TatoebaExample> = serde_json::from_reader(BufReader::new(file))
        .with_context(|| format!("failed to parse Tatoeba JSON: {}", input_path.display()))?;

    let examples_output = File::create(&examples_path).with_context(|| {
        format!(
            "failed to create staging output: {}",
            examples_path.display()
        )
    })?;
    let links_output = File::create(&links_path)
        .with_context(|| format!("failed to create staging output: {}", links_path.display()))?;
    let mut examples_writer = BufWriter::new(examples_output);
    let mut links_writer = BufWriter::new(links_output);
    let mut example_records = 0_u64;
    let mut link_records = 0_u64;

    for example in examples {
        let staged_example = StagedExample {
            source_id: source.id.clone(),
            source_version: source.version.clone(),
            source_ref: Some(example.id.clone()),
            language: source.language.clone(),
            sentence_normalized: normalize_sentence(&example.text),
            sentence: example.text.clone(),
            sentence_reading: None,
            translation_language: Some("en".to_string()),
            translation_text: Some(example.translation.clone()),
            difficulty_level: None,
            domain: Some("daily".to_string()),
            metadata: json!({
                "word_count": example.words.len(),
            }),
        };
        serde_json::to_writer(&mut examples_writer, &staged_example)?;
        examples_writer.write_all(b"\n")?;
        example_records += 1;

        let mut seen = HashSet::new();
        for word in example.words {
            let lemma = word.headword.trim();
            if lemma.is_empty() {
                continue;
            }

            let lemma_normalized = normalize_japanese_lemma(lemma);
            if lemma_normalized.is_empty() || !seen.insert(lemma_normalized.clone()) {
                continue;
            }

            let surface = word
                .surface_form
                .clone()
                .filter(|value| !value.trim().is_empty());
            let highlight_text = surface.as_deref().unwrap_or(lemma);
            let (highlight_start, highlight_end) =
                find_char_range(&example.text, highlight_text).unwrap_or((0, 0));
            let reading = word.reading.filter(|value| {
                let value = value.trim();
                !value.is_empty() && !value.starts_with('#')
            });

            let staged_link = StagedExampleLink {
                source_id: source.id.clone(),
                source_version: source.version.clone(),
                source_ref: Some(format!("{}:{}", example.id, lemma_normalized)),
                example_source_ref: example.id.clone(),
                lexeme_language: source.language.clone(),
                lemma: lemma.to_string(),
                lemma_normalized,
                surface_form: surface,
                reading,
                highlight_start: Some(highlight_start),
                highlight_end: Some(highlight_end),
                match_score: 1.0,
                metadata: json!({}),
            };
            serde_json::to_writer(&mut links_writer, &staged_link)?;
            links_writer.write_all(b"\n")?;
            link_records += 1;
        }
    }

    examples_writer.flush()?;
    links_writer.flush()?;
    write_stage_manifest_with_outputs(
        source,
        vec![
            StageOutput {
                kind: "example_jsonl".to_string(),
                path: examples_path.display().to_string(),
                records: example_records,
            },
            StageOutput {
                kind: "example_link_jsonl".to_string(),
                path: links_path.display().to_string(),
                records: link_records,
            },
        ],
    )
}

fn stage_english_word_list(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    force: bool,
    spec: EnglishListSpec,
) -> Result<StageManifest> {
    info!(source = %source.id, "staging source");

    let input_path = resolved_asset_path(workspace, source, 0)?;
    if !input_path.exists() {
        bail!(
            "raw asset missing for '{}'. run `cargo run -p linguaforge-cli -- fetch {}` first",
            source.id,
            source.id
        );
    }

    let output_dir = workspace.staging_source_dir(&source.id, &source.version);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create staging dir: {}", output_dir.display()))?;
    let output_path = output_dir.join("lexemes.jsonl");

    if let Some(cached) = load_cached_manifest(&output_dir, &output_path, force)? {
        return Ok(cached);
    }

    let output = File::create(&output_path)
        .with_context(|| format!("failed to create staging output: {}", output_path.display()))?;
    let mut writer = BufWriter::new(output);
    let mut seen = HashSet::new();
    let mut records = 0_u64;

    match spec.parser {
        EnglishListParser::PlainText => {
            let file = File::open(&input_path)
                .with_context(|| format!("failed to open word list: {}", input_path.display()))?;
            let reader = BufReader::new(file);

            for (index, line) in reader.lines().enumerate() {
                let lemma = line?;
                let lemma = lemma.trim();
                if lemma.is_empty() {
                    continue;
                }

                let normalized = normalize_english_lemma(lemma);
                if normalized.is_empty() || !seen.insert(normalized.clone()) {
                    continue;
                }

                let staged = StagedLexeme {
                    source_id: source.id.clone(),
                    source_version: source.version.clone(),
                    language: source.language.clone(),
                    record_kind: "lexeme".to_string(),
                    source_ref: Some((index + 1).to_string()),
                    lemma: lemma.to_string(),
                    lemma_normalized: normalized,
                    display_form: lemma.to_string(),
                    reading: None,
                    pronunciation: None,
                    primary_pos: Some("unknown".to_string()),
                    gloss_en: None,
                    gloss_ko: None,
                    frequency_rank: Some((index + 1) as u32),
                    cefr_level: None,
                    jlpt_level: None,
                    domain: Some("general".to_string()),
                    register: Some("neutral".to_string()),
                    tags: vec![format!("list:{}", spec.list_tag), spec.core_tag.to_string()],
                    metadata: json!({
                        "line_number": index + 1,
                        "list": spec.list_tag,
                    }),
                };

                serde_json::to_writer(&mut writer, &staged)?;
                writer.write_all(b"\n")?;
                records += 1;
            }
        }
        EnglishListParser::CsvHeadwordDefinition => {
            let mut file = File::open(&input_path).with_context(|| {
                format!("failed to open CSV word list: {}", input_path.display())
            })?;
            let mut raw_bytes = Vec::new();
            file.read_to_end(&mut raw_bytes).with_context(|| {
                format!("failed to read CSV word list: {}", input_path.display())
            })?;
            let decoded = String::from_utf8_lossy(&raw_bytes).into_owned();
            let mut csv_reader = ReaderBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_reader(decoded.as_bytes());

            for (index, record) in csv_reader.records().enumerate() {
                let record = record?;
                let lemma = record.get(0).unwrap_or("").trim();
                if lemma.is_empty() {
                    continue;
                }

                let normalized = normalize_english_lemma(lemma);
                if normalized.is_empty() || !seen.insert(normalized.clone()) {
                    continue;
                }

                let frequency_rank = (index + 1) as u32;
                let staged = StagedLexeme {
                    source_id: source.id.clone(),
                    source_version: source.version.clone(),
                    language: source.language.clone(),
                    record_kind: "lexeme".to_string(),
                    source_ref: Some(frequency_rank.to_string()),
                    lemma: lemma.to_string(),
                    lemma_normalized: normalized,
                    display_form: lemma.to_string(),
                    reading: None,
                    pronunciation: None,
                    primary_pos: Some("unknown".to_string()),
                    gloss_en: record
                        .get(1)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned),
                    gloss_ko: None,
                    frequency_rank: Some(frequency_rank),
                    cefr_level: spec
                        .cefr_from_rank
                        .then(|| cefr_from_frequency_rank(frequency_rank).to_string()),
                    jlpt_level: None,
                    domain: Some("general".to_string()),
                    register: Some("neutral".to_string()),
                    tags: vec![format!("list:{}", spec.list_tag), spec.core_tag.to_string()],
                    metadata: json!({
                        "row_number": index + 1,
                        "column_count": record.len(),
                        "list": spec.list_tag,
                    }),
                };

                serde_json::to_writer(&mut writer, &staged)?;
                writer.write_all(b"\n")?;
                records += 1;
            }
        }
    }

    writer.flush()?;
    write_stage_manifest(source, &output_path, records)
}

fn load_cached_manifest(
    output_dir: &Path,
    output_path: &Path,
    force: bool,
) -> Result<Option<StageManifest>> {
    if !output_path.exists() || force {
        return Ok(None);
    }

    let manifest_path = output_dir.join("manifest.json");
    if !manifest_path.exists() {
        return Ok(None);
    }

    let manifest_raw = fs::read_to_string(&manifest_path)
        .with_context(|| format!("failed to read stage manifest: {}", manifest_path.display()))?;
    let manifest: StageManifest = serde_json::from_str(&manifest_raw).with_context(|| {
        format!(
            "failed to parse stage manifest: {}",
            manifest_path.display()
        )
    })?;
    Ok(Some(manifest))
}

fn write_stage_manifest(
    source: &SourceDefinition,
    output_path: &Path,
    records: u64,
) -> Result<StageManifest> {
    write_stage_manifest_with_outputs(
        source,
        vec![StageOutput {
            kind: "lexeme_jsonl".to_string(),
            path: output_path.display().to_string(),
            records,
        }],
    )
}

fn write_stage_manifest_with_outputs(
    source: &SourceDefinition,
    outputs: Vec<StageOutput>,
) -> Result<StageManifest> {
    let manifest = StageManifest {
        source_id: source.id.clone(),
        source_version: source.version.clone(),
        generated_at: chrono::Utc::now(),
        outputs,
    };

    let manifest_path = Path::new(&manifest.outputs[0].path)
        .parent()
        .context("staging output path has no parent")?
        .join("manifest.json");
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?).with_context(|| {
        format!(
            "failed to write stage manifest: {}",
            manifest_path.display()
        )
    })?;

    Ok(manifest)
}

fn resolved_asset_path(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    file_index: usize,
) -> Result<PathBuf> {
    let file = source
        .files
        .get(file_index)
        .context("source does not have expected asset index")?;
    let base_dir = workspace.raw_source_dir(&source.id, &source.version);

    let path = match file.extract {
        ExtractMode::SingleFile | ExtractMode::Archive => base_dir.join(file.extracted_label()?),
        ExtractMode::None => base_dir.join(file.download_name()?),
    };

    Ok(path)
}

fn parse_kanjidic_character<R: BufRead>(reader: &mut Reader<R>) -> Result<KanjidicCharacter> {
    let mut character = KanjidicCharacter::default();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) if event.name().as_ref() == b"literal" => {
                character.literal = read_text_node(reader, b"literal")?;
            }
            Event::Start(event) if event.name().as_ref() == b"radical" => {
                character.radical = parse_kanjidic_radical(reader)?;
            }
            Event::Start(event) if event.name().as_ref() == b"misc" => {
                parse_kanjidic_misc(reader, &mut character)?;
            }
            Event::Start(event) if event.name().as_ref() == b"reading_meaning" => {
                parse_kanjidic_reading_meaning(reader, &mut character)?;
            }
            Event::End(event) if event.name().as_ref() == b"character" => break,
            Event::Eof => bail!("unexpected EOF while parsing KANJIDIC2 character"),
            _ => {}
        }
        buf.clear();
    }

    Ok(character)
}

fn parse_kanjidic_radical<R: BufRead>(reader: &mut Reader<R>) -> Result<Option<String>> {
    let mut radical = None;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) if event.name().as_ref() == b"rad_value" => {
                let is_classical = event
                    .attributes()
                    .flatten()
                    .find(|attr| attr.key.as_ref() == b"rad_type")
                    .and_then(|attr| String::from_utf8(attr.value.into_owned()).ok())
                    .map(|value| value == "classical")
                    .unwrap_or(false);
                let value = read_text_node(reader, b"rad_value")?;
                if radical.is_none() || is_classical {
                    radical = Some(value);
                }
            }
            Event::End(event) if event.name().as_ref() == b"radical" => break,
            Event::Eof => bail!("unexpected EOF while parsing KANJIDIC2 radical"),
            _ => {}
        }
        buf.clear();
    }

    Ok(radical)
}

fn parse_kanjidic_misc<R: BufRead>(
    reader: &mut Reader<R>,
    character: &mut KanjidicCharacter,
) -> Result<()> {
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) if event.name().as_ref() == b"grade" => {
                character.grade = read_text_node(reader, b"grade")?.parse::<u32>().ok();
            }
            Event::Start(event) if event.name().as_ref() == b"stroke_count" => {
                if character.stroke_count.is_none() {
                    character.stroke_count =
                        read_text_node(reader, b"stroke_count")?.parse::<u32>().ok();
                } else {
                    let _ = read_text_node(reader, b"stroke_count")?;
                }
            }
            Event::Start(event) if event.name().as_ref() == b"freq" => {
                character.frequency_rank = read_text_node(reader, b"freq")?.parse::<u32>().ok();
            }
            Event::Start(event) if event.name().as_ref() == b"jlpt" => {
                character.jlpt_level = read_text_node(reader, b"jlpt")?.parse::<u8>().ok();
            }
            Event::Start(event) => {
                let end = event.name().as_ref().to_vec();
                let _ = read_text_node(reader, &end)?;
            }
            Event::End(event) if event.name().as_ref() == b"misc" => break,
            Event::Eof => bail!("unexpected EOF while parsing KANJIDIC2 misc"),
            _ => {}
        }
        buf.clear();
    }

    Ok(())
}

fn parse_kanjidic_reading_meaning<R: BufRead>(
    reader: &mut Reader<R>,
    character: &mut KanjidicCharacter,
) -> Result<()> {
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) if event.name().as_ref() == b"rmgroup" => {
                parse_kanjidic_rmgroup(reader, character)?;
            }
            Event::Start(event) if event.name().as_ref() == b"nanori" => {
                let value = read_text_node(reader, b"nanori")?;
                if !value.is_empty() {
                    character.readings_nanori.push(value);
                }
            }
            Event::End(event) if event.name().as_ref() == b"reading_meaning" => break,
            Event::Eof => bail!("unexpected EOF while parsing KANJIDIC2 reading_meaning"),
            _ => {}
        }
        buf.clear();
    }

    Ok(())
}

fn parse_kanjidic_rmgroup<R: BufRead>(
    reader: &mut Reader<R>,
    character: &mut KanjidicCharacter,
) -> Result<()> {
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) if event.name().as_ref() == b"reading" => {
                let reading_type = event
                    .attributes()
                    .flatten()
                    .find(|attr| attr.key.as_ref() == b"r_type")
                    .and_then(|attr| String::from_utf8(attr.value.into_owned()).ok())
                    .unwrap_or_default();
                let value = read_text_node(reader, b"reading")?;
                match reading_type.as_str() {
                    "ja_on" => character.readings_on.push(value),
                    "ja_kun" => character.readings_kun.push(value),
                    _ => {}
                }
            }
            Event::Start(event) if event.name().as_ref() == b"meaning" => {
                let language = event
                    .attributes()
                    .flatten()
                    .find(|attr| attr.key.as_ref() == b"m_lang")
                    .and_then(|attr| String::from_utf8(attr.value.into_owned()).ok())
                    .unwrap_or_else(|| "en".to_string());
                let value = read_text_node(reader, b"meaning")?;
                if language == "en" && !value.is_empty() {
                    character.meanings_en.push(value);
                }
            }
            Event::End(event) if event.name().as_ref() == b"rmgroup" => break,
            Event::Eof => bail!("unexpected EOF while parsing KANJIDIC2 rmgroup"),
            _ => {}
        }
        buf.clear();
    }

    Ok(())
}

fn kanjidic_character_to_staged(
    source: &SourceDefinition,
    character: KanjidicCharacter,
) -> StagedKanji {
    StagedKanji {
        source_id: source.id.clone(),
        source_version: source.version.clone(),
        source_ref: Some(character.literal.clone()),
        character: character.literal,
        stroke_count: character.stroke_count,
        grade: character.grade,
        jlpt_level: character.jlpt_level,
        frequency_rank: character.frequency_rank,
        radical: character.radical,
        svg_path: None,
        readings_on: character.readings_on,
        readings_kun: character.readings_kun,
        readings_nanori: character.readings_nanori,
        meanings_en: character.meanings_en,
        meanings_ko: Vec::new(),
        metadata: json!({}),
    }
}

fn parse_jmdict_entry<R: BufRead>(reader: &mut Reader<R>) -> Result<JmdictEntry> {
    let mut entry = JmdictEntry::default();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) if event.name().as_ref() == b"ent_seq" => {
                entry.ent_seq = read_text_node(reader, b"ent_seq")?;
            }
            Event::Start(event) if event.name().as_ref() == b"k_ele" => {
                let (text, priorities) = parse_jmdict_k_ele(reader)?;
                if let Some(text) = text {
                    entry.kanji_forms.push(text);
                }
                entry.priority_codes.extend(priorities);
            }
            Event::Start(event) if event.name().as_ref() == b"r_ele" => {
                let (text, priorities) = parse_jmdict_r_ele(reader)?;
                if let Some(text) = text {
                    entry.reading_forms.push(text);
                }
                entry.priority_codes.extend(priorities);
            }
            Event::Start(event) if event.name().as_ref() == b"sense" => {
                entry.senses.push(parse_jmdict_sense(reader)?);
            }
            Event::End(event) if event.name().as_ref() == b"entry" => break,
            Event::Eof => bail!("unexpected EOF while parsing JMdict entry"),
            _ => {}
        }
        buf.clear();
    }

    Ok(entry)
}

fn parse_jmdict_k_ele<R: BufRead>(reader: &mut Reader<R>) -> Result<(Option<String>, Vec<String>)> {
    let mut text = None;
    let mut priorities = Vec::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) if event.name().as_ref() == b"keb" => {
                text = Some(read_text_node(reader, b"keb")?);
            }
            Event::Start(event) if event.name().as_ref() == b"ke_pri" => {
                priorities.push(read_text_node(reader, b"ke_pri")?);
            }
            Event::Start(event) if event.name().as_ref() == b"ke_inf" => {
                let _ = read_text_node(reader, b"ke_inf")?;
            }
            Event::End(event) if event.name().as_ref() == b"k_ele" => break,
            Event::Eof => bail!("unexpected EOF while parsing JMdict k_ele"),
            _ => {}
        }
        buf.clear();
    }

    Ok((text, priorities))
}

fn parse_jmdict_r_ele<R: BufRead>(reader: &mut Reader<R>) -> Result<(Option<String>, Vec<String>)> {
    let mut text = None;
    let mut priorities = Vec::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) if event.name().as_ref() == b"reb" => {
                text = Some(read_text_node(reader, b"reb")?);
            }
            Event::Start(event) if event.name().as_ref() == b"re_pri" => {
                priorities.push(read_text_node(reader, b"re_pri")?);
            }
            Event::Start(event)
                if matches!(
                    event.name().as_ref(),
                    b"re_inf" | b"re_nokanji" | b"re_restr"
                ) =>
            {
                let end = event.name().as_ref().to_vec();
                let _ = read_text_node(reader, &end)?;
            }
            Event::End(event) if event.name().as_ref() == b"r_ele" => break,
            Event::Eof => bail!("unexpected EOF while parsing JMdict r_ele"),
            _ => {}
        }
        buf.clear();
    }

    Ok((text, priorities))
}

fn parse_jmdict_sense<R: BufRead>(reader: &mut Reader<R>) -> Result<JmdictSense> {
    let mut sense = JmdictSense::default();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(event) if event.name().as_ref() == b"pos" => {
                sense.pos_codes.push(read_text_node(reader, b"pos")?);
            }
            Event::Start(event) if event.name().as_ref() == b"field" => {
                sense.field_codes.push(read_text_node(reader, b"field")?);
            }
            Event::Start(event) if event.name().as_ref() == b"misc" => {
                sense.misc_codes.push(read_text_node(reader, b"misc")?);
            }
            Event::Start(event) if event.name().as_ref() == b"gloss" => {
                sense.glosses.push(read_text_node(reader, b"gloss")?);
            }
            Event::Start(event)
                if matches!(
                    event.name().as_ref(),
                    b"xref" | b"ant" | b"s_inf" | b"dial" | b"lsource" | b"stagk" | b"stagr"
                ) =>
            {
                let end = event.name().as_ref().to_vec();
                let _ = read_text_node(reader, &end)?;
            }
            Event::End(event) if event.name().as_ref() == b"sense" => break,
            Event::Eof => bail!("unexpected EOF while parsing JMdict sense"),
            _ => {}
        }
        buf.clear();
    }

    Ok(sense)
}

fn read_text_node<R: BufRead>(reader: &mut Reader<R>, end: &[u8]) -> Result<String> {
    let mut buf = Vec::new();
    let mut text = String::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Text(event) => {
                text.push_str(&String::from_utf8_lossy(event.as_ref()));
            }
            Event::CData(event) => {
                text.push_str(&String::from_utf8_lossy(event.as_ref()));
            }
            Event::GeneralRef(event) => {
                text.push('&');
                text.push_str(&String::from_utf8_lossy(event.as_ref()));
                text.push(';');
            }
            Event::End(event) if event.name().as_ref() == end => break,
            Event::Eof => bail!("unexpected EOF while reading XML text node"),
            _ => {}
        }
        buf.clear();
    }

    Ok(text.trim().to_string())
}

fn jmdict_entry_to_lexemes(source: &SourceDefinition, entry: JmdictEntry) -> Vec<StagedLexeme> {
    if entry.reading_forms.is_empty() && entry.kanji_forms.is_empty() {
        return Vec::new();
    }

    let ent_seq = entry.ent_seq.clone();
    let kanji_forms = entry.kanji_forms.clone();
    let reading_forms = entry.reading_forms.clone();
    let priority_codes = entry.priority_codes.clone();
    let display_form = entry
        .kanji_forms
        .first()
        .cloned()
        .or_else(|| entry.reading_forms.first().cloned())
        .unwrap_or_default();
    let reading = entry.reading_forms.first().cloned();
    let lemma_normalized = normalize_japanese_lemma(&display_form);
    let frequency_rank = jmdict_frequency_rank(&entry.priority_codes);

    let mut previous_pos_codes: Vec<String> = Vec::new();
    let mut output = Vec::new();

    for (sense_index, sense) in entry.senses.into_iter().enumerate() {
        let raw_pos_codes = if sense.pos_codes.is_empty() {
            previous_pos_codes.clone()
        } else {
            previous_pos_codes = sense.pos_codes.clone();
            sense.pos_codes.clone()
        };

        let decoded_pos_codes = raw_pos_codes
            .iter()
            .map(|code| decode_jmdict_entity(code))
            .collect::<Vec<_>>();
        let primary_pos = decoded_pos_codes
            .first()
            .map(|code| map_jmdict_pos(code).to_string())
            .or_else(|| Some("unknown".to_string()));

        let field_tags = sense
            .field_codes
            .iter()
            .map(|code| decode_jmdict_entity(code))
            .collect::<Vec<_>>();
        let misc_tags = sense
            .misc_codes
            .iter()
            .map(|code| decode_jmdict_entity(code))
            .collect::<Vec<_>>();

        let mut tags = vec!["source:jmdict".to_string(), "lang:ja".to_string()];
        tags.extend(field_tags.iter().map(|code| format!("field:{code}")));
        tags.extend(misc_tags.iter().map(|code| format!("misc:{code}")));
        dedupe_preserve_order(&mut tags);

        output.push(StagedLexeme {
            source_id: source.id.clone(),
            source_version: source.version.clone(),
            language: source.language.clone(),
            record_kind: "lexeme".to_string(),
            source_ref: Some(ent_seq.clone()),
            lemma: display_form.clone(),
            lemma_normalized: lemma_normalized.clone(),
            display_form: display_form.clone(),
            reading: reading.clone(),
            pronunciation: reading.clone(),
            primary_pos,
            gloss_en: if sense.glosses.is_empty() {
                None
            } else {
                Some(sense.glosses.join("; "))
            },
            gloss_ko: None,
            frequency_rank,
            cefr_level: None,
            jlpt_level: None,
            domain: infer_domain_from_jmdict_fields(&field_tags),
            register: infer_register_from_jmdict_misc(&misc_tags),
            tags,
            metadata: json!({
                "ent_seq": ent_seq.clone(),
                "sense_index": sense_index + 1,
                "kanji_forms": kanji_forms.clone(),
                "reading_forms": reading_forms.clone(),
                "priority_codes": priority_codes.clone(),
                "raw_pos_codes": decoded_pos_codes,
                "field_codes": field_tags,
                "misc_codes": misc_tags,
            }),
        });
    }

    output
}

fn normalize_english_lemma(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut last_was_separator = false;

    for ch in input.trim().chars() {
        let ch = ch.to_ascii_lowercase();
        let keep = ch.is_ascii_alphanumeric() || matches!(ch, '-' | '\'' | ' ');
        if !keep {
            continue;
        }

        if ch == ' ' {
            if !last_was_separator {
                output.push(ch);
                last_was_separator = true;
            }
        } else {
            output.push(ch);
            last_was_separator = false;
        }
    }

    output.trim().to_string()
}

fn normalize_japanese_lemma(input: &str) -> String {
    input.nfkc().collect::<String>().trim().to_string()
}

fn normalize_sentence(input: &str) -> String {
    input
        .nfkc()
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn find_char_range(haystack: &str, needle: &str) -> Option<(u32, u32)> {
    let start_byte = haystack.find(needle)?;
    let start_chars = haystack[..start_byte].chars().count() as u32;
    let end_chars = start_chars + needle.chars().count() as u32;
    Some((start_chars, end_chars))
}

fn cefr_from_frequency_rank(rank: u32) -> &'static str {
    match rank {
        1..=500 => "A1",
        501..=1000 => "A2",
        1001..=1500 => "B1",
        1501..=2000 => "B2",
        _ => "C1",
    }
}

fn decode_jmdict_entity(value: &str) -> String {
    value
        .trim()
        .trim_start_matches('&')
        .trim_end_matches(';')
        .to_string()
}

fn map_jmdict_pos(code: &str) -> &'static str {
    match code {
        "v1" => "verb-ichidan",
        code if code.starts_with("v5") => "verb-godan",
        code if code.starts_with("vs") => "verb-suru",
        code if code.starts_with("vk") || code.starts_with("vz") => "verb",
        "adj-i" => "adjective-i",
        "adj-na" => "adjective-na",
        "adj-no" => "adjective-no",
        "adj-pn" => "adjective-prenoun",
        code if code.starts_with("adj") => "adjective",
        "adv" | "adv-to" => "adverb",
        "exp" => "expression",
        "prt" => "particle",
        "pn" => "pronoun",
        "ctr" => "counter",
        "conj" => "conjunction",
        "int" => "interjection",
        "pref" => "prefix",
        "suf" => "suffix",
        "aux" | "aux-v" | "aux-adj" => "auxiliary",
        code if code.starts_with("n") => "noun",
        _ => "unknown",
    }
}

fn infer_domain_from_jmdict_fields(fields: &[String]) -> Option<String> {
    for code in fields {
        let domain = match code.as_str() {
            "comp" => Some("tech"),
            "bus" | "econ" | "finance" => Some("business"),
            "med" => Some("medical"),
            "law" => Some("legal"),
            "food" => Some("food"),
            "sports" | "baseb" | "sumo" | "martial arts" => Some("sports"),
            _ => None,
        };
        if let Some(domain) = domain {
            return Some(domain.to_string());
        }
    }

    Some("general".to_string())
}

fn infer_register_from_jmdict_misc(misc: &[String]) -> Option<String> {
    for code in misc {
        let register = match code.as_str() {
            "col" => Some("colloquial"),
            "sl" => Some("slang"),
            "hon" => Some("honorific"),
            "hum" => Some("humble"),
            "arch" | "obs" | "obsc" => Some("archaic"),
            "pol" | "form" => Some("formal"),
            _ => None,
        };
        if let Some(register) = register {
            return Some(register.to_string());
        }
    }

    Some("neutral".to_string())
}

fn jmdict_frequency_rank(priority_codes: &[String]) -> Option<u32> {
    let mut best: Option<u32> = None;
    for raw in priority_codes {
        let code = decode_jmdict_entity(raw);
        let rank = if let Some(rest) = code.strip_prefix("nf") {
            rest.parse::<u32>()
                .ok()
                .map(|group| group.saturating_sub(1) * 500 + 1)
        } else {
            match code.as_str() {
                "news1" => Some(500),
                "news2" => Some(1000),
                "ichi1" | "spec1" | "gai1" => Some(1500),
                "ichi2" | "spec2" | "gai2" => Some(3000),
                _ => None,
            }
        };

        if let Some(rank) = rank {
            best = Some(best.map_or(rank, |current| current.min(rank)));
        }
    }
    best
}

fn dedupe_preserve_order(values: &mut Vec<String>) {
    let mut seen = HashSet::new();
    values.retain(|value| seen.insert(value.clone()));
}

#[derive(Clone, Copy)]
struct EnglishListSpec {
    parser: EnglishListParser,
    list_tag: &'static str,
    core_tag: &'static str,
    cefr_from_rank: bool,
}

#[derive(Clone, Copy)]
enum EnglishListParser {
    PlainText,
    CsvHeadwordDefinition,
}

#[cfg(test)]
mod tests {
    use super::{decode_jmdict_entity, map_jmdict_pos, normalize_english_lemma};

    #[test]
    fn normalizes_basic_ascii() {
        assert_eq!(normalize_english_lemma("  Running  "), "running");
    }

    #[test]
    fn drops_symbols_but_keeps_inner_dash() {
        assert_eq!(normalize_english_lemma("co-op!"), "co-op");
    }

    #[test]
    fn decodes_jmdict_entity_references() {
        assert_eq!(decode_jmdict_entity("&adj-na;"), "adj-na");
    }

    #[test]
    fn maps_jmdict_pos_to_internal_code() {
        assert_eq!(map_jmdict_pos("v1"), "verb-ichidan");
        assert_eq!(map_jmdict_pos("n"), "noun");
    }
}
