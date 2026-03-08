use crate::GenerateCoursesCommand;
use crate::migrate::{migrate_content_db, open_db};
use anyhow::Result;
use linguaforge_core::workspace::WorkspaceLayout;
use rusqlite::{OptionalExtension, Transaction, params};

pub async fn run_generate_courses(
    workspace: &WorkspaceLayout,
    command: GenerateCoursesCommand,
) -> Result<()> {
    migrate_content_db(workspace)?;
    let mut conn = open_db(&workspace.content_db_path())?;
    let tx = conn.transaction()?;

    if command.replace {
        tx.execute("DELETE FROM course_templates WHERE auto_generated = 1", [])?;
    }

    let ja_language_id = lookup_language_id(&tx, "ja")?;
    let en_language_id = lookup_language_id(&tx, "en")?;
    let unit_size = i64::from(command.unit_size.max(5));

    for (level, label) in [(5_i64, "N5"), (4, "N4"), (3, "N3"), (2, "N2"), (1, "N1")] {
        let item_ids = select_lexeme_ids(
            &tx,
            "SELECT id FROM lexemes WHERE language_id = ?1 AND jlpt_level = ?2 ORDER BY frequency_rank IS NULL, frequency_rank, quality_score DESC, id",
            params![ja_language_id, level],
        )?;
        if item_ids.is_empty() {
            continue;
        }

        upsert_course(
            &tx,
            CourseSpec {
                course_key: format!("ja-jlpt-{label}"),
                language_id: ja_language_id,
                name: format!("Japanese JLPT {label}"),
                description: format!(
                    "JLPT {label} vocabulary ordered by frequency and data quality."
                ),
                category: "exam".to_string(),
                target_exam: Some(format!("JLPT-{label}")),
                target_domain: None,
                difficulty_start: Some(level as i32),
                difficulty_end: Some(level as i32),
                item_type: "lexeme".to_string(),
                unit_prefix: format!("{label} Unit"),
            },
            &item_ids,
            unit_size,
        )?;
    }

    let ja_kanji_ids = select_kanji_ids(
        &tx,
        "SELECT id FROM kanji ORDER BY jlpt_level IS NULL, jlpt_level DESC, grade IS NULL, grade, frequency_rank IS NULL, frequency_rank, id",
        params![],
    )?;
    if !ja_kanji_ids.is_empty() {
        upsert_course(
            &tx,
            CourseSpec {
                course_key: "ja-kanji-core".to_string(),
                language_id: ja_language_id,
                name: "Japanese Kanji Core".to_string(),
                description: "Core kanji ordered by JLPT, school grade, and frequency.".to_string(),
                category: "foundation".to_string(),
                target_exam: Some("JLPT-Kanji".to_string()),
                target_domain: None,
                difficulty_start: Some(1),
                difficulty_end: Some(10),
                item_type: "kanji".to_string(),
                unit_prefix: "Kanji Set".to_string(),
            },
            &ja_kanji_ids,
            unit_size,
        )?;
    }

    let english_a = select_lexeme_ids(
        &tx,
        "SELECT id FROM lexemes WHERE language_id = ?1 AND cefr_level IN ('A1','A2') ORDER BY frequency_rank IS NULL, frequency_rank, quality_score DESC, id",
        params![en_language_id],
    )?;
    if !english_a.is_empty() {
        upsert_course(
            &tx,
            CourseSpec {
                course_key: "en-core-a1-a2".to_string(),
                language_id: en_language_id,
                name: "English Core A1-A2".to_string(),
                description: "Foundational English lexemes for beginner study.".to_string(),
                category: "foundation".to_string(),
                target_exam: Some("CEFR-A1-A2".to_string()),
                target_domain: None,
                difficulty_start: Some(1),
                difficulty_end: Some(2),
                item_type: "lexeme".to_string(),
                unit_prefix: "A Course".to_string(),
            },
            &english_a,
            unit_size,
        )?;
    }

    let english_b = select_lexeme_ids(
        &tx,
        "SELECT id FROM lexemes WHERE language_id = ?1 AND cefr_level IN ('B1','B2','C1') ORDER BY cefr_level, frequency_rank IS NULL, frequency_rank, quality_score DESC, id",
        params![en_language_id],
    )?;
    if !english_b.is_empty() {
        upsert_course(
            &tx,
            CourseSpec {
                course_key: "en-core-b1-c1".to_string(),
                language_id: en_language_id,
                name: "English Core B1-C1".to_string(),
                description: "Intermediate to advanced English core lexemes.".to_string(),
                category: "foundation".to_string(),
                target_exam: Some("CEFR-B1-C1".to_string()),
                target_domain: None,
                difficulty_start: Some(3),
                difficulty_end: Some(6),
                item_type: "lexeme".to_string(),
                unit_prefix: "B Course".to_string(),
            },
            &english_b,
            unit_size,
        )?;
    }

    let oxford_ids = select_lexeme_ids(
        &tx,
        "
        SELECT DISTINCT l.id
        FROM lexemes l
        JOIN lexeme_tag_map m ON m.lexeme_id = l.id
        JOIN tags t ON t.id = m.tag_id
        WHERE l.language_id = ?1 AND t.code LIKE 'list:oxford%'
        ORDER BY l.frequency_rank IS NULL, l.frequency_rank, l.quality_score DESC, l.id
        ",
        params![en_language_id],
    )?;
    if !oxford_ids.is_empty() {
        upsert_course(
            &tx,
            CourseSpec {
                course_key: "en-oxford-core".to_string(),
                language_id: en_language_id,
                name: "English Oxford Core".to_string(),
                description: "Oxford 3000 and 5000 words merged into a single study course."
                    .to_string(),
                category: "foundation".to_string(),
                target_exam: Some("Oxford".to_string()),
                target_domain: None,
                difficulty_start: Some(1),
                difficulty_end: Some(6),
                item_type: "lexeme".to_string(),
                unit_prefix: "Oxford Unit".to_string(),
            },
            &oxford_ids,
            unit_size,
        )?;
    }

    let ngsl_ids = select_lexeme_ids(
        &tx,
        "
        SELECT DISTINCT l.id
        FROM lexemes l
        JOIN lexeme_tag_map m ON m.lexeme_id = l.id
        JOIN tags t ON t.id = m.tag_id
        WHERE l.language_id = ?1 AND t.code = 'list:ngsl'
        ORDER BY l.frequency_rank IS NULL, l.frequency_rank, l.quality_score DESC, l.id
        ",
        params![en_language_id],
    )?;
    if !ngsl_ids.is_empty() {
        upsert_course(
            &tx,
            CourseSpec {
                course_key: "en-ngsl-core".to_string(),
                language_id: en_language_id,
                name: "English NGSL Core".to_string(),
                description: "New General Service List ordered by frequency.".to_string(),
                category: "foundation".to_string(),
                target_exam: Some("NGSL".to_string()),
                target_domain: None,
                difficulty_start: Some(1),
                difficulty_end: Some(4),
                item_type: "lexeme".to_string(),
                unit_prefix: "NGSL Unit".to_string(),
            },
            &ngsl_ids,
            unit_size,
        )?;
    }

    tx.commit()?;
    println!("generated auto courses with unit size {}", unit_size);
    Ok(())
}

struct CourseSpec {
    course_key: String,
    language_id: i64,
    name: String,
    description: String,
    category: String,
    target_exam: Option<String>,
    target_domain: Option<String>,
    difficulty_start: Option<i32>,
    difficulty_end: Option<i32>,
    item_type: String,
    unit_prefix: String,
}

fn upsert_course(
    tx: &Transaction<'_>,
    spec: CourseSpec,
    item_ids: &[i64],
    unit_size: i64,
) -> Result<()> {
    let existing_id = tx
        .query_row(
            "SELECT id FROM course_templates WHERE course_key = ?1",
            params![spec.course_key],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;

    let course_id = if let Some(course_id) = existing_id {
        tx.execute(
            "
            UPDATE course_templates
            SET language_id = ?2,
                name = ?3,
                description = ?4,
                category = ?5,
                target_exam = ?6,
                target_domain = ?7,
                difficulty_start = ?8,
                difficulty_end = ?9,
                auto_generated = 1
            WHERE id = ?1
            ",
            params![
                course_id,
                spec.language_id,
                spec.name,
                spec.description,
                spec.category,
                spec.target_exam,
                spec.target_domain,
                spec.difficulty_start,
                spec.difficulty_end,
            ],
        )?;
        tx.execute(
            "DELETE FROM course_units WHERE course_id = ?1",
            params![course_id],
        )?;
        course_id
    } else {
        tx.execute(
            "
            INSERT INTO course_templates (
                language_id,
                course_key,
                name,
                description,
                category,
                target_exam,
                target_domain,
                difficulty_start,
                difficulty_end,
                auto_generated
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)
            ",
            params![
                spec.language_id,
                spec.course_key,
                spec.name,
                spec.description,
                spec.category,
                spec.target_exam,
                spec.target_domain,
                spec.difficulty_start,
                spec.difficulty_end,
            ],
        )?;
        tx.last_insert_rowid()
    };

    let mut unit_order = 1_i64;
    for chunk in item_ids.chunks(unit_size as usize) {
        let title = format!("{} {:02}", spec.unit_prefix, unit_order);
        tx.execute(
            "INSERT INTO course_units (course_id, unit_order, title, description) VALUES (?1, ?2, ?3, ?4)",
            params![course_id, unit_order, title, format!("{} items", chunk.len())],
        )?;
        let unit_id = tx.last_insert_rowid();

        for (item_order, item_id) in chunk.iter().enumerate() {
            tx.execute(
                "INSERT INTO unit_items (unit_id, item_type, item_id, item_order) VALUES (?1, ?2, ?3, ?4)",
                params![unit_id, spec.item_type, item_id, item_order as i64 + 1],
            )?;
        }

        unit_order += 1;
    }

    Ok(())
}

fn select_lexeme_ids<P: rusqlite::Params>(
    tx: &Transaction<'_>,
    sql: &str,
    params: P,
) -> Result<Vec<i64>> {
    let mut stmt = tx.prepare(sql)?;
    let rows = stmt.query_map(params, |row| row.get::<_, i64>(0))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn select_kanji_ids<P: rusqlite::Params>(
    tx: &Transaction<'_>,
    sql: &str,
    params: P,
) -> Result<Vec<i64>> {
    let mut stmt = tx.prepare(sql)?;
    let rows = stmt.query_map(params, |row| row.get::<_, i64>(0))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn lookup_language_id(tx: &Transaction<'_>, code: &str) -> Result<i64> {
    Ok(tx.query_row(
        "SELECT id FROM languages WHERE code = ?1",
        params![code],
        |row| row.get(0),
    )?)
}
