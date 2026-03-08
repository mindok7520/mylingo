use chrono::{Duration, Utc};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

const DEFAULT_PROFILE_KEY: &str = "default";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchLexeme {
    id: i64,
    language: String,
    display_form: String,
    reading: Option<String>,
    part_of_speech: String,
    gloss_en: Option<String>,
    gloss_ko: Option<String>,
    frequency_rank: Option<i64>,
    jlpt_level: Option<i64>,
    cefr_level: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LexemeSense {
    sense_order: i64,
    gloss_en: Option<String>,
    gloss_ko: Option<String>,
    gloss_detail: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LexemeExample {
    id: i64,
    sentence: String,
    sentence_reading: Option<String>,
    translation_en: Option<String>,
    match_score: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LexemeKanji {
    character: String,
    grade: Option<i64>,
    jlpt_level: Option<i64>,
    frequency_rank: Option<i64>,
    meanings: Vec<String>,
    onyomi: Vec<String>,
    kunyomi: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LexemeDetail {
    id: i64,
    language: String,
    lemma: String,
    display_form: String,
    reading: Option<String>,
    part_of_speech: String,
    frequency_rank: Option<i64>,
    jlpt_level: Option<i64>,
    cefr_level: Option<String>,
    quality_score: f64,
    senses: Vec<LexemeSense>,
    examples: Vec<LexemeExample>,
    kanji: Vec<LexemeKanji>,
    tags: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ActiveSession {
    id: i64,
    mode: String,
    started_at: String,
    course_key: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DashboardSnapshot {
    profile_key: String,
    due_reviews: i64,
    new_items: i64,
    total_review_items: i64,
    review_events_today: i64,
    course_templates: i64,
    lexeme_count: i64,
    kanji_count: i64,
    active_session: Option<ActiveSession>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StudyStartOption {
    course_key: String,
    language: String,
    name: String,
    description: Option<String>,
    category: String,
    level_label: String,
    recommended_reason: String,
    unit_count: i64,
    item_count: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CourseMap {
    course_key: String,
    name: String,
    description: Option<String>,
    level_label: String,
    recommended_reason: String,
    units: Vec<CourseMapUnit>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CourseMapUnit {
    unit_order: i64,
    title: String,
    total_items: i64,
    learned_count: i64,
    reviewed_count: i64,
    is_completed: bool,
    is_current: bool,
    is_locked: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReviewQueueItem {
    review_item_id: i64,
    lexeme_id: i64,
    display_form: String,
    reading: Option<String>,
    gloss_en: Option<String>,
    gloss_ko: Option<String>,
    part_of_speech: String,
    scheduled_at: Option<String>,
    mastery_level: String,
    interval_hours: i64,
    due_state: String,
    unit_order: Option<i64>,
    unit_title: Option<String>,
    is_new: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionState {
    session_id: i64,
    mode: String,
    started_at: String,
    finished_at: Option<String>,
    course_key: Option<String>,
}

#[derive(Debug, Clone)]
struct CourseQueueSeed {
    lexeme_id: i64,
    unit_order: i64,
    unit_title: String,
}

#[derive(Debug)]
struct ReviewStateInput {
    ease_factor: f64,
    interval_hours: i64,
    repetitions: i64,
    lapse_count: i64,
    correct_streak: i64,
}

#[derive(Debug)]
struct ReviewStateOutput {
    ease_factor: f64,
    interval_hours: i64,
    repetitions: i64,
    lapse_count: i64,
    correct_streak: i64,
    mastery_level: String,
    scheduled_at: String,
}

#[tauri::command]
fn search_lexemes(query: String, limit: Option<i64>) -> Result<Vec<SearchLexeme>, String> {
    let conn = open_content_db().map_err(|err| err.to_string())?;
    let limit = limit.unwrap_or(24).clamp(1, 100);
    let trimmed = query.trim();

    let sql = if trimmed.is_empty() {
        "
        SELECT
            l.id,
            lang.code,
            l.display_form,
            l.reading,
            pos.display_name,
            MIN(NULLIF(s.gloss_en, '')),
            MIN(NULLIF(s.gloss_ko, '')),
            l.frequency_rank,
            l.jlpt_level,
            l.cefr_level
        FROM lexemes l
        JOIN languages lang ON lang.id = l.language_id
        JOIN pos_tags pos ON pos.id = l.primary_pos_id
        LEFT JOIN lexeme_senses s ON s.lexeme_id = l.id
        GROUP BY l.id
        ORDER BY l.frequency_rank IS NULL, l.frequency_rank, l.quality_score DESC, l.id
        LIMIT ?1
        "
    } else {
        "
        SELECT
            l.id,
            lang.code,
            l.display_form,
            l.reading,
            pos.display_name,
            MIN(NULLIF(s.gloss_en, '')),
            MIN(NULLIF(s.gloss_ko, '')),
            l.frequency_rank,
            l.jlpt_level,
            l.cefr_level
        FROM lexemes l
        JOIN languages lang ON lang.id = l.language_id
        JOIN pos_tags pos ON pos.id = l.primary_pos_id
        LEFT JOIN lexeme_senses s ON s.lexeme_id = l.id
        LEFT JOIN lexeme_search ls ON ls.lexeme_id = l.id
        WHERE ls.rowid IN (
            SELECT rowid FROM lexeme_search WHERE lexeme_search MATCH ?1
        )
           OR l.display_form LIKE ?2
           OR COALESCE(l.reading, '') LIKE ?2
           OR COALESCE(s.gloss_en, '') LIKE ?2
           OR COALESCE(s.gloss_ko, '') LIKE ?2
        GROUP BY l.id
        ORDER BY
            CASE WHEN l.display_form = ?3 THEN 0 ELSE 1 END,
            l.frequency_rank IS NULL,
            l.frequency_rank,
            l.quality_score DESC,
            l.id
        LIMIT ?4
        "
    };

    let mut stmt = conn.prepare(sql).map_err(|err| err.to_string())?;
    let rows = if trimmed.is_empty() {
        stmt.query_map(params![limit], map_search_lexeme)
            .map_err(|err| err.to_string())?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|err| err.to_string())?
    } else {
        let like = format!("%{trimmed}%");
        stmt.query_map(params![trimmed, like, trimmed, limit], map_search_lexeme)
            .map_err(|err| err.to_string())?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|err| err.to_string())?
    };

    Ok(rows)
}

#[tauri::command]
fn get_lexeme_detail(lexeme_id: i64) -> Result<Option<LexemeDetail>, String> {
    let conn = open_content_db().map_err(|err| err.to_string())?;

    let base = conn
        .query_row(
            "
            SELECT
                l.id,
                lang.code,
                l.lemma,
                l.display_form,
                l.reading,
                pos.display_name,
                l.frequency_rank,
                l.jlpt_level,
                l.cefr_level,
                l.quality_score
            FROM lexemes l
            JOIN languages lang ON lang.id = l.language_id
            JOIN pos_tags pos ON pos.id = l.primary_pos_id
            WHERE l.id = ?1
            ",
            params![lexeme_id],
            |row: &rusqlite::Row<'_>| {
                Ok(LexemeDetail {
                    id: row.get(0)?,
                    language: row.get(1)?,
                    lemma: row.get(2)?,
                    display_form: row.get(3)?,
                    reading: row.get(4)?,
                    part_of_speech: row.get(5)?,
                    frequency_rank: row.get(6)?,
                    jlpt_level: row.get(7)?,
                    cefr_level: row.get(8)?,
                    quality_score: row.get(9)?,
                    senses: Vec::new(),
                    examples: Vec::new(),
                    kanji: Vec::new(),
                    tags: Vec::new(),
                })
            },
        )
        .optional()
        .map_err(|err| err.to_string())?;

    let Some(mut detail) = base else {
        return Ok(None);
    };

    detail.senses = load_senses(&conn, lexeme_id).map_err(|err| err.to_string())?;
    detail.examples = load_examples(&conn, lexeme_id).map_err(|err| err.to_string())?;
    detail.kanji = load_kanji(&conn, lexeme_id).map_err(|err| err.to_string())?;
    detail.tags = load_tags(&conn, lexeme_id).map_err(|err| err.to_string())?;

    Ok(Some(detail))
}

#[tauri::command]
fn get_dashboard_snapshot() -> Result<DashboardSnapshot, String> {
    let content = open_content_db().map_err(|err| err.to_string())?;
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    let profile_id = ensure_default_profile(&progress).map_err(|err| err.to_string())?;
    let now = Utc::now().to_rfc3339();

    let active_session = progress
        .query_row(
            "
            SELECT id, mode, started_at, json_extract(metadata_json, '$.courseKey')
            FROM study_sessions
            WHERE profile_id = ?1 AND finished_at IS NULL
            ORDER BY started_at DESC
            LIMIT 1
            ",
            params![profile_id],
            |row: &rusqlite::Row<'_>| {
                Ok(ActiveSession {
                    id: row.get(0)?,
                    mode: row.get(1)?,
                    started_at: row.get(2)?,
                    course_key: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(|err| err.to_string())?;

    Ok(DashboardSnapshot {
        profile_key: DEFAULT_PROFILE_KEY.to_string(),
        due_reviews: scalar_i64(
            &progress,
            "
            SELECT COUNT(*)
            FROM review_items ri
            LEFT JOIN srs_state s ON s.review_item_id = ri.id
            WHERE ri.item_type = 'lexeme'
              AND (s.scheduled_at IS NULL OR s.scheduled_at <= ?1)
            ",
            params![now],
        )
        .map_err(|err| err.to_string())?,
        new_items: scalar_i64(
            &progress,
            "
            SELECT COUNT(*)
            FROM review_items ri
            LEFT JOIN srs_state s ON s.review_item_id = ri.id
            WHERE ri.item_type = 'lexeme'
              AND COALESCE(s.repetitions, 0) = 0
            ",
            [],
        )
        .map_err(|err| err.to_string())?,
        total_review_items: scalar_i64(
            &progress,
            "SELECT COUNT(*) FROM review_items WHERE item_type = 'lexeme'",
            [],
        )
        .map_err(|err| err.to_string())?,
        review_events_today: scalar_i64(
            &progress,
            "
            SELECT COUNT(*)
            FROM review_events
            WHERE reviewed_at >= datetime('now', 'start of day')
            ",
            [],
        )
        .map_err(|err| err.to_string())?,
        course_templates: scalar_i64(&content, "SELECT COUNT(*) FROM course_templates", [])
            .map_err(|err| err.to_string())?,
        lexeme_count: scalar_i64(&content, "SELECT COUNT(*) FROM lexemes", [])
            .map_err(|err| err.to_string())?,
        kanji_count: scalar_i64(&content, "SELECT COUNT(*) FROM kanji", [])
            .map_err(|err| err.to_string())?,
        active_session,
    })
}

#[tauri::command]
fn get_study_starts() -> Result<Vec<StudyStartOption>, String> {
    let content = open_content_db().map_err(|err| err.to_string())?;
    get_study_start_catalog(&content).map_err(|err| err.to_string())
}

#[tauri::command]
fn get_course_map(course_key: String) -> Result<CourseMap, String> {
    let content = open_content_db().map_err(|err| err.to_string())?;
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    let profile_id = ensure_default_profile(&progress).map_err(|err| err.to_string())?;
    let starts = get_study_start_catalog(&content).map_err(|err| err.to_string())?;
    let option = starts
        .into_iter()
        .find(|option| option.course_key == course_key)
        .ok_or_else(|| format!("unknown course key: {course_key}"))?;

    let current_unit_order = ensure_course_progress_row(&progress, profile_id, &course_key)
        .map_err(|err| err.to_string())?;
    let units = load_course_map_units(
        &content,
        &progress,
        profile_id,
        &course_key,
        current_unit_order,
    )
    .map_err(|err| err.to_string())?;

    Ok(CourseMap {
        course_key,
        name: option.name,
        description: option.description,
        level_label: option.level_label,
        recommended_reason: option.recommended_reason,
        units,
    })
}

#[tauri::command]
fn start_study_session(
    mode: Option<String>,
    course_key: Option<String>,
) -> Result<SessionState, String> {
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    let profile_id = ensure_default_profile(&progress).map_err(|err| err.to_string())?;
    let now = Utc::now().to_rfc3339();
    let mode = mode.unwrap_or_else(|| "review".to_string());

    if let Some(existing) = progress
        .query_row(
            "
            SELECT id, mode, started_at, finished_at, json_extract(metadata_json, '$.courseKey')
            FROM study_sessions
            WHERE profile_id = ?1 AND finished_at IS NULL
            ORDER BY started_at DESC
            LIMIT 1
            ",
            params![profile_id],
            |row: &rusqlite::Row<'_>| {
                Ok(SessionState {
                    session_id: row.get(0)?,
                    mode: row.get(1)?,
                    started_at: row.get(2)?,
                    finished_at: row.get(3)?,
                    course_key: row.get(4)?,
                })
            },
        )
        .optional()
        .map_err(|err| err.to_string())?
    {
        if existing.mode == mode && existing.course_key == course_key {
            return Ok(existing);
        }

        progress
            .execute(
                "UPDATE study_sessions SET finished_at = ?2 WHERE id = ?1 AND finished_at IS NULL",
                params![existing.session_id, now],
            )
            .map_err(|err| err.to_string())?;
    }

    let session_key = format!("{DEFAULT_PROFILE_KEY}-{}", Utc::now().timestamp_millis());
    let metadata_json = course_key
        .clone()
        .map(|value| serde_json::json!({ "courseKey": value }).to_string());
    if let Some(ref course_key) = course_key {
        ensure_course_progress_row(&progress, profile_id, course_key)
            .map_err(|err| err.to_string())?;
    }
    progress
        .execute(
            "
            INSERT INTO study_sessions (profile_id, session_key, mode, started_at, device, metadata_json)
            VALUES (?1, ?2, ?3, ?4, 'desktop', ?5)
            ",
            params![profile_id, session_key, mode, now, metadata_json],
        )
        .map_err(|err| err.to_string())?;

    Ok(SessionState {
        session_id: progress.last_insert_rowid(),
        mode,
        started_at: now,
        finished_at: None,
        course_key,
    })
}

#[tauri::command]
fn finish_study_session(session_id: i64) -> Result<SessionState, String> {
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    let finished_at = Utc::now().to_rfc3339();
    progress
        .execute(
            "UPDATE study_sessions SET finished_at = COALESCE(finished_at, ?2) WHERE id = ?1",
            params![session_id, finished_at],
        )
        .map_err(|err| err.to_string())?;

    progress
        .query_row(
            "SELECT id, mode, started_at, finished_at, json_extract(metadata_json, '$.courseKey') FROM study_sessions WHERE id = ?1",
            params![session_id],
            |row: &rusqlite::Row<'_>| {
                Ok(SessionState {
                    session_id: row.get(0)?,
                    mode: row.get(1)?,
                    started_at: row.get(2)?,
                    finished_at: row.get(3)?,
                    course_key: row.get(4)?,
                })
            },
        )
        .map_err(|err| err.to_string())
}

#[tauri::command]
fn get_due_reviews(
    course_key: Option<String>,
    limit: Option<i64>,
) -> Result<Vec<ReviewQueueItem>, String> {
    let content = open_content_db().map_err(|err| err.to_string())?;
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    let _profile_id = ensure_default_profile(&progress).map_err(|err| err.to_string())?;
    let now = Utc::now().to_rfc3339();
    let limit = limit.unwrap_or(20).clamp(1, 100);

    if let Some(course_key) = course_key {
        return load_course_queue(&content, &progress, &course_key, &now, limit)
            .map_err(|err| err.to_string());
    }

    let mut stmt = progress
        .prepare(
            "
            SELECT
                ri.id,
                ri.item_id,
                s.scheduled_at,
                COALESCE(s.mastery_level, 'new'),
                COALESCE(s.interval_hours, 0),
                CASE
                    WHEN s.scheduled_at IS NULL THEN 'new'
                    WHEN s.scheduled_at <= ?1 THEN 'due'
                    ELSE 'scheduled'
                END AS due_state
            FROM review_items ri
            LEFT JOIN srs_state s ON s.review_item_id = ri.id
            WHERE ri.item_type = 'lexeme'
              AND (s.scheduled_at IS NULL OR s.scheduled_at <= ?1)
            ORDER BY s.scheduled_at IS NOT NULL, s.scheduled_at, ri.id
            LIMIT ?2
            ",
        )
        .map_err(|err| err.to_string())?;

    let review_rows = stmt
        .query_map(params![now, limit], |row: &rusqlite::Row<'_>| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
            ))
        })
        .map_err(|err| err.to_string())?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|err| err.to_string())?;

    let mut items = Vec::with_capacity(review_rows.len());
    for (review_item_id, lexeme_id, scheduled_at, mastery_level, interval_hours, due_state) in
        review_rows
    {
        let item = hydrate_review_queue_item(
            &content,
            review_item_id,
            lexeme_id,
            scheduled_at.clone(),
            mastery_level.clone(),
            interval_hours,
            due_state.clone(),
            None,
            None,
            scheduled_at.is_none(),
        )
        .map_err(|err| err.to_string())?;

        if let Some(item) = item {
            items.push(item);
        }
    }

    Ok(items)
}

#[tauri::command]
fn submit_lexeme_review(
    session_id: i64,
    lexeme_id: i64,
    grade: String,
    response_time_ms: Option<i64>,
) -> Result<ReviewQueueItem, String> {
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    let content = open_content_db().map_err(|err| err.to_string())?;
    let profile_id = ensure_default_profile(&progress).map_err(|err| err.to_string())?;
    let now = Utc::now();
    let now_rfc3339 = now.to_rfc3339();

    let tx = progress
        .unchecked_transaction()
        .map_err(|err| err.to_string())?;
    let review_item_id =
        ensure_review_item(&tx, "lexeme", lexeme_id).map_err(|err| err.to_string())?;
    let previous = tx
        .query_row(
            "
            SELECT ease_factor, interval_hours, repetitions, lapse_count, correct_streak, scheduled_at
            FROM srs_state
            WHERE review_item_id = ?1
            ",
            params![review_item_id],
            |row: &rusqlite::Row<'_>| {
                Ok((
                    ReviewStateInput {
                        ease_factor: row.get(0)?,
                        interval_hours: row.get(1)?,
                        repetitions: row.get(2)?,
                        lapse_count: row.get(3)?,
                        correct_streak: row.get(4)?,
                    },
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()
        .map_err(|err| err.to_string())?;

    let input = previous
        .as_ref()
        .map(|(state, _)| state)
        .unwrap_or(&ReviewStateInput {
            ease_factor: 2.5,
            interval_hours: 0,
            repetitions: 0,
            lapse_count: 0,
            correct_streak: 0,
        });
    let was_new = input.repetitions == 0;
    let next = next_review_state(input, &grade, now);
    let scheduled_before = previous.and_then(|(_, scheduled_at)| scheduled_at);

    tx.execute(
        "
        INSERT INTO srs_state (
            review_item_id,
            ease_factor,
            interval_hours,
            repetitions,
            lapse_count,
            correct_streak,
            mastery_level,
            scheduled_at,
            last_reviewed_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT(review_item_id) DO UPDATE SET
            ease_factor = excluded.ease_factor,
            interval_hours = excluded.interval_hours,
            repetitions = excluded.repetitions,
            lapse_count = excluded.lapse_count,
            correct_streak = excluded.correct_streak,
            mastery_level = excluded.mastery_level,
            scheduled_at = excluded.scheduled_at,
            last_reviewed_at = excluded.last_reviewed_at
        ",
        params![
            review_item_id,
            next.ease_factor,
            next.interval_hours,
            next.repetitions,
            next.lapse_count,
            next.correct_streak,
            next.mastery_level,
            next.scheduled_at,
            now_rfc3339,
        ],
    )
    .map_err(|err| err.to_string())?;

    tx.execute(
        "
        INSERT INTO review_events (
            review_item_id,
            session_id,
            review_type,
            grade,
            response_time_ms,
            scheduled_before,
            scheduled_after,
            reviewed_at
        ) VALUES (?1, ?2, 'flashcard', ?3, ?4, ?5, ?6, ?7)
        ",
        params![
            review_item_id,
            session_id,
            grade,
            response_time_ms,
            scheduled_before,
            next.scheduled_at,
            now_rfc3339,
        ],
    )
    .map_err(|err| err.to_string())?;

    update_course_progress_for_review(
        &tx,
        &content,
        profile_id,
        session_id,
        lexeme_id,
        was_new,
        grade != "again",
    )
    .map_err(|err| err.to_string())?;

    tx.commit().map_err(|err| err.to_string())?;

    hydrate_review_queue_item(
        &content,
        review_item_id,
        lexeme_id,
        Some(next.scheduled_at),
        next.mastery_level,
        next.interval_hours,
        "scheduled".to_string(),
        None,
        None,
        false,
    )
    .map_err(|err| err.to_string())?
    .ok_or_else(|| "review item disappeared after update".to_string())
}

pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            search_lexemes,
            get_lexeme_detail,
            get_dashboard_snapshot,
            get_study_starts,
            get_course_map,
            start_study_session,
            finish_study_session,
            get_due_reviews,
            submit_lexeme_review
        ])
        .run(tauri::generate_context!())
        .expect("failed to run LinguaForge desktop app");
}

fn map_search_lexeme(row: &rusqlite::Row<'_>) -> rusqlite::Result<SearchLexeme> {
    Ok(SearchLexeme {
        id: row.get(0)?,
        language: row.get(1)?,
        display_form: row.get(2)?,
        reading: row.get(3)?,
        part_of_speech: row.get(4)?,
        gloss_en: row.get(5)?,
        gloss_ko: row.get(6)?,
        frequency_rank: row.get(7)?,
        jlpt_level: row.get(8)?,
        cefr_level: row.get(9)?,
    })
}

fn get_study_start_catalog(content: &Connection) -> anyhow::Result<Vec<StudyStartOption>> {
    let mut stmt = content.prepare(
        "
        SELECT
            ct.course_key,
            lang.code,
            ct.name,
            ct.description,
            ct.category,
            ct.target_exam,
            ct.difficulty_start,
            COUNT(DISTINCT cu.id) AS unit_count,
            COUNT(ui.item_id) AS item_count
        FROM course_templates ct
        JOIN languages lang ON lang.id = ct.language_id
        LEFT JOIN course_units cu ON cu.course_id = ct.id
        LEFT JOIN unit_items ui ON ui.unit_id = cu.id
        GROUP BY ct.id
        ORDER BY
            CASE
                WHEN ct.course_key = 'ja-jlpt-N5' THEN 0
                WHEN ct.course_key = 'en-core-a1-a2' THEN 1
                WHEN ct.course_key = 'ja-jlpt-N4' THEN 2
                ELSE 10
            END,
            ct.difficulty_start IS NULL,
            ct.difficulty_start,
            ct.name
        ",
    )?;

    let mut starts = stmt
        .query_map([], |row: &rusqlite::Row<'_>| {
            let course_key: String = row.get(0)?;
            let target_exam: Option<String> = row.get(5)?;
            let difficulty_start: Option<i64> = row.get(6)?;
            Ok(StudyStartOption {
                level_label: course_level_label(
                    &course_key,
                    difficulty_start,
                    target_exam.as_deref(),
                ),
                recommended_reason: course_recommendation(
                    &course_key,
                    difficulty_start,
                    target_exam.as_deref(),
                ),
                course_key,
                language: row.get(1)?,
                name: row.get(2)?,
                description: row.get(3)?,
                category: row.get(4)?,
                unit_count: row.get(7)?,
                item_count: row.get(8)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    starts.extend(load_virtual_study_starts(content)?);
    starts.sort_by_key(|option| study_start_priority(&option.course_key));
    Ok(starts)
}

fn load_virtual_study_starts(content: &Connection) -> anyhow::Result<Vec<StudyStartOption>> {
    let mut starts = Vec::new();

    if let Some(option) = build_virtual_study_start(
        content,
        "ja-kindergarten-hiragana",
        "ja",
        "일본어 유치원 - 히라가나",
        "히라가나만 보이는 가장 쉬운 카드부터 천천히 시작",
    )? {
        starts.push(option);
    }

    if let Some(option) = build_virtual_study_start(
        content,
        "ja-kindergarten-katakana",
        "ja",
        "일본어 유치원 - 가타카나",
        "가타카나 단어만 따로 익히는 초초급 코스",
    )? {
        starts.push(option);
    }

    if let Some(option) = build_virtual_study_start(
        content,
        "en-kindergarten",
        "en",
        "영어 유치원",
        "가장 짧고 쉬운 기초 단어부터 다시 시작",
    )? {
        starts.push(option);
    }

    Ok(starts)
}

fn build_virtual_study_start(
    content: &Connection,
    course_key: &str,
    language: &str,
    name: &str,
    description: &str,
) -> anyhow::Result<Option<StudyStartOption>> {
    let seeds = load_virtual_course_queue_seeds(content, course_key)?;
    if seeds.is_empty() {
        return Ok(None);
    }

    let unit_count = seeds.iter().map(|seed| seed.unit_order).max().unwrap_or(0);
    Ok(Some(StudyStartOption {
        course_key: course_key.to_string(),
        language: language.to_string(),
        name: name.to_string(),
        description: Some(description.to_string()),
        category: "starter".to_string(),
        level_label: "완전 처음".to_string(),
        recommended_reason: if language == "ja" {
            "문자를 막 익히기 시작한 단계에서도 바로 들어갈 수 있음".to_string()
        } else {
            "알파벳 이후 가장 쉬운 어휘부터 부담 없이 시작 가능".to_string()
        },
        unit_count,
        item_count: seeds.len() as i64,
    }))
}

fn study_start_priority(course_key: &str) -> i32 {
    match course_key {
        "ja-kindergarten-hiragana" => 0,
        "ja-kindergarten-katakana" => 1,
        "en-kindergarten" => 2,
        "ja-jlpt-N5" => 10,
        "en-core-a1-a2" => 11,
        "ja-jlpt-N4" => 12,
        _ => 30,
    }
}

fn load_course_queue(
    content: &Connection,
    progress: &Connection,
    course_key: &str,
    now: &str,
    limit: i64,
) -> anyhow::Result<Vec<ReviewQueueItem>> {
    let profile_id = ensure_default_profile(progress)?;
    let current_unit_order = ensure_course_progress_row(progress, profile_id, course_key)?;
    let mut seeds = load_course_queue_seeds(content, course_key)?;
    if seeds.is_empty() {
        return Ok(Vec::new());
    }

    let filtered: Vec<CourseQueueSeed> = seeds
        .iter()
        .filter(|seed| seed.unit_order == current_unit_order)
        .cloned()
        .collect();
    if !filtered.is_empty() {
        seeds = filtered;
    }

    let mut due_items = Vec::new();
    let mut new_items = Vec::new();
    let mut state_stmt = progress.prepare(
        "
        SELECT
            ri.id,
            s.scheduled_at,
            COALESCE(s.mastery_level, 'new'),
            COALESCE(s.interval_hours, 0)
        FROM review_items ri
        LEFT JOIN srs_state s ON s.review_item_id = ri.id
        WHERE ri.item_type = 'lexeme' AND ri.item_id = ?1
        LIMIT 1
        ",
    )?;

    for seed in seeds {
        let state = state_stmt
            .query_row(params![seed.lexeme_id], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .optional()?;

        let (review_item_id, scheduled_at, mastery_level, interval_hours, due_state, is_new) =
            match state {
                Some((review_item_id, scheduled_at, mastery_level, interval_hours)) => {
                    if let Some(ref scheduled_at) = scheduled_at {
                        if scheduled_at.as_str() > now {
                            continue;
                        }
                    }

                    (
                        review_item_id,
                        scheduled_at.clone(),
                        mastery_level,
                        interval_hours,
                        if scheduled_at.is_some() {
                            "due".to_string()
                        } else {
                            "new".to_string()
                        },
                        scheduled_at.is_none(),
                    )
                }
                None => (0, None, "new".to_string(), 0, "new".to_string(), true),
            };

        let item = hydrate_review_queue_item(
            content,
            review_item_id,
            seed.lexeme_id,
            scheduled_at,
            mastery_level,
            interval_hours,
            due_state.clone(),
            Some(seed.unit_order),
            Some(seed.unit_title),
            is_new,
        )?;

        if let Some(item) = item {
            if due_state == "due" {
                due_items.push(item);
            } else {
                new_items.push(item);
            }
        }

        if due_items.len() + new_items.len() >= limit as usize {
            break;
        }
    }

    due_items.extend(new_items);
    due_items.truncate(limit as usize);
    Ok(due_items)
}

fn load_course_map_units(
    content: &Connection,
    progress: &Connection,
    profile_id: i64,
    course_key: &str,
    current_unit_order: i64,
) -> anyhow::Result<Vec<CourseMapUnit>> {
    let seeds = load_course_queue_seeds(content, course_key)?;
    let mut units = Vec::new();
    let mut current_group: Option<(i64, String, i64)> = None;

    for seed in seeds {
        if let Some((unit_order, title, total_items)) = current_group.take() {
            if unit_order == seed.unit_order {
                current_group = Some((unit_order, title, total_items + 1));
            } else {
                units.push(build_course_map_unit(
                    progress,
                    profile_id,
                    course_key,
                    unit_order,
                    &title,
                    total_items,
                    current_unit_order,
                )?);
                current_group = Some((seed.unit_order, seed.unit_title, 1));
            }
        } else {
            current_group = Some((seed.unit_order, seed.unit_title, 1));
        }
    }

    if let Some((unit_order, title, total_items)) = current_group {
        units.push(build_course_map_unit(
            progress,
            profile_id,
            course_key,
            unit_order,
            &title,
            total_items,
            current_unit_order,
        )?);
    }

    Ok(units)
}

fn build_course_map_unit(
    progress: &Connection,
    profile_id: i64,
    course_key: &str,
    unit_order: i64,
    title: &str,
    total_items: i64,
    current_unit_order: i64,
) -> anyhow::Result<CourseMapUnit> {
    let progress_row = progress
        .query_row(
            "
            SELECT learned_count, reviewed_count, is_completed
            FROM unit_progress
            WHERE profile_id = ?1 AND course_key = ?2 AND unit_order = ?3
            ",
            params![profile_id, course_key, unit_order],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?;

    let (learned_count, reviewed_count, is_completed) = progress_row.unwrap_or((0, 0, 0));

    Ok(CourseMapUnit {
        unit_order,
        title: title.to_string(),
        total_items,
        learned_count,
        reviewed_count,
        is_completed: is_completed == 1,
        is_current: unit_order == current_unit_order,
        is_locked: unit_order > current_unit_order,
    })
}

fn load_course_queue_seeds(
    content: &Connection,
    course_key: &str,
) -> anyhow::Result<Vec<CourseQueueSeed>> {
    if matches!(
        course_key,
        "ja-kindergarten-hiragana" | "ja-kindergarten-katakana" | "en-kindergarten"
    ) {
        return load_virtual_course_queue_seeds(content, course_key);
    }

    load_real_course_queue_seeds(content, course_key)
}

fn load_real_course_queue_seeds(
    content: &Connection,
    course_key: &str,
) -> anyhow::Result<Vec<CourseQueueSeed>> {
    let mut stmt = content.prepare(
        "
        SELECT ui.item_id, cu.unit_order, cu.title
        FROM course_templates ct
        JOIN course_units cu ON cu.course_id = ct.id
        JOIN unit_items ui ON ui.unit_id = cu.id
        WHERE ct.course_key = ?1 AND ui.item_type = 'lexeme'
        ORDER BY cu.unit_order, ui.item_order, ui.item_id
        ",
    )?;

    Ok(stmt
        .query_map(params![course_key], |row| {
            Ok(CourseQueueSeed {
                lexeme_id: row.get(0)?,
                unit_order: row.get(1)?,
                unit_title: row.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

fn load_virtual_course_queue_seeds(
    content: &Connection,
    course_key: &str,
) -> anyhow::Result<Vec<CourseQueueSeed>> {
    let mut seen = HashSet::new();
    let mut lexeme_ids = Vec::new();

    match course_key {
        "ja-kindergarten-hiragana" => {
            let mut stmt = content.prepare(
                "
                SELECT l.id, l.display_form, l.reading
                FROM lexemes l
                JOIN languages lang ON lang.id = l.language_id
                WHERE lang.code = 'ja'
                  AND (l.jlpt_level = 5 OR l.frequency_rank IS NOT NULL)
                ORDER BY l.jlpt_level IS NULL, l.jlpt_level DESC, l.frequency_rank IS NULL, l.frequency_rank, l.quality_score DESC, l.id
                LIMIT 240
                ",
            )?;

            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })?;

            for row in rows {
                let (lexeme_id, display_form, reading) = row?;
                if !is_hiragana_only(&display_form) {
                    continue;
                }
                if display_form.chars().count() > 6 {
                    continue;
                }
                let dedupe_key = reading.unwrap_or_else(|| display_form.clone());
                if seen.insert(dedupe_key) {
                    lexeme_ids.push(lexeme_id);
                }
                if lexeme_ids.len() >= 72 {
                    break;
                }
            }
        }
        "ja-kindergarten-katakana" => {
            let mut stmt = content.prepare(
                "
                SELECT l.id, l.display_form, l.reading
                FROM lexemes l
                JOIN languages lang ON lang.id = l.language_id
                WHERE lang.code = 'ja'
                  AND (l.frequency_rank IS NOT NULL OR l.jlpt_level = 5)
                ORDER BY l.frequency_rank IS NULL, l.frequency_rank, l.quality_score DESC, l.id
                LIMIT 320
                ",
            )?;

            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })?;

            for row in rows {
                let (lexeme_id, display_form, reading) = row?;
                if !is_katakana_only(&display_form) {
                    continue;
                }
                if display_form.chars().count() > 8 {
                    continue;
                }
                let dedupe_key = reading.unwrap_or_else(|| display_form.clone());
                if seen.insert(dedupe_key) {
                    lexeme_ids.push(lexeme_id);
                }
                if lexeme_ids.len() >= 72 {
                    break;
                }
            }
        }
        "en-kindergarten" => {
            let mut stmt = content.prepare(
                "
                SELECT l.id, l.display_form
                FROM lexemes l
                JOIN languages lang ON lang.id = l.language_id
                WHERE lang.code = 'en'
                  AND (l.cefr_level IN ('A1', 'A2') OR l.frequency_rank <= 2500)
                ORDER BY l.cefr_level IS NULL, l.cefr_level, l.frequency_rank IS NULL, l.frequency_rank, l.quality_score DESC, l.id
                LIMIT 240
                ",
            )?;

            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?;

            for row in rows {
                let (lexeme_id, display_form) = row?;
                if display_form.chars().count() > 8 {
                    continue;
                }
                if !display_form
                    .chars()
                    .all(|ch| ch.is_ascii_alphabetic() || ch == ' ' || ch == '-')
                {
                    continue;
                }
                if seen.insert(display_form.to_ascii_lowercase()) {
                    lexeme_ids.push(lexeme_id);
                }
                if lexeme_ids.len() >= 72 {
                    break;
                }
            }
        }
        _ => {}
    }

    Ok(lexeme_ids
        .into_iter()
        .enumerate()
        .map(|(index, lexeme_id)| CourseQueueSeed {
            lexeme_id,
            unit_order: (index as i64 / 8) + 1,
            unit_title: virtual_unit_title(course_key, (index as i64 / 8) + 1),
        })
        .collect())
}

fn hydrate_review_queue_item(
    content: &Connection,
    review_item_id: i64,
    lexeme_id: i64,
    scheduled_at: Option<String>,
    mastery_level: String,
    interval_hours: i64,
    due_state: String,
    unit_order: Option<i64>,
    unit_title: Option<String>,
    is_new: bool,
) -> anyhow::Result<Option<ReviewQueueItem>> {
    Ok(content
        .query_row(
            "
            SELECT
                l.id,
                l.display_form,
                l.reading,
                pos.display_name,
                MIN(NULLIF(s.gloss_en, '')),
                MIN(NULLIF(s.gloss_ko, ''))
            FROM lexemes l
            JOIN pos_tags pos ON pos.id = l.primary_pos_id
            LEFT JOIN lexeme_senses s ON s.lexeme_id = l.id
            WHERE l.id = ?1
            GROUP BY l.id
            ",
            params![lexeme_id],
            |row: &rusqlite::Row<'_>| {
                Ok(ReviewQueueItem {
                    review_item_id,
                    lexeme_id: row.get(0)?,
                    display_form: row.get(1)?,
                    reading: row.get(2)?,
                    part_of_speech: row.get(3)?,
                    gloss_en: row.get(4)?,
                    gloss_ko: row.get(5)?,
                    scheduled_at,
                    mastery_level,
                    interval_hours,
                    due_state,
                    unit_order,
                    unit_title,
                    is_new,
                })
            },
        )
        .optional()?)
}

fn virtual_unit_title(course_key: &str, unit_order: i64) -> String {
    match course_key {
        "ja-kindergarten-hiragana" => format!("히라가나 {unit_order}"),
        "ja-kindergarten-katakana" => format!("가타카나 {unit_order}"),
        "en-kindergarten" => format!("첫 단어 {unit_order}"),
        _ => format!("{unit_order}단계"),
    }
}

fn is_hiragana_only(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|ch| matches!(ch as u32, 0x3040..=0x309f) || ch == '・' || ch == 'ー' || ch == ' ')
}

fn is_katakana_only(text: &str) -> bool {
    !text.is_empty()
        && text
            .chars()
            .all(|ch| matches!(ch as u32, 0x30a0..=0x30ff) || ch == '・' || ch == ' ' || ch == 'ー')
}

fn load_senses(conn: &Connection, lexeme_id: i64) -> rusqlite::Result<Vec<LexemeSense>> {
    let mut stmt = conn.prepare(
        "
        SELECT sense_order, gloss_en, gloss_ko, gloss_detail
        FROM lexeme_senses
        WHERE lexeme_id = ?1
        ORDER BY sense_order
        ",
    )?;
    stmt.query_map(params![lexeme_id], |row| {
        Ok(LexemeSense {
            sense_order: row.get(0)?,
            gloss_en: row.get(1)?,
            gloss_ko: row.get(2)?,
            gloss_detail: row.get(3)?,
        })
    })?
    .collect()
}

fn load_examples(conn: &Connection, lexeme_id: i64) -> rusqlite::Result<Vec<LexemeExample>> {
    let mut stmt = conn.prepare(
        "
        SELECT
            e.id,
            e.sentence,
            e.sentence_reading,
            MIN(CASE WHEN lang.code = 'en' THEN et.translation_text END),
            le.match_score
        FROM lexeme_examples le
        JOIN examples e ON e.id = le.example_id
        LEFT JOIN example_translations et ON et.example_id = e.id
        LEFT JOIN languages lang ON lang.id = et.language_id
        WHERE le.lexeme_id = ?1
        GROUP BY e.id, le.match_score
        ORDER BY le.match_score DESC, e.id
        LIMIT 6
        ",
    )?;
    stmt.query_map(params![lexeme_id], |row| {
        Ok(LexemeExample {
            id: row.get(0)?,
            sentence: row.get(1)?,
            sentence_reading: row.get(2)?,
            translation_en: row.get(3)?,
            match_score: row.get(4)?,
        })
    })?
    .collect()
}

fn load_kanji(conn: &Connection, lexeme_id: i64) -> rusqlite::Result<Vec<LexemeKanji>> {
    let mut stmt = conn.prepare(
        "
        SELECT DISTINCT k.id, k.character, k.grade, k.jlpt_level, k.frequency_rank
        FROM kanji_lexemes kl
        JOIN kanji k ON k.id = kl.kanji_id
        WHERE kl.lexeme_id = ?1
        ORDER BY kl.position_index, k.id
        ",
    )?;
    let rows = stmt
        .query_map(params![lexeme_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut kanji = Vec::with_capacity(rows.len());
    for (kanji_id, character, grade, jlpt_level, frequency_rank) in rows {
        kanji.push(LexemeKanji {
            character,
            grade,
            jlpt_level,
            frequency_rank,
            meanings: load_kanji_values(
                conn,
                "SELECT meaning_text FROM kanji_meanings WHERE kanji_id = ?1 ORDER BY meaning_text LIMIT 8",
                kanji_id,
            )?,
            onyomi: load_kanji_values(
                conn,
                "SELECT reading_text FROM kanji_readings WHERE kanji_id = ?1 AND reading_type = 'onyomi' ORDER BY reading_text LIMIT 8",
                kanji_id,
            )?,
            kunyomi: load_kanji_values(
                conn,
                "SELECT reading_text FROM kanji_readings WHERE kanji_id = ?1 AND reading_type = 'kunyomi' ORDER BY reading_text LIMIT 8",
                kanji_id,
            )?,
        });
    }

    Ok(kanji)
}

fn load_kanji_values(conn: &Connection, sql: &str, kanji_id: i64) -> rusqlite::Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    stmt.query_map(params![kanji_id], |row| row.get::<_, String>(0))?
        .collect()
}

fn load_tags(conn: &Connection, lexeme_id: i64) -> rusqlite::Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "
        SELECT t.display_name
        FROM lexeme_tag_map m
        JOIN tags t ON t.id = m.tag_id
        WHERE m.lexeme_id = ?1
        ORDER BY t.display_name
        ",
    )?;
    stmt.query_map(params![lexeme_id], |row| row.get::<_, String>(0))?
        .collect()
}

fn open_content_db() -> anyhow::Result<Connection> {
    let path = if let Ok(path) = std::env::var("LINGUAFORGE_CONTENT_DB") {
        PathBuf::from(path)
    } else {
        workspace_root()?.join("db/content.db")
    };
    let conn = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    conn.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA query_only = ON;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -16000;
        ",
    )?;
    Ok(conn)
}

fn open_progress_db() -> anyhow::Result<Connection> {
    let path = if let Ok(path) = std::env::var("LINGUAFORGE_PROGRESS_DB") {
        PathBuf::from(path)
    } else {
        workspace_root()?.join("db/progress.db")
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -16000;
        ",
    )?;
    conn.execute_batch(include_str!("../../../../sql/progress_init.sql"))?;
    conn.execute(
        "
        INSERT INTO profiles (profile_key, display_name)
        VALUES (?1, ?2)
        ON CONFLICT(profile_key) DO UPDATE SET display_name = excluded.display_name
        ",
        params![DEFAULT_PROFILE_KEY, "Default"],
    )?;
    Ok(conn)
}

fn workspace_root() -> anyhow::Result<PathBuf> {
    let current = std::env::current_dir()?;
    if let Some(root) = find_workspace_root(&current) {
        return Ok(root);
    }

    let manifest_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
    if let Some(root) = find_workspace_root(&manifest_root) {
        return Ok(root);
    }

    anyhow::bail!("failed to locate LinguaForge workspace root")
}

fn find_workspace_root(start: &Path) -> Option<PathBuf> {
    for candidate in start.ancestors() {
        if candidate.join("sql/content_init.sql").exists() && candidate.join("db").exists() {
            return Some(candidate.to_path_buf());
        }
    }
    None
}

fn ensure_default_profile(conn: &Connection) -> anyhow::Result<i64> {
    Ok(conn.query_row(
        "SELECT id FROM profiles WHERE profile_key = ?1",
        params![DEFAULT_PROFILE_KEY],
        |row| row.get(0),
    )?)
}

fn ensure_course_progress_row(
    conn: &Connection,
    profile_id: i64,
    course_key: &str,
) -> anyhow::Result<i64> {
    conn.execute(
        "
        INSERT INTO course_progress (profile_id, course_key, current_unit_order, completed_units)
        VALUES (?1, ?2, 1, 0)
        ON CONFLICT(profile_id, course_key) DO NOTHING
        ",
        params![profile_id, course_key],
    )?;

    Ok(conn.query_row(
        "SELECT current_unit_order FROM course_progress WHERE profile_id = ?1 AND course_key = ?2",
        params![profile_id, course_key],
        |row| row.get(0),
    )?)
}

fn update_course_progress_for_review(
    tx: &rusqlite::Transaction<'_>,
    content: &Connection,
    profile_id: i64,
    session_id: i64,
    lexeme_id: i64,
    was_new: bool,
    passed: bool,
) -> anyhow::Result<()> {
    let course_key = tx
        .query_row(
            "SELECT json_extract(metadata_json, '$.courseKey') FROM study_sessions WHERE id = ?1",
            params![session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten();

    let Some(course_key) = course_key else {
        return Ok(());
    };

    let seeds = load_course_queue_seeds(content, &course_key)?;
    let Some(seed) = seeds.iter().find(|seed| seed.lexeme_id == lexeme_id) else {
        return Ok(());
    };

    tx.execute(
        "
        INSERT INTO unit_progress (
            profile_id,
            course_key,
            unit_order,
            learned_count,
            reviewed_count,
            is_completed,
            updated_at
        ) VALUES (?1, ?2, ?3, ?4, 1, 0, CURRENT_TIMESTAMP)
        ON CONFLICT(profile_id, course_key, unit_order) DO UPDATE SET
            learned_count = learned_count + excluded.learned_count,
            reviewed_count = reviewed_count + 1,
            updated_at = CURRENT_TIMESTAMP
        ",
        params![
            profile_id,
            course_key,
            seed.unit_order,
            if was_new && passed { 1 } else { 0 }
        ],
    )?;

    let learned_count: i64 = tx.query_row(
        "
        SELECT learned_count
        FROM unit_progress
        WHERE profile_id = ?1 AND course_key = ?2 AND unit_order = ?3
        ",
        params![profile_id, course_key, seed.unit_order],
        |row| row.get(0),
    )?;

    let unit_total = seeds
        .iter()
        .filter(|candidate| candidate.unit_order == seed.unit_order)
        .count() as i64;
    let max_unit = seeds
        .iter()
        .map(|candidate| candidate.unit_order)
        .max()
        .unwrap_or(1);

    if learned_count >= unit_total {
        tx.execute(
            "
            UPDATE unit_progress
            SET is_completed = 1, updated_at = CURRENT_TIMESTAMP
            WHERE profile_id = ?1 AND course_key = ?2 AND unit_order = ?3
            ",
            params![profile_id, course_key, seed.unit_order],
        )?;

        tx.execute(
            "
            UPDATE course_progress
            SET current_unit_order = CASE
                    WHEN ?3 < ?4 THEN ?3 + 1
                    ELSE ?3
                END,
                completed_units = MAX(completed_units, ?3),
                updated_at = CURRENT_TIMESTAMP
            WHERE profile_id = ?1 AND course_key = ?2
            ",
            params![profile_id, course_key, seed.unit_order, max_unit],
        )?;
    }

    Ok(())
}

fn ensure_review_item(
    tx: &rusqlite::Transaction<'_>,
    item_type: &str,
    item_id: i64,
) -> anyhow::Result<i64> {
    tx.execute(
        "INSERT OR IGNORE INTO review_items (item_type, item_id) VALUES (?1, ?2)",
        params![item_type, item_id],
    )?;
    Ok(tx.query_row(
        "SELECT id FROM review_items WHERE item_type = ?1 AND item_id = ?2",
        params![item_type, item_id],
        |row| row.get(0),
    )?)
}

fn next_review_state(
    input: &ReviewStateInput,
    grade: &str,
    now: chrono::DateTime<Utc>,
) -> ReviewStateOutput {
    let normalized = grade.trim().to_ascii_lowercase();
    let current_ease = input.ease_factor.max(1.3);

    let (ease_factor, interval_hours, repetitions, lapse_count, correct_streak, mastery_level) =
        match normalized.as_str() {
            "again" => (
                (current_ease - 0.2).max(1.3),
                1,
                0,
                input.lapse_count + 1,
                0,
                "relearning".to_string(),
            ),
            "hard" => {
                let interval = if input.repetitions == 0 {
                    8
                } else {
                    ((input.interval_hours.max(8) as f64) * 1.2).round() as i64
                };
                (
                    (current_ease - 0.15).max(1.3),
                    interval.max(8),
                    input.repetitions + 1,
                    input.lapse_count,
                    input.correct_streak + 1,
                    "learning".to_string(),
                )
            }
            "easy" => {
                let interval = if input.repetitions == 0 {
                    72
                } else {
                    ((input.interval_hours.max(24) as f64) * current_ease * 1.5).round() as i64
                };
                (
                    current_ease + 0.1,
                    interval.max(24),
                    input.repetitions + 1,
                    input.lapse_count,
                    input.correct_streak + 1,
                    if interval >= 24 * 14 {
                        "mastered".to_string()
                    } else {
                        "reviewing".to_string()
                    },
                )
            }
            _ => {
                let interval = if input.repetitions == 0 {
                    24
                } else {
                    ((input.interval_hours.max(12) as f64) * current_ease).round() as i64
                };
                (
                    current_ease + 0.03,
                    interval.max(12),
                    input.repetitions + 1,
                    input.lapse_count,
                    input.correct_streak + 1,
                    if interval >= 24 * 10 {
                        "mastered".to_string()
                    } else {
                        "reviewing".to_string()
                    },
                )
            }
        };

    ReviewStateOutput {
        ease_factor,
        interval_hours,
        repetitions,
        lapse_count,
        correct_streak,
        mastery_level,
        scheduled_at: (now + Duration::hours(interval_hours)).to_rfc3339(),
    }
}

fn course_level_label(
    course_key: &str,
    difficulty_start: Option<i64>,
    target_exam: Option<&str>,
) -> String {
    let normalized_key = course_key.to_ascii_lowercase();
    if let Some(target_exam) = target_exam {
        if target_exam.starts_with("JLPT-") {
            return format!("시험 기준 {target_exam}");
        }
        if target_exam.starts_with("CEFR-A") {
            return "영어 기초".to_string();
        }
        if target_exam.starts_with("CEFR-B") || target_exam.starts_with("CEFR-C") {
            return "영어 중급 이상".to_string();
        }
    }

    match (
        normalized_key.as_str(),
        difficulty_start.unwrap_or_default(),
    ) {
        ("ja-kindergarten-hiragana", _) => "히라가나 입문".to_string(),
        ("ja-kindergarten-katakana", _) => "가타카나 입문".to_string(),
        ("en-kindergarten", _) => "완전 처음".to_string(),
        (key, _) if key.contains("n5") => "일본어 첫 단계".to_string(),
        (key, _) if key.contains("n4") => "일본어 기초 확장".to_string(),
        (key, _) if key.contains("kanji") => "한자 중심".to_string(),
        (_, 0..=2) => "가볍게 시작".to_string(),
        (_, 3..=4) => "중급 도전".to_string(),
        _ => "심화 학습".to_string(),
    }
}

fn course_recommendation(
    course_key: &str,
    difficulty_start: Option<i64>,
    target_exam: Option<&str>,
) -> String {
    if course_key == "ja-kindergarten-hiragana" {
        return "히라가나만 집중해서 보는 가장 쉬운 첫 일본어 코스".to_string();
    }
    if course_key == "ja-kindergarten-katakana" {
        return "가타카나 표기를 따로 익히고 싶은 초초급 시작 코스".to_string();
    }
    if course_key == "en-kindergarten" {
        return "가장 짧고 쉬운 생활 단어부터 감각을 붙이기 좋은 시작 코스".to_string();
    }
    if course_key == "ja-jlpt-N5" {
        return "히라가나 이후 가장 자연스럽게 시작할 수 있는 일본어 코스".to_string();
    }
    if course_key == "ja-jlpt-N4" {
        return "기초 어휘를 안다면 바로 표현 폭을 넓히기 좋은 단계".to_string();
    }
    if course_key == "en-core-a1-a2" {
        return "영어 기본 단어를 다시 다지며 천천히 시작하기 좋음".to_string();
    }
    if course_key == "en-core-b1-c1" {
        return "이미 기초가 있는 학습자가 활용 어휘를 늘리기 좋음".to_string();
    }
    if course_key == "ja-kanji-core" {
        return "어휘와 병행해서 자주 나오는 한자를 따로 잡고 싶을 때 추천".to_string();
    }
    if let Some(target_exam) = target_exam {
        return format!("{target_exam} 기준으로 자동 구성된 코스");
    }

    match difficulty_start.unwrap_or_default() {
        0..=2 => "부담 없이 학습 감각을 되찾기 좋은 구성".to_string(),
        3..=4 => "기초를 마친 뒤 학습량을 늘리기 좋은 구성".to_string(),
        _ => "이미 학습 중인 사용자를 위한 심화 구성".to_string(),
    }
}

fn scalar_i64<P: rusqlite::Params>(
    conn: &Connection,
    sql: &str,
    params: P,
) -> rusqlite::Result<i64> {
    conn.query_row(sql, params, |row| row.get(0))
}
