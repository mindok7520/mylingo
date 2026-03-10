use anyhow::Context;
use chrono::{Duration, Utc};
use reqwest::{blocking::Client, Url};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;
use unicode_normalization::UnicodeNormalization;

const DEFAULT_PROFILE_KEY: &str = "default";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchLexeme {
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
pub struct LexemeSense {
    sense_order: i64,
    gloss_en: Option<String>,
    gloss_ko: Option<String>,
    gloss_detail: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LexemeExample {
    id: i64,
    sentence: String,
    sentence_reading: Option<String>,
    translation_en: Option<String>,
    match_score: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LexemeKanji {
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
pub struct LexemeDetail {
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
    generated_meaning_ko: Option<String>,
    generated_explanation_ko: Option<String>,
    generated_provider_label: Option<String>,
    senses: Vec<LexemeSense>,
    examples: Vec<LexemeExample>,
    kanji: Vec<LexemeKanji>,
    tags: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActiveSession {
    id: i64,
    mode: String,
    started_at: String,
    course_key: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardSnapshot {
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
pub struct StudyStartOption {
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
pub struct CourseMap {
    course_key: String,
    name: String,
    description: Option<String>,
    level_label: String,
    recommended_reason: String,
    units: Vec<CourseMapUnit>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CourseMapUnit {
    unit_order: i64,
    title: String,
    total_items: i64,
    learned_count: i64,
    reviewed_count: i64,
    is_completed: bool,
    is_current: bool,
    is_locked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LlmProviderSettings {
    enabled: bool,
    provider: String,
    base_url: String,
    model: String,
    api_key: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GeneratedSentenceLesson {
    sentence: String,
    translation_ko: String,
    explanation_ko: String,
    usage_tip_ko: String,
    provider_label: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KoreanMeaningHint {
    lexeme_id: i64,
    meaning_ko: String,
    explanation_ko: Option<String>,
    provider_label: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LlmConnectionStatus {
    provider_label: String,
    base_url: String,
    model_found: bool,
    message: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JapaneseBoosterPack {
    profile_key: String,
    profile_label: String,
    theme_key: String,
    theme_label: String,
    course_key: String,
    unit_title: String,
    inserted_count: usize,
    attached_existing_count: usize,
    skipped_count: usize,
    generated_lexeme_ids: Vec<i64>,
}

#[derive(Debug, Clone, Copy)]
struct JapaneseBoosterProfile {
    key: &'static str,
    course_key: &'static str,
    label: &'static str,
    description: &'static str,
    target_domain: &'static str,
    difficulty_start: i64,
    difficulty_end: i64,
    prompt_focus: &'static str,
    prompt_constraints: &'static str,
    prompt_examples: &'static str,
    prompt_avoid: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct JapaneseBoosterTheme {
    key: &'static str,
    label: &'static str,
    prompt_focus: &'static str,
    prompt_examples: &'static str,
    prompt_avoid: &'static str,
    target_item_count: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JapaneseBoosterRecommendation {
    profile_key: String,
    profile_label: String,
    theme_key: String,
    theme_label: String,
    reason: String,
    current_coverage: i64,
    target_coverage: i64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GeneratedLexemeFeedbackResult {
    lexeme_id: i64,
    rating: String,
    profile_key: Option<String>,
    theme_key: Option<String>,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeneratedJapaneseLexemeDraft {
    surface: String,
    reading: Option<String>,
    part_of_speech: String,
    meaning_ko: String,
    meaning_en: Option<String>,
    explanation_ko: Option<String>,
    sentence: Option<String>,
    sentence_reading: Option<String>,
    translation_ko: Option<String>,
}

#[derive(Debug, Clone)]
struct PromptLexeme {
    language: String,
    display_form: String,
    reading: Option<String>,
    gloss_ko: Option<String>,
    gloss_en: Option<String>,
    part_of_speech: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewQueueItem {
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
pub struct SessionState {
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
    let progress = open_progress_db().map_err(|err| err.to_string())?;
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
            (
                SELECT NULLIF(ls.gloss_en, '')
                FROM lexeme_senses ls
                WHERE ls.lexeme_id = l.id
                ORDER BY NULLIF(ls.gloss_ko, '') IS NOT NULL DESC, ls.quality_score DESC, ls.sense_order
                LIMIT 1
            ),
            (
                SELECT NULLIF(ls.gloss_ko, '')
                FROM lexeme_senses ls
                WHERE ls.lexeme_id = l.id
                ORDER BY NULLIF(ls.gloss_ko, '') IS NOT NULL DESC, ls.quality_score DESC, ls.sense_order
                LIMIT 1
            ),
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
            (
                SELECT NULLIF(ls.gloss_en, '')
                FROM lexeme_senses ls
                WHERE ls.lexeme_id = l.id
                ORDER BY NULLIF(ls.gloss_ko, '') IS NOT NULL DESC, ls.quality_score DESC, ls.sense_order
                LIMIT 1
            ),
            (
                SELECT NULLIF(ls.gloss_ko, '')
                FROM lexeme_senses ls
                WHERE ls.lexeme_id = l.id
                ORDER BY NULLIF(ls.gloss_ko, '') IS NOT NULL DESC, ls.quality_score DESC, ls.sense_order
                LIMIT 1
            ),
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

    let mut rows = rows;
    hydrate_search_results_with_korean_cache(&progress, &mut rows)
        .map_err(|err| err.to_string())?;
    Ok(rows)
}

#[tauri::command]
fn get_lexeme_detail(lexeme_id: i64) -> Result<Option<LexemeDetail>, String> {
    let conn = open_content_db().map_err(|err| err.to_string())?;
    let progress = open_progress_db().map_err(|err| err.to_string())?;

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
                    generated_meaning_ko: None,
                    generated_explanation_ko: None,
                    generated_provider_label: None,
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
    hydrate_detail_with_korean_cache(&progress, &mut detail).map_err(|err| err.to_string())?;

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
fn get_llm_settings() -> Result<LlmProviderSettings, String> {
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    load_llm_settings(&progress)
        .map(default_llm_settings)
        .map_err(|err| err.to_string())
}

#[tauri::command]
fn save_llm_settings(settings: LlmProviderSettings) -> Result<LlmProviderSettings, String> {
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    let profile_id = ensure_default_profile(&progress).map_err(|err| err.to_string())?;
    let normalized = normalize_llm_settings(settings);
    progress
        .execute(
            "
            INSERT INTO app_settings (profile_id, setting_key, setting_value)
            VALUES (?1, 'llm_provider_json', ?2)
            ON CONFLICT(profile_id, setting_key) DO UPDATE SET setting_value = excluded.setting_value
            ",
            params![profile_id, serde_json::to_string(&normalized).map_err(|err| err.to_string())?],
        )
        .map_err(|err| err.to_string())?;
    Ok(normalized)
}

#[tauri::command]
fn test_llm_settings(settings: Option<LlmProviderSettings>) -> Result<LlmConnectionStatus, String> {
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    let resolved = match settings {
        Some(value) => normalize_llm_settings(value),
        None => default_llm_settings(load_llm_settings(&progress).map_err(|err| err.to_string())?),
    };

    test_llm_connection(&resolved).map_err(|err| err.to_string())
}

#[tauri::command]
fn generate_sentence_lesson(
    lexeme_id: i64,
    support_lexeme_ids: Option<Vec<i64>>,
) -> Result<GeneratedSentenceLesson, String> {
    let content = open_content_db().map_err(|err| err.to_string())?;
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    let settings =
        default_llm_settings(load_llm_settings(&progress).map_err(|err| err.to_string())?);
    if !settings.enabled {
        return Err(
            "현재 저장된 로컬 LLM 설정이 꺼져 있다. 홈에서 LLM 연결 테스트를 다시 실행하거나 설정 저장 후 다시 시도해줘."
                .to_string(),
        );
    }

    let mut focus = load_prompt_lexeme(&content, lexeme_id).map_err(|err| err.to_string())?;
    hydrate_prompt_lexeme_with_korean_cache(&progress, lexeme_id, &mut focus)
        .map_err(|err| err.to_string())?;
    let support_lexeme_ids = support_lexeme_ids.unwrap_or_default();
    let mut support_words = Vec::new();
    for support_lexeme_id in support_lexeme_ids.into_iter().take(3) {
        if support_lexeme_id == lexeme_id {
            continue;
        }
        if let Some(mut word) = load_prompt_lexeme_optional(&content, support_lexeme_id)
            .map_err(|err| err.to_string())?
        {
            hydrate_prompt_lexeme_with_korean_cache(&progress, support_lexeme_id, &mut word)
                .map_err(|err| err.to_string())?;
            support_words.push(word);
        }
    }

    request_sentence_lesson(&settings, &focus, &support_words).map_err(|err| err.to_string())
}

#[tauri::command]
fn ensure_korean_meanings(lexeme_ids: Vec<i64>) -> Result<Vec<KoreanMeaningHint>, String> {
    let content = open_content_db().map_err(|err| err.to_string())?;
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    let settings =
        default_llm_settings(load_llm_settings(&progress).map_err(|err| err.to_string())?);

    let mut output = Vec::new();
    for lexeme_id in lexeme_ids.into_iter().take(8) {
        if let Some(hint) = ensure_korean_meaning_hint(&content, &progress, lexeme_id, &settings)
            .map_err(|err| err.to_string())?
        {
            output.push(hint);
        }
    }

    Ok(output)
}

#[tauri::command]
fn generate_japanese_booster_pack(
    profile_key: Option<String>,
    theme_key: Option<String>,
    count: Option<i64>,
) -> Result<JapaneseBoosterPack, String> {
    let mut content = open_content_db_write().map_err(|err| err.to_string())?;
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    let settings =
        default_llm_settings(load_llm_settings(&progress).map_err(|err| err.to_string())?);
    let profile = japanese_booster_profile(profile_key.as_deref());
    let theme = japanese_booster_theme(theme_key.as_deref());

    generate_japanese_booster_pack_inner(
        &mut content,
        &progress,
        &settings,
        profile,
        theme,
        count.unwrap_or(8),
    )
    .map_err(|err| err.to_string())
}

#[tauri::command]
fn recommend_japanese_booster() -> Result<JapaneseBoosterRecommendation, String> {
    let content = open_content_db().map_err(|err| err.to_string())?;
    recommend_japanese_booster_inner(&content).map_err(|err| err.to_string())
}

#[tauri::command]
fn submit_generated_lexeme_feedback(
    lexeme_id: i64,
    profile_key: Option<String>,
    theme_key: Option<String>,
    rating: String,
) -> Result<GeneratedLexemeFeedbackResult, String> {
    let progress = open_progress_db().map_err(|err| err.to_string())?;
    save_generated_lexeme_feedback(
        &progress,
        lexeme_id,
        profile_key.as_deref(),
        theme_key.as_deref(),
        &rating,
        None,
    )
    .map_err(|err| err.to_string())
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
            &progress,
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
        &progress,
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

pub fn search_lexemes_api(query: String, limit: Option<i64>) -> Result<Vec<SearchLexeme>, String> {
    search_lexemes(query, limit)
}

pub fn get_lexeme_detail_api(lexeme_id: i64) -> Result<Option<LexemeDetail>, String> {
    get_lexeme_detail(lexeme_id)
}

pub fn get_dashboard_snapshot_api() -> Result<DashboardSnapshot, String> {
    get_dashboard_snapshot()
}

pub fn get_study_starts_api() -> Result<Vec<StudyStartOption>, String> {
    get_study_starts()
}

pub fn get_course_map_api(course_key: String) -> Result<CourseMap, String> {
    get_course_map(course_key)
}

pub fn get_llm_settings_api() -> Result<LlmProviderSettings, String> {
    get_llm_settings()
}

pub fn save_llm_settings_api(settings: LlmProviderSettings) -> Result<LlmProviderSettings, String> {
    save_llm_settings(settings)
}

pub fn test_llm_settings_api(
    settings: Option<LlmProviderSettings>,
) -> Result<LlmConnectionStatus, String> {
    test_llm_settings(settings)
}

pub fn generate_sentence_lesson_api(
    lexeme_id: i64,
    support_lexeme_ids: Option<Vec<i64>>,
) -> Result<GeneratedSentenceLesson, String> {
    generate_sentence_lesson(lexeme_id, support_lexeme_ids)
}

pub fn ensure_korean_meanings_api(lexeme_ids: Vec<i64>) -> Result<Vec<KoreanMeaningHint>, String> {
    ensure_korean_meanings(lexeme_ids)
}

pub fn generate_japanese_booster_pack_api(
    profile_key: Option<String>,
    theme_key: Option<String>,
    count: Option<i64>,
) -> Result<JapaneseBoosterPack, String> {
    generate_japanese_booster_pack(profile_key, theme_key, count)
}

pub fn recommend_japanese_booster_api() -> Result<JapaneseBoosterRecommendation, String> {
    recommend_japanese_booster()
}

pub fn submit_generated_lexeme_feedback_api(
    lexeme_id: i64,
    profile_key: Option<String>,
    theme_key: Option<String>,
    rating: String,
) -> Result<GeneratedLexemeFeedbackResult, String> {
    submit_generated_lexeme_feedback(lexeme_id, profile_key, theme_key, rating)
}

pub fn start_study_session_api(
    mode: Option<String>,
    course_key: Option<String>,
) -> Result<SessionState, String> {
    start_study_session(mode, course_key)
}

pub fn finish_study_session_api(session_id: i64) -> Result<SessionState, String> {
    finish_study_session(session_id)
}

pub fn get_due_reviews_api(
    course_key: Option<String>,
    limit: Option<i64>,
) -> Result<Vec<ReviewQueueItem>, String> {
    get_due_reviews(course_key, limit)
}

pub fn submit_lexeme_review_api(
    session_id: i64,
    lexeme_id: i64,
    grade: String,
    response_time_ms: Option<i64>,
) -> Result<ReviewQueueItem, String> {
    submit_lexeme_review(session_id, lexeme_id, grade, response_time_ms)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            search_lexemes,
            get_lexeme_detail,
            get_dashboard_snapshot,
            get_study_starts,
            get_course_map,
            get_llm_settings,
            save_llm_settings,
            test_llm_settings,
            generate_sentence_lesson,
            ensure_korean_meanings,
            generate_japanese_booster_pack,
            recommend_japanese_booster,
            submit_generated_lexeme_feedback,
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

fn default_llm_settings(settings: Option<LlmProviderSettings>) -> LlmProviderSettings {
    normalize_llm_settings(settings.unwrap_or_else(|| LlmProviderSettings {
        enabled: false,
        provider: "ollama".to_string(),
        base_url: "http://127.0.0.1:11434".to_string(),
        model: "qwen2.5:3b-instruct".to_string(),
        api_key: None,
    }))
}

fn normalize_service_base_url(raw: &str, default_port: u16, default_path: Option<&str>) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }

    let with_scheme = if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    };

    match Url::parse(&with_scheme) {
        Ok(mut parsed) => {
            if parsed.port().is_none() {
                let _ = parsed.set_port(Some(default_port));
            }
            if let Some(path) = default_path {
                if parsed.path().is_empty() || parsed.path() == "/" {
                    parsed.set_path(path);
                }
            } else {
                parsed.set_path("");
            }
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string().trim_end_matches('/').to_string()
        }
        Err(_) => with_scheme.trim_end_matches('/').to_string(),
    }
}

fn ollama_base_url_candidates(base_url: &str) -> Vec<String> {
    let mut values = vec![normalize_service_base_url(base_url, 11434, None)];
    values.push("http://127.0.0.1:11434".to_string());
    values.push("http://localhost:11434".to_string());
    values.retain(|value| !value.is_empty());
    values.dedup();
    values
}

fn normalize_llm_settings(mut settings: LlmProviderSettings) -> LlmProviderSettings {
    settings.provider = settings.provider.trim().to_ascii_lowercase();
    if settings.provider.is_empty() {
        settings.provider = "ollama".to_string();
    }

    settings.base_url = settings.base_url.trim().trim_end_matches('/').to_string();
    if settings.base_url.is_empty() {
        settings.base_url = if settings.provider == "openai-compatible" {
            "http://127.0.0.1:1234/v1".to_string()
        } else {
            "http://127.0.0.1:11434".to_string()
        };
    } else {
        settings.base_url = if settings.provider == "openai-compatible" {
            normalize_service_base_url(&settings.base_url, 1234, Some("/v1"))
        } else {
            normalize_service_base_url(&settings.base_url, 11434, None)
        };
    }

    settings.model = settings.model.trim().to_string();
    if settings.model.is_empty() {
        settings.model = if settings.provider == "openai-compatible" {
            "local-model".to_string()
        } else {
            "qwen2.5:3b-instruct".to_string()
        };
    }

    settings.api_key = settings
        .api_key
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    settings
}

fn load_llm_settings(conn: &Connection) -> anyhow::Result<Option<LlmProviderSettings>> {
    let profile_id = ensure_default_profile(conn)?;
    let raw = conn
        .query_row(
            "
            SELECT setting_value
            FROM app_settings
            WHERE profile_id = ?1 AND setting_key = 'llm_provider_json'
            ",
            params![profile_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;

    raw.map(|value| serde_json::from_str(&value).map(normalize_llm_settings))
        .transpose()
        .map_err(Into::into)
}

fn llm_client() -> anyhow::Result<Client> {
    Client::builder()
        .timeout(StdDuration::from_secs(25))
        .build()
        .map_err(Into::into)
}

fn load_cached_korean_meaning(
    conn: &Connection,
    lexeme_id: i64,
) -> anyhow::Result<Option<KoreanMeaningHint>> {
    let profile_id = ensure_default_profile(conn)?;
    conn.query_row(
        "
        SELECT lexeme_id, meaning_ko, explanation_ko, provider_label
        FROM lexeme_ko_cache
        WHERE profile_id = ?1 AND lexeme_id = ?2
        ",
        params![profile_id, lexeme_id],
        |row| {
            Ok(KoreanMeaningHint {
                lexeme_id: row.get(0)?,
                meaning_ko: row.get(1)?,
                explanation_ko: row.get(2)?,
                provider_label: row.get(3)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn save_cached_korean_meaning(
    conn: &Connection,
    hint: &KoreanMeaningHint,
) -> anyhow::Result<KoreanMeaningHint> {
    let profile_id = ensure_default_profile(conn)?;
    conn.execute(
        "
        INSERT INTO lexeme_ko_cache (profile_id, lexeme_id, meaning_ko, explanation_ko, provider_label, updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(profile_id, lexeme_id) DO UPDATE SET
            meaning_ko = excluded.meaning_ko,
            explanation_ko = excluded.explanation_ko,
            provider_label = excluded.provider_label,
            updated_at = excluded.updated_at
        ",
        params![
            profile_id,
            hint.lexeme_id,
            hint.meaning_ko.as_str(),
            hint.explanation_ko.as_deref(),
            hint.provider_label.as_str(),
            Utc::now().to_rfc3339(),
        ],
    )?;
    Ok(hint.clone())
}

fn save_generated_lexeme_feedback(
    conn: &Connection,
    lexeme_id: i64,
    profile_key: Option<&str>,
    theme_key: Option<&str>,
    rating: &str,
    note: Option<&str>,
) -> anyhow::Result<GeneratedLexemeFeedbackResult> {
    if !matches!(rating, "good" | "too_easy" | "too_hard" | "inaccurate") {
        anyhow::bail!(
            "feedback rating must be one of 'good', 'too_easy', 'too_hard', or 'inaccurate'"
        );
    }

    let profile_id = ensure_default_profile(conn)?;
    conn.execute(
        "
        INSERT INTO ai_generated_lexeme_feedback (
            profile_id,
            lexeme_id,
            profile_key,
            theme_key,
            rating,
            note,
            updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(profile_id, lexeme_id) DO UPDATE SET
            profile_key = excluded.profile_key,
            theme_key = excluded.theme_key,
            rating = excluded.rating,
            note = excluded.note,
            updated_at = excluded.updated_at
        ",
        params![
            profile_id,
            lexeme_id,
            profile_key,
            theme_key,
            rating,
            note,
            Utc::now().to_rfc3339(),
        ],
    )?;

    Ok(GeneratedLexemeFeedbackResult {
        lexeme_id,
        rating: rating.to_string(),
        profile_key: profile_key.map(ToOwned::to_owned),
        theme_key: theme_key.map(ToOwned::to_owned),
        message: match rating {
            "good" => "이 단어를 좋은 AI 생성 예시로 저장했다.".to_string(),
            "too_easy" => {
                "이 단어를 너무 쉬운 카드로 저장했다. 다음 생성에서는 난도를 조금 올리겠다."
                    .to_string()
            }
            "too_hard" => {
                "이 단어를 너무 어려운 카드로 저장했다. 다음 생성에서는 난도를 조금 낮추겠다."
                    .to_string()
            }
            _ => {
                "이 단어를 뜻이 부정확한 예시로 저장했다. 다음 생성에서 의미 정확도를 더 엄격하게 보겠다."
                    .to_string()
            }
        },
    })
}

fn load_feedback_surfaces(
    content: &Connection,
    progress: &Connection,
    profile_key: Option<&str>,
    theme_key: Option<&str>,
    rating: &str,
) -> anyhow::Result<Vec<String>> {
    let profile_id = ensure_default_profile(progress)?;
    let mut stmt = progress.prepare(
        "
        SELECT lexeme_id
        FROM ai_generated_lexeme_feedback
        WHERE profile_id = ?1
          AND rating = ?2
          AND (?3 IS NULL OR profile_key = ?3)
          AND (?4 IS NULL OR theme_key = ?4)
        ORDER BY updated_at DESC
        LIMIT 6
        ",
    )?;

    let lexeme_ids = stmt
        .query_map(params![profile_id, rating, profile_key, theme_key], |row| {
            row.get::<_, i64>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut surfaces = Vec::new();
    for lexeme_id in lexeme_ids {
        if let Some(surface) = content
            .query_row(
                "SELECT display_form FROM lexemes WHERE id = ?1",
                params![lexeme_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            surfaces.push(surface);
        }
    }
    Ok(surfaces)
}

fn load_feedback_prompt_guidance(
    content: &Connection,
    progress: &Connection,
    profile_key: &str,
    theme_key: &str,
) -> anyhow::Result<String> {
    let liked = load_feedback_surfaces(
        content,
        progress,
        Some(profile_key),
        Some(theme_key),
        "good",
    )?;
    let too_easy = load_feedback_surfaces(
        content,
        progress,
        Some(profile_key),
        Some(theme_key),
        "too_easy",
    )?;
    let too_hard = load_feedback_surfaces(
        content,
        progress,
        Some(profile_key),
        Some(theme_key),
        "too_hard",
    )?;
    let inaccurate = load_feedback_surfaces(
        content,
        progress,
        Some(profile_key),
        Some(theme_key),
        "inaccurate",
    )?;

    if liked.is_empty() && too_easy.is_empty() && too_hard.is_empty() && inaccurate.is_empty() {
        return Ok("이전 생성 피드백 없음".to_string());
    }

    Ok(format!(
        "좋은 예시로 저장된 단어: {}\n너무 쉬운 예시로 저장된 단어: {}\n너무 어려운 예시로 저장된 단어: {}\n뜻이 부정확한 예시로 저장된 단어: {}",
        if liked.is_empty() {
            "없음".to_string()
        } else {
            liked.join(", ")
        },
        if too_easy.is_empty() {
            "없음".to_string()
        } else {
            too_easy.join(", ")
        },
        if too_hard.is_empty() {
            "없음".to_string()
        } else {
            too_hard.join(", ")
        },
        if inaccurate.is_empty() {
            "없음".to_string()
        } else {
            inaccurate.join(", ")
        }
    ))
}

fn generated_theme_coverage_count(
    content: &Connection,
    course_key: &str,
    theme_key: &str,
) -> anyhow::Result<i64> {
    content
        .query_row(
            "
            SELECT COUNT(ui.item_id)
            FROM course_templates ct
            JOIN course_units cu ON cu.course_id = ct.id
            JOIN unit_items ui ON ui.unit_id = cu.id AND ui.item_type = 'lexeme'
            WHERE ct.course_key = ?1
              AND cu.description LIKE ?2
            ",
            params![course_key, format!("%[theme:{}]%", theme_key)],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn recommend_japanese_booster_inner(
    content: &Connection,
) -> anyhow::Result<JapaneseBoosterRecommendation> {
    let total_japanese_lexemes: i64 = content.query_row(
        "
        SELECT COUNT(*)
        FROM lexemes l
        JOIN languages lang ON lang.id = l.language_id
        WHERE lang.code = 'ja'
        ",
        [],
        |row| row.get(0),
    )?;

    let profile = if total_japanese_lexemes < 1800 {
        japanese_booster_profile(Some("kindergarten"))
    } else if total_japanese_lexemes < 4200 {
        japanese_booster_profile(Some("jlpt-n5"))
    } else {
        japanese_booster_profile(Some("daily-conversation"))
    };

    let themes = [
        japanese_booster_theme(Some("family")),
        japanese_booster_theme(Some("school")),
        japanese_booster_theme(Some("shopping")),
        japanese_booster_theme(Some("emotions")),
    ];

    let (theme, current_coverage) = themes
        .into_iter()
        .map(|theme| {
            let count =
                generated_theme_coverage_count(content, profile.course_key, theme.key).unwrap_or(0);
            (theme, count)
        })
        .min_by_key(|(theme, count)| count * 100 / theme.target_item_count.max(1))
        .unwrap_or((japanese_booster_theme(Some("family")), 0));

    Ok(JapaneseBoosterRecommendation {
        profile_key: profile.key.to_string(),
        profile_label: profile.label.to_string(),
        theme_key: theme.key.to_string(),
        theme_label: theme.label.to_string(),
        reason: if total_japanese_lexemes < 1800 {
            format!(
                "현재 일본어 DB가 아직 얇은 편이라 {} 보강이 먼저 어울린다. {} 주제는 현재 {}/{}개라 가장 비어 있다.",
                profile.label, theme.label, current_coverage, theme.target_item_count
            )
        } else if total_japanese_lexemes < 4200 {
            format!(
                "기초 단어는 어느 정도 있지만 JLPT N5 느낌의 빈 구간이 남아 있다. {} 주제를 먼저 메우는 편이 좋다. ({}/{})",
                theme.label, current_coverage, theme.target_item_count
            )
        } else {
            format!(
                "기초 DB는 갖춰져 있어서 생활 회화 빈 주제를 메우는 편이 효율적이다. {} 주제 커버리지가 가장 낮다. ({}/{})",
                theme.label, current_coverage, theme.target_item_count
            )
        },
        current_coverage,
        target_coverage: theme.target_item_count,
    })
}

fn hydrate_search_results_with_korean_cache(
    progress: &Connection,
    rows: &mut [SearchLexeme],
) -> anyhow::Result<()> {
    for row in rows.iter_mut() {
        if row.language == "ja"
            && row
                .gloss_ko
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
        {
            if let Some(hint) = load_cached_korean_meaning(progress, row.id)? {
                row.gloss_ko = Some(hint.meaning_ko);
            }
        }
    }
    Ok(())
}

fn hydrate_review_item_with_korean_cache(
    progress: &Connection,
    item: &mut ReviewQueueItem,
) -> anyhow::Result<()> {
    if item
        .gloss_ko
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
    {
        return Ok(());
    }

    if let Some(hint) = load_cached_korean_meaning(progress, item.lexeme_id)? {
        item.gloss_ko = Some(hint.meaning_ko);
    }

    Ok(())
}

fn hydrate_detail_with_korean_cache(
    progress: &Connection,
    detail: &mut LexemeDetail,
) -> anyhow::Result<()> {
    if let Some(hint) = load_cached_korean_meaning(progress, detail.id)? {
        detail.generated_meaning_ko = Some(hint.meaning_ko.clone());
        detail.generated_explanation_ko = hint.explanation_ko.clone();
        detail.generated_provider_label = Some(hint.provider_label.clone());

        if detail.senses.iter().all(|sense| {
            sense
                .gloss_ko
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        }) {
            if let Some(first) = detail.senses.first_mut() {
                first.gloss_ko = Some(hint.meaning_ko);
            }
        }
    }

    Ok(())
}

fn hydrate_prompt_lexeme_with_korean_cache(
    progress: &Connection,
    lexeme_id: i64,
    lexeme: &mut PromptLexeme,
) -> anyhow::Result<()> {
    if lexeme
        .gloss_ko
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
    {
        return Ok(());
    }

    if let Some(hint) = load_cached_korean_meaning(progress, lexeme_id)? {
        lexeme.gloss_ko = Some(hint.meaning_ko);
    }

    Ok(())
}

fn ensure_korean_meaning_hint(
    content: &Connection,
    progress: &Connection,
    lexeme_id: i64,
    settings: &LlmProviderSettings,
) -> anyhow::Result<Option<KoreanMeaningHint>> {
    if let Some(hint) = load_cached_korean_meaning(progress, lexeme_id)? {
        return Ok(Some(hint));
    }

    let Some(prompt_lexeme) = load_prompt_lexeme_optional(content, lexeme_id)? else {
        return Ok(None);
    };

    if prompt_lexeme.language != "ja" {
        return Ok(None);
    }

    if prompt_lexeme
        .gloss_ko
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
    {
        return Ok(None);
    }

    if !settings.enabled {
        return Ok(None);
    }

    let hint = request_korean_meaning_hint(settings, lexeme_id, &prompt_lexeme)?;
    save_cached_korean_meaning(progress, &hint).map(Some)
}

fn request_korean_meaning_hint(
    settings: &LlmProviderSettings,
    lexeme_id: i64,
    focus: &PromptLexeme,
) -> anyhow::Result<KoreanMeaningHint> {
    let prompt = format!(
        "너는 일본어 초급 학습자를 돕는 한국어 사전 코치다. 반드시 JSON 객체 하나만 반환해라. 마크다운, 코드블록, 설명문 없이 JSON만 출력한다.\n\n필드: meaningKo, explanationKo\n규칙:\n1. meaningKo는 가장 대표적인 한국어 뜻 1줄\n2. explanationKo는 초급 학습자용 짧은 한국어 설명 1~2문장\n3. 일본어 단어가 여러 뜻을 가져도 초급자가 먼저 배워야 할 뜻을 우선한다\n4. 영어를 그대로 반복하지 말고 자연스러운 한국어로 바꾼다\n\n단어: {word}\n읽기: {reading}\n품사: {pos}\n영어 뜻 참고: {gloss_en}",
        word = focus.display_form.as_str(),
        reading = focus.reading.as_deref().unwrap_or("없음"),
        pos = focus.part_of_speech.as_str(),
        gloss_en = focus.gloss_en.as_deref().unwrap_or("없음"),
    );

    let provider_label = format!("{} / {}", settings.provider, settings.model);
    let raw = match settings.provider.as_str() {
        "ollama" => request_llm_json_from_ollama(settings, &prompt)
            .with_context(|| format!("Ollama 연결 실패: {}", settings.base_url))?,
        "openai-compatible" => request_llm_json_from_openai(settings, &prompt)
            .with_context(|| format!("OpenAI 호환 LLM 연결 실패: {}", settings.base_url))?,
        other => anyhow::bail!("지원하지 않는 provider: {other}"),
    };

    let value: serde_json::Value = serde_json::from_str(extract_json_object(&raw).unwrap_or(&raw))?;
    Ok(KoreanMeaningHint {
        lexeme_id,
        meaning_ko: pick_string(&value, &["meaningKo", "meaning_ko", "meaning"])
            .ok_or_else(|| anyhow::anyhow!("meaningKo 필드가 없다"))?,
        explanation_ko: pick_string(&value, &["explanationKo", "explanation_ko", "explanation"]),
        provider_label,
    })
}

fn request_llm_json_from_ollama(
    settings: &LlmProviderSettings,
    prompt: &str,
) -> anyhow::Result<String> {
    let candidates = ollama_base_url_candidates(&settings.base_url);
    let client = llm_client()?;
    let mut last_error = None;

    for base_url in &candidates {
        match client
            .post(format!("{}/api/generate", base_url))
            .json(&serde_json::json!({
                "model": settings.model,
                "prompt": prompt,
                "stream": false,
                "format": "json"
            }))
            .send()
        {
            Ok(response) => {
                let response = response
                    .error_for_status()
                    .with_context(|| format!("Ollama가 {} 모델 요청을 거부했다", settings.model))?;
                let body: serde_json::Value = response.json()?;
                return body
                    .get("response")
                    .and_then(|value| value.as_str())
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| anyhow::anyhow!("ollama 응답에서 response 필드를 찾지 못했다"));
            }
            Err(error) => {
                last_error = Some(format!("{} ({})", base_url, error));
            }
        }
    }

    anyhow::bail!(
        "현재 머신에서 Ollama에 연결하지 못했다. 저장된 주소: {}. 시도한 주소: {}. 같은 머신의 Ollama면 LLM 주소는 `http://127.0.0.1:11434` 를 쓰는 편이 가장 안전하다. 마지막 오류: {}",
        settings.base_url,
        candidates.join(", "),
        last_error.unwrap_or_else(|| "알 수 없는 오류".to_string())
    )
}

fn request_llm_json_from_openai(
    settings: &LlmProviderSettings,
    prompt: &str,
) -> anyhow::Result<String> {
    let mut request = llm_client()?
        .post(format!("{}/chat/completions", settings.base_url))
        .json(&serde_json::json!({
            "model": settings.model,
            "messages": [
                {"role": "system", "content": "한국어 설명만 제공하는 언어 학습 코치다. 사용자가 원하는 JSON 객체만 반환한다."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.4
        }));

    if let Some(api_key) = &settings.api_key {
        request = request.bearer_auth(api_key);
    }

    let body: serde_json::Value = request
        .send()
        .with_context(|| format!("현재 머신에서 {} 에 연결하지 못했다", settings.base_url))?
        .error_for_status()?
        .json()?;

    body.get("choices")
        .and_then(|value| value.as_array())
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("content"))
        .and_then(|content| content.as_str())
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow::anyhow!("openai-compatible 응답에서 content 필드를 찾지 못했다"))
}

fn test_llm_connection(settings: &LlmProviderSettings) -> anyhow::Result<LlmConnectionStatus> {
    let provider_label = format!("{} / {}", settings.provider, settings.model);
    match settings.provider.as_str() {
        "ollama" => {
            let candidates = ollama_base_url_candidates(&settings.base_url);
            let client = llm_client()?;
            let mut body = None;
            let mut connected_base_url = None;
            let mut last_error = None;

            for base_url in &candidates {
                match client.get(format!("{}/api/tags", base_url)).send() {
                    Ok(response) => match response.error_for_status() {
                        Ok(ok_response) => {
                            body = Some(ok_response.json::<serde_json::Value>()?);
                            connected_base_url = Some(base_url.clone());
                            break;
                        }
                        Err(error) => {
                            last_error = Some(format!("{} ({})", base_url, error));
                        }
                    },
                    Err(error) => {
                        last_error = Some(format!("{} ({})", base_url, error));
                    }
                }
            }

            let connected_base_url = connected_base_url.ok_or_else(|| {
                anyhow::anyhow!(
                    "현재 머신에서 Ollama에 연결하지 못했다. 저장된 주소: {}. 시도한 주소: {}. 같은 머신의 Ollama면 `http://127.0.0.1:11434` 를 써야 한다. 마지막 오류: {}",
                    settings.base_url,
                    candidates.join(", "),
                    last_error.unwrap_or_else(|| "알 수 없는 오류".to_string())
                )
            })?;

            let body = body.expect("connected base URL should produce body");

            let model_found = body
                .get("models")
                .and_then(|value| value.as_array())
                .map(|models| {
                    models.iter().any(|model| {
                        model
                            .get("name")
                            .and_then(|value| value.as_str())
                            .map(|name| name == settings.model)
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false);

            Ok(LlmConnectionStatus {
                provider_label,
                base_url: connected_base_url.clone(),
                model_found,
                message: if model_found {
                    format!(
                        "{} 에 연결했고 {} 모델도 확인했다. 입력값이 다르더라도 서버에서는 이 주소로 Ollama에 도달했다.",
                        connected_base_url, settings.model
                    )
                } else {
                    format!(
                        "{} 에는 연결했지만 {} 모델은 찾지 못했다. `ollama list` 로 모델 이름을 다시 확인해줘.",
                        connected_base_url, settings.model
                    )
                },
            })
        }
        "openai-compatible" => {
            let body: serde_json::Value = llm_client()?
                .get(format!("{}/models", settings.base_url))
                .send()
                .with_context(|| format!("현재 머신에서 {} 에 연결하지 못했다", settings.base_url))?
                .error_for_status()?
                .json()?;
            let model_found = body
                .get("data")
                .and_then(|value| value.as_array())
                .map(|models| {
                    models.iter().any(|model| {
                        model
                            .get("id")
                            .and_then(|value| value.as_str())
                            .map(|id| id == settings.model)
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false);
            Ok(LlmConnectionStatus {
                provider_label,
                base_url: settings.base_url.clone(),
                model_found,
                message: if model_found {
                    format!(
                        "{} 에 연결했고 {} 모델도 확인했다.",
                        settings.base_url, settings.model
                    )
                } else {
                    format!(
                        "{} 에는 연결했지만 {} 모델은 목록에서 찾지 못했다.",
                        settings.base_url, settings.model
                    )
                },
            })
        }
        other => anyhow::bail!("지원하지 않는 provider: {other}"),
    }
}

fn normalize_japanese_surface(value: &str) -> String {
    value
        .nfkc()
        .collect::<String>()
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .trim()
        .to_string()
}

fn normalize_generated_pos_code(value: &str) -> &str {
    match value.trim().to_ascii_lowercase().as_str() {
        "noun" | "명사" => "noun",
        "verb-ichidan" | "ichidan" | "1단동사" => "verb-ichidan",
        "verb-godan" | "godan" | "5단동사" => "verb-godan",
        "adjective-i" | "i-adjective" | "い형용사" => "adjective-i",
        "adjective-na" | "na-adjective" | "な형용사" => "adjective-na",
        "adverb" | "부사" => "adverb",
        _ => "expression",
    }
}

fn japanese_booster_profile(profile_key: Option<&str>) -> JapaneseBoosterProfile {
    match profile_key.unwrap_or("daily-conversation") {
        "kindergarten" => JapaneseBoosterProfile {
            key: "kindergarten",
            course_key: "ja-ai-kindergarten",
            label: "일본어 AI 유치원",
            description: "히라가나와 가장 쉬운 기초 생활 단어만 모은 AI 보강 코스",
            target_domain: "kindergarten",
            difficulty_start: 1,
            difficulty_end: 1,
            prompt_focus: "아주 어린 초급 학습자가 첫 100단어를 배울 때 필요한 쉬운 단어",
            prompt_constraints:
                "가능하면 히라가나 위주, 길이 4글자 안팎, 인사/가족/사물/기초 동작 중심",
            prompt_examples: "예: おはよう, みず, いぬ, たべる, いく, おおきい",
            prompt_avoid:
                "금지 예: 외래어만 있는 카타카나 단어, 사람 이름, 지명, 브랜드명, 긴 문장형 표현",
        },
        "jlpt-n5" => JapaneseBoosterProfile {
            key: "jlpt-n5",
            course_key: "ja-ai-jlpt-n5",
            label: "일본어 AI JLPT N5",
            description: "JLPT N5 감각에 맞춘 기초 단어를 보강하는 AI 코스",
            target_domain: "jlpt-n5",
            difficulty_start: 1,
            difficulty_end: 2,
            prompt_focus: "JLPT N5 초반에 자주 나오는 핵심 단어와 기초 활용",
            prompt_constraints: "명사, 기본 동사, 기초 형용사 위주, 시험/교실/일상 주제 허용",
            prompt_examples: "예: 学校, 先生, 飲む, 行く, 新しい, 時間, 雨",
            prompt_avoid:
                "금지 예: N4 이상 문어체 표현, 추상 명사만 있는 단어, 드문 한자어, 고유명사",
        },
        _ => JapaneseBoosterProfile {
            key: "daily-conversation",
            course_key: "ja-ai-daily-conversation",
            label: "일본어 AI 생활 회화",
            description: "하루 대화에서 자주 쓰는 일본어 단어를 보강하는 AI 코스",
            target_domain: "daily-conversation",
            difficulty_start: 1,
            difficulty_end: 2,
            prompt_focus: "집, 학교, 가게, 이동, 감정 표현에서 자주 쓰는 생활 회화 단어",
            prompt_constraints:
                "회화에서 바로 쓰기 쉬운 단어 우선, 지나치게 문어체이거나 드문 단어 금지",
            prompt_examples: "예: こんにちは, ありがとう, まつ, つかう, 近い, すぐ, ほんとう",
            prompt_avoid:
                "금지 예: 전문용어, 고유명사, 브랜드명, 외래어 남발, 지나치게 딱딱한 서면어",
        },
    }
}

fn japanese_booster_theme(theme_key: Option<&str>) -> JapaneseBoosterTheme {
    match theme_key.unwrap_or("family") {
        "school" => JapaneseBoosterTheme {
            key: "school",
            label: "학교",
            prompt_focus: "학교, 수업, 숙제, 시간표, 선생님, 친구와 관련된 단어",
            prompt_examples: "예: せんせい, きょうしつ, つくえ, ノート, やすみ, べんきょう",
            prompt_avoid: "금지 예: 대학 전공용 전문어, 너무 어려운 행정용어, 고유명사",
            target_item_count: 24,
        },
        "shopping" => JapaneseBoosterTheme {
            key: "shopping",
            label: "쇼핑",
            prompt_focus: "가게, 물건, 계산, 가격, 주문, 음식 구매와 관련된 단어",
            prompt_examples: "예: みせ, やすい, たかい, かう, りんご, おかね",
            prompt_avoid: "금지 예: 브랜드명, 전문 상품명, 드문 외래어",
            target_item_count: 24,
        },
        "emotions" => JapaneseBoosterTheme {
            key: "emotions",
            label: "감정",
            prompt_focus: "좋다, 싫다, 기쁘다, 피곤하다 같은 기본 감정과 상태 표현 단어",
            prompt_examples: "예: うれしい, かなしい, こわい, すき, きらい, つかれる",
            prompt_avoid: "금지 예: 추상 심리학 용어, 지나치게 문학적인 감정 표현",
            target_item_count: 20,
        },
        _ => JapaneseBoosterTheme {
            key: "family",
            label: "가족/집",
            prompt_focus: "가족, 집, 몸, 식사, 아침 준비처럼 매일 집에서 쓰는 단어",
            prompt_examples: "예: おかあさん, いえ, みず, ごはん, ねる, あさ",
            prompt_avoid: "금지 예: 친척 호칭을 과하게 세분화한 드문 단어, 옛말, 이름",
            target_item_count: 24,
        },
    }
}

fn is_valid_generated_surface(surface: &str) -> bool {
    let normalized = normalize_japanese_surface(surface);
    let length = normalized.chars().count();
    length >= 1
        && length <= 8
        && normalized
            .chars()
            .any(|ch| matches!(ch as u32, 0x3040..=0x30ff | 0x4e00..=0x9faf))
}

fn is_valid_generated_reading(reading: &str) -> bool {
    let trimmed = reading.trim();
    !trimmed.is_empty()
        && trimmed.chars().all(|ch| {
            matches!(ch as u32, 0x3040..=0x309f | 0x30a0..=0x30ff) || ch == ' ' || ch == 'ー'
        })
}

fn contains_ascii_wording(value: &str) -> bool {
    value.chars().filter(|ch| ch.is_ascii_alphabetic()).count() >= 4
}

fn is_hiragana_text(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| matches!(ch as u32, 0x3040..=0x309f) || ch == ' ' || ch == 'ー')
}

fn is_valid_generated_japanese_draft(
    profile: JapaneseBoosterProfile,
    draft: &GeneratedJapaneseLexemeDraft,
) -> bool {
    let sentence_ok = draft
        .sentence
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|sentence| sentence.contains(draft.surface.trim()) && sentence.chars().count() <= 24)
        .unwrap_or(false);

    let translation_ok = draft
        .translation_ko
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();

    let korean_text_ok = !contains_ascii_wording(&draft.meaning_ko)
        && !contains_ascii_wording(draft.explanation_ko.as_deref().unwrap_or(""))
        && !contains_ascii_wording(draft.translation_ko.as_deref().unwrap_or(""));

    let profile_specific_ok = match profile.key {
        "kindergarten" => {
            normalize_japanese_surface(&draft.surface).chars().count() <= 4
                && is_hiragana_text(draft.reading.as_deref().unwrap_or(""))
                && draft
                    .sentence
                    .as_deref()
                    .map(|sentence| sentence.chars().count() <= 14)
                    .unwrap_or(false)
        }
        "jlpt-n5" => normalize_japanese_surface(&draft.surface).chars().count() <= 6,
        _ => true,
    };

    is_valid_generated_surface(&draft.surface)
        && draft
            .reading
            .as_deref()
            .map(is_valid_generated_reading)
            .unwrap_or(false)
        && !draft.meaning_ko.trim().is_empty()
        && !draft
            .explanation_ko
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
        && sentence_ok
        && translation_ok
        && korean_text_ok
        && profile_specific_ok
}

fn lookup_required_id(
    conn: &Connection,
    table: &str,
    code_column: &str,
    code: &str,
) -> anyhow::Result<i64> {
    conn.query_row(
        &format!("SELECT id FROM {table} WHERE {code_column} = ?1"),
        params![code],
        |row| row.get(0),
    )
    .with_context(|| format!("{} 에서 {}={} 값을 찾지 못했다", table, code_column, code))
}

fn ensure_generated_course(
    conn: &Connection,
    language_id: i64,
    profile: JapaneseBoosterProfile,
) -> anyhow::Result<i64> {
    conn.execute(
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
        ) VALUES (?1, ?2, ?3, ?4, 'ai-generated', NULL, ?5, ?6, ?7, 1)
        ON CONFLICT(course_key) DO UPDATE SET
            name = excluded.name,
            description = excluded.description,
            category = excluded.category,
            target_domain = excluded.target_domain,
            difficulty_start = excluded.difficulty_start,
            difficulty_end = excluded.difficulty_end,
            auto_generated = excluded.auto_generated
        ",
        params![
            language_id,
            profile.course_key,
            profile.label,
            profile.description,
            profile.target_domain,
            profile.difficulty_start,
            profile.difficulty_end,
        ],
    )?;

    conn.query_row(
        "SELECT id FROM course_templates WHERE course_key = ?1",
        params![profile.course_key],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn create_generated_unit(
    conn: &Connection,
    course_id: i64,
    profile: JapaneseBoosterProfile,
    theme: JapaneseBoosterTheme,
) -> anyhow::Result<(i64, String)> {
    let next_order: i64 = conn.query_row(
        "SELECT COALESCE(MAX(unit_order), 0) + 1 FROM course_units WHERE course_id = ?1",
        params![course_id],
        |row| row.get(0),
    )?;
    let title = format!("{} · {} {next_order}", profile.label, theme.label);
    conn.execute(
        "
        INSERT INTO course_units (course_id, unit_order, title, description)
        VALUES (?1, ?2, ?3, ?4)
        ",
        params![
            course_id,
            next_order,
            title,
            format!("{} [theme:{}]", profile.description, theme.key),
        ],
    )?;
    Ok((conn.last_insert_rowid(), title))
}

fn attach_lexeme_to_unit(
    conn: &Connection,
    unit_id: i64,
    lexeme_id: i64,
    item_order: i64,
) -> anyhow::Result<bool> {
    let inserted = conn.execute(
        "
        INSERT OR IGNORE INTO unit_items (unit_id, item_type, item_id, item_order)
        VALUES (?1, 'lexeme', ?2, ?3)
        ",
        params![unit_id, lexeme_id, item_order],
    )?;
    Ok(inserted > 0)
}

fn course_contains_lexeme(
    conn: &Connection,
    course_id: i64,
    lexeme_id: i64,
) -> anyhow::Result<bool> {
    let count: i64 = conn.query_row(
        "
        SELECT COUNT(*)
        FROM course_units cu
        JOIN unit_items ui ON ui.unit_id = cu.id AND ui.item_type = 'lexeme'
        WHERE cu.course_id = ?1 AND ui.item_id = ?2
        ",
        params![course_id, lexeme_id],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn find_existing_lexeme_id(
    conn: &Connection,
    language_id: i64,
    normalized_surface: &str,
) -> anyhow::Result<Option<i64>> {
    conn.query_row(
        "
        SELECT id
        FROM lexemes
        WHERE language_id = ?1 AND lemma_normalized = ?2
        ORDER BY quality_score DESC, id
        LIMIT 1
        ",
        params![language_id, normalized_surface],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

fn ensure_example_translation_ko(
    conn: &Connection,
    example_id: i64,
    ko_language_id: Option<i64>,
    translation_ko: Option<&str>,
) -> anyhow::Result<()> {
    if let (Some(language_id), Some(text)) = (
        ko_language_id,
        translation_ko
            .map(str::trim)
            .filter(|value| !value.is_empty()),
    ) {
        conn.execute(
            "
            INSERT OR IGNORE INTO example_translations (example_id, language_id, translation_text, is_primary)
            VALUES (?1, ?2, ?3, 1)
            ",
            params![example_id, language_id, text],
        )?;
    }
    Ok(())
}

fn insert_generated_lexeme(
    conn: &Connection,
    language_id: i64,
    ko_language_id: Option<i64>,
    pos_id: i64,
    draft: &GeneratedJapaneseLexemeDraft,
) -> anyhow::Result<i64> {
    let normalized_surface = normalize_japanese_surface(&draft.surface);
    conn.execute(
        "
        INSERT INTO lexemes (
            language_id,
            lemma,
            lemma_normalized,
            display_form,
            reading,
            pronunciation,
            primary_pos_id,
            quality_score,
            is_ai_enriched,
            is_verified,
            updated_at
        ) VALUES (?1, ?2, ?3, ?2, ?4, ?4, ?5, 0.42, 1, 0, ?6)
        ",
        params![
            language_id,
            draft.surface.trim(),
            normalized_surface,
            draft.reading.as_deref().map(str::trim),
            pos_id,
            Utc::now().to_rfc3339(),
        ],
    )?;
    let lexeme_id = conn.last_insert_rowid();

    conn.execute(
        "
        INSERT INTO lexeme_senses (lexeme_id, sense_order, gloss_ko, gloss_en, gloss_detail, quality_score)
        VALUES (?1, 1, ?2, ?3, ?4, 0.55)
        ",
        params![
            lexeme_id,
            draft.meaning_ko.trim(),
            draft.meaning_en.as_deref().map(str::trim),
            draft.explanation_ko.as_deref().map(str::trim),
        ],
    )?;

    conn.execute(
        "
        INSERT INTO lexeme_search (lexeme_id, surface, reading, gloss_ko, gloss_en)
        VALUES (?1, ?2, COALESCE(?3, ''), ?4, COALESCE(?5, ''))
        ",
        params![
            lexeme_id,
            draft.surface.trim(),
            draft.reading.as_deref().map(str::trim),
            draft.meaning_ko.trim(),
            draft.meaning_en.as_deref().map(str::trim),
        ],
    )?;

    if let Some(sentence) = draft
        .sentence
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let normalized_sentence = sentence.nfkc().collect::<String>();
        let example_id = conn
            .query_row(
                "SELECT id FROM examples WHERE sentence_normalized = ?1",
                params![normalized_sentence],
                |row| row.get(0),
            )
            .optional()?;

        let example_id = if let Some(id) = example_id {
            id
        } else {
            conn.execute(
                "
                INSERT INTO examples (language_id, sentence, sentence_normalized, sentence_reading, difficulty_level, quality_score)
                VALUES (?1, ?2, ?3, ?4, 1, 0.45)
                ",
                params![language_id, sentence, normalized_sentence, draft.sentence_reading.as_deref().map(str::trim)],
            )?;
            conn.last_insert_rowid()
        };

        ensure_example_translation_ko(
            conn,
            example_id,
            ko_language_id,
            draft.translation_ko.as_deref(),
        )?;
        conn.execute(
            "
            INSERT OR IGNORE INTO lexeme_examples (lexeme_id, example_id, match_score)
            VALUES (?1, ?2, 0.65)
            ",
            params![lexeme_id, example_id],
        )?;
    }

    Ok(lexeme_id)
}

fn parse_generated_japanese_drafts(raw: &str) -> anyhow::Result<Vec<GeneratedJapaneseLexemeDraft>> {
    let json_text = extract_json_object(raw).unwrap_or(raw).trim();
    let value: serde_json::Value = serde_json::from_str(json_text)?;
    let entries = if let Some(array) = value.as_array() {
        array.clone()
    } else {
        value
            .get("words")
            .and_then(|entry| entry.as_array())
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("words 배열이 없다"))?
    };

    entries
        .into_iter()
        .map(serde_json::from_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn request_japanese_booster_drafts(
    settings: &LlmProviderSettings,
    profile: JapaneseBoosterProfile,
    theme: JapaneseBoosterTheme,
    feedback_guidance: &str,
    count: i64,
) -> anyhow::Result<Vec<GeneratedJapaneseLexemeDraft>> {
    let prompt = format!(
        "너는 모바일 일본어 학습앱의 단어 코스를 편집하는 전문 교사다. 반드시 JSON 객체 하나만 반환해라. 코드블록, 마크다운, 설명문 없이 JSON만 출력한다.\n\n형식: {{\"words\":[...]}}\nwords 각 항목 필드: surface, reading, partOfSpeech, meaningKo, meaningEn, explanationKo, sentence, sentenceReading, translationKo\n\n이번 코스 성격: {focus}\n세부 제약: {constraints}\n세부 주제: {theme_focus}\n주제 예시: {theme_examples}\n권장 단어 예시: {examples}\n반드시 피할 것: {avoid}\n주제에서 피할 것: {theme_avoid}\n이전 사용자 피드백: {feedback_guidance}\n\n규칙:\n1. 총 {count}개\n2. 서로 중복 금지\n3. 모두 초급 학습자가 오늘 바로 써먹을 수 있는 단어\n4. partOfSpeech 는 noun, verb-ichidan, verb-godan, adjective-i, adjective-na, adverb, expression 중 하나만 사용\n5. surface 는 1~8글자, 일본어 문자만 사용\n6. reading 은 반드시 일본어 읽기로 채우고 비우지 말 것\n7. meaningKo 는 짧고 자연스러운 한국어 대표 뜻 1개\n8. explanationKo 는 초급 학습자가 헷갈리지 않게 설명하는 한국어 1~2문장\n9. sentence 는 surface 가 실제로 들어간 아주 짧은 일본어 예문\n10. sentenceReading 과 translationKo 도 반드시 채울 것\n11. 외래어, 고유명사, 너무 문어체인 단어, 희귀어 금지\n12. 영어 알파벳 섞인 설명문 금지\n13. JSON 외 텍스트 금지"
        ,
        focus = profile.prompt_focus,
        constraints = profile.prompt_constraints,
        theme_focus = theme.prompt_focus,
        theme_examples = theme.prompt_examples,
        examples = profile.prompt_examples,
        avoid = profile.prompt_avoid,
        theme_avoid = theme.prompt_avoid,
        feedback_guidance = feedback_guidance,
    );

    let raw = match settings.provider.as_str() {
        "ollama" => request_llm_json_from_ollama(settings, &prompt)?,
        "openai-compatible" => request_llm_json_from_openai(settings, &prompt)?,
        other => anyhow::bail!("지원하지 않는 provider: {other}"),
    };

    parse_generated_japanese_drafts(&raw)
}

fn generate_japanese_booster_pack_inner(
    content: &mut Connection,
    progress: &Connection,
    settings: &LlmProviderSettings,
    profile: JapaneseBoosterProfile,
    theme: JapaneseBoosterTheme,
    count: i64,
) -> anyhow::Result<JapaneseBoosterPack> {
    if !settings.enabled {
        anyhow::bail!("로컬 LLM이 꺼져 있다. 설정 페이지에서 먼저 켜줘.");
    }

    let count = count.clamp(4, 12);
    let feedback_guidance =
        load_feedback_prompt_guidance(content, progress, profile.key, theme.key)?;
    let drafts =
        request_japanese_booster_drafts(settings, profile, theme, &feedback_guidance, count * 2)?;
    let provider_label = format!("{} / {}", settings.provider, settings.model);
    let language_id = lookup_required_id(content, "languages", "code", "ja")?;
    let ko_language_id = content
        .query_row("SELECT id FROM languages WHERE code = 'ko'", [], |row| {
            row.get(0)
        })
        .optional()?;

    let tx = content.transaction()?;
    let course_id = ensure_generated_course(&tx, language_id, profile)?;
    let (unit_id, unit_title) = create_generated_unit(&tx, course_id, profile, theme)?;

    let mut inserted_count = 0usize;
    let mut attached_existing_count = 0usize;
    let mut skipped_count = 0usize;
    let mut item_order = 1i64;
    let mut generated_lexeme_ids = Vec::new();
    let mut seen = HashSet::new();

    for draft in drafts {
        if inserted_count + attached_existing_count >= count as usize {
            break;
        }

        let surface = normalize_japanese_surface(&draft.surface);
        if !is_valid_generated_japanese_draft(profile, &draft)
            || surface.is_empty()
            || !seen.insert(surface.clone())
        {
            skipped_count += 1;
            continue;
        }

        let pos_code = normalize_generated_pos_code(&draft.part_of_speech);
        let pos_id = lookup_required_id(&tx, "pos_tags", "code", pos_code)?;

        let lexeme_id =
            if let Some(existing_id) = find_existing_lexeme_id(&tx, language_id, &surface)? {
                if course_contains_lexeme(&tx, course_id, existing_id)? {
                    skipped_count += 1;
                    continue;
                }
                let attached = attach_lexeme_to_unit(&tx, unit_id, existing_id, item_order)?;
                if attached {
                    attached_existing_count += 1;
                    item_order += 1;
                } else {
                    skipped_count += 1;
                    continue;
                }
                existing_id
            } else {
                let created_id =
                    insert_generated_lexeme(&tx, language_id, ko_language_id, pos_id, &draft)?;
                attach_lexeme_to_unit(&tx, unit_id, created_id, item_order)?;
                inserted_count += 1;
                item_order += 1;
                created_id
            };

        save_cached_korean_meaning(
            progress,
            &KoreanMeaningHint {
                lexeme_id,
                meaning_ko: draft.meaning_ko.trim().to_string(),
                explanation_ko: draft
                    .explanation_ko
                    .as_deref()
                    .map(str::trim)
                    .map(ToOwned::to_owned),
                provider_label: provider_label.clone(),
            },
        )?;
        generated_lexeme_ids.push(lexeme_id);
    }

    tx.commit()?;

    Ok(JapaneseBoosterPack {
        profile_key: profile.key.to_string(),
        profile_label: profile.label.to_string(),
        theme_key: theme.key.to_string(),
        theme_label: theme.label.to_string(),
        course_key: profile.course_key.to_string(),
        unit_title,
        inserted_count,
        attached_existing_count,
        skipped_count,
        generated_lexeme_ids,
    })
}

fn load_prompt_lexeme(conn: &Connection, lexeme_id: i64) -> anyhow::Result<PromptLexeme> {
    load_prompt_lexeme_optional(conn, lexeme_id)?
        .ok_or_else(|| anyhow::anyhow!("lexeme not found: {lexeme_id}"))
}

fn load_prompt_lexeme_optional(
    conn: &Connection,
    lexeme_id: i64,
) -> anyhow::Result<Option<PromptLexeme>> {
    Ok(conn
        .query_row(
            "
            SELECT
                lang.code,
                l.display_form,
                l.reading,
                pos.display_name,
                (
                    SELECT NULLIF(ls.gloss_ko, '')
                    FROM lexeme_senses ls
                    WHERE ls.lexeme_id = l.id
                    ORDER BY NULLIF(ls.gloss_ko, '') IS NOT NULL DESC, ls.quality_score DESC, ls.sense_order
                    LIMIT 1
                ),
                (
                    SELECT NULLIF(ls.gloss_en, '')
                    FROM lexeme_senses ls
                    WHERE ls.lexeme_id = l.id
                    ORDER BY NULLIF(ls.gloss_ko, '') IS NOT NULL DESC, ls.quality_score DESC, ls.sense_order
                    LIMIT 1
                )
            FROM lexemes l
            JOIN languages lang ON lang.id = l.language_id
            JOIN pos_tags pos ON pos.id = l.primary_pos_id
            LEFT JOIN lexeme_senses s ON s.lexeme_id = l.id
            WHERE l.id = ?1
            GROUP BY l.id
            ",
            params![lexeme_id],
            |row| {
                Ok(PromptLexeme {
                    language: row.get(0)?,
                    display_form: row.get(1)?,
                    reading: row.get(2)?,
                    part_of_speech: row.get(3)?,
                    gloss_ko: row.get(4)?,
                    gloss_en: row.get(5)?,
                })
            },
        )
        .optional()?)
}

fn request_sentence_lesson(
    settings: &LlmProviderSettings,
    focus: &PromptLexeme,
    support_words: &[PromptLexeme],
) -> anyhow::Result<GeneratedSentenceLesson> {
    let support_text = if support_words.is_empty() {
        "없음".to_string()
    } else {
        support_words
            .iter()
            .map(|word| {
                format!(
                    "{} ({})",
                    word.display_form.as_str(),
                    word.gloss_ko
                        .as_deref()
                        .or_else(|| word.gloss_en.as_deref())
                        .unwrap_or("뜻 정보 없음")
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    };

    let prompt = format!(
        "너는 한국어 설명만 제공하는 언어 학습 코치다.\n목표 언어: {language}\n집중 단어: {word}\n읽기: {reading}\n품사: {pos}\n한국어 뜻: {gloss_ko}\n영어 뜻 참고: {gloss_en}\n같이 써도 되는 이미 배운 단어: {support_text}\n\n반드시 JSON 객체 하나만 반환해라. 마크다운, 설명문, 코드블록 없이 JSON만 출력한다.\n필드: sentence, translationKo, explanationKo, usageTipKo\n규칙:\n1. sentence는 목표 언어 문장 1개\n2. translationKo는 자연스러운 한국어 해석\n3. explanationKo는 초급 학습자용 한국어 설명\n4. usageTipKo는 짧은 한국어 학습 팁\n5. sentence에는 집중 단어가 반드시 포함되어야 한다\n6. 가능하면 이미 배운 단어를 1~2개 자연스럽게 섞는다\n7. 너무 어렵거나 긴 문장은 피한다",
        language = match focus.language.as_str() {
            "ja" => "일본어",
            "en" => "영어",
            _ => &focus.language,
        },
        word = focus.display_form.as_str(),
        reading = focus.reading.as_deref().unwrap_or("없음"),
        pos = focus.part_of_speech.as_str(),
        gloss_ko = focus.gloss_ko.as_deref().unwrap_or("없음"),
        gloss_en = focus.gloss_en.as_deref().unwrap_or("없음"),
    );

    let provider_label = format!("{} / {}", settings.provider, settings.model);
    match settings.provider.as_str() {
        "ollama" => request_sentence_lesson_from_ollama(settings, &prompt, provider_label),
        "openai-compatible" => {
            request_sentence_lesson_from_openai(settings, &prompt, provider_label)
        }
        other => anyhow::bail!("지원하지 않는 provider: {other}"),
    }
}

fn request_sentence_lesson_from_ollama(
    settings: &LlmProviderSettings,
    prompt: &str,
    provider_label: String,
) -> anyhow::Result<GeneratedSentenceLesson> {
    let text = request_llm_json_from_ollama(settings, prompt)?;
    parse_generated_sentence_lesson(&text, provider_label)
}

fn request_sentence_lesson_from_openai(
    settings: &LlmProviderSettings,
    prompt: &str,
    provider_label: String,
) -> anyhow::Result<GeneratedSentenceLesson> {
    let text = request_llm_json_from_openai(settings, prompt)?;
    parse_generated_sentence_lesson(&text, provider_label)
}

fn parse_generated_sentence_lesson(
    raw: &str,
    provider_label: String,
) -> anyhow::Result<GeneratedSentenceLesson> {
    let json_text = extract_json_object(raw).unwrap_or(raw).trim();
    let value: serde_json::Value = serde_json::from_str(json_text)?;

    Ok(GeneratedSentenceLesson {
        sentence: pick_string(&value, &["sentence"])
            .ok_or_else(|| anyhow::anyhow!("sentence 필드가 없다"))?,
        translation_ko: pick_string(&value, &["translationKo", "translation_ko", "translation"])
            .ok_or_else(|| anyhow::anyhow!("translationKo 필드가 없다"))?,
        explanation_ko: pick_string(&value, &["explanationKo", "explanation_ko", "explanation"])
            .ok_or_else(|| anyhow::anyhow!("explanationKo 필드가 없다"))?,
        usage_tip_ko: pick_string(&value, &["usageTipKo", "usage_tip_ko", "tip"]).unwrap_or_else(
            || {
                "문장을 소리 내어 읽고 핵심 단어가 문장에서 어떻게 쓰이는지 같이 확인해보자."
                    .to_string()
            },
        ),
        provider_label,
    })
}

fn pick_string(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(|entry| entry.as_str())
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn extract_json_object(raw: &str) -> Option<&str> {
    let start = raw.find('{')?;
    let end = raw.rfind('}')?;
    raw.get(start..=end)
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
        "ja-ai-kindergarten" => 3,
        "ja-ai-jlpt-n5" => 4,
        "ja-ai-daily-conversation" => 5,
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
            progress,
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
    progress: &Connection,
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
                (
                    SELECT NULLIF(ls.gloss_en, '')
                    FROM lexeme_senses ls
                    WHERE ls.lexeme_id = l.id
                    ORDER BY NULLIF(ls.gloss_ko, '') IS NOT NULL DESC, ls.quality_score DESC, ls.sense_order
                    LIMIT 1
                ),
                (
                    SELECT NULLIF(ls.gloss_ko, '')
                    FROM lexeme_senses ls
                    WHERE ls.lexeme_id = l.id
                    ORDER BY NULLIF(ls.gloss_ko, '') IS NOT NULL DESC, ls.quality_score DESC, ls.sense_order
                    LIMIT 1
                )
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
        .optional()?
        .map(|mut item| -> anyhow::Result<ReviewQueueItem> {
            hydrate_review_item_with_korean_cache(progress, &mut item)?;
            Ok(item)
        })
        .transpose()?)
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

fn open_content_db_write() -> anyhow::Result<Connection> {
    let path = if let Ok(path) = std::env::var("LINGUAFORGE_CONTENT_DB") {
        PathBuf::from(path)
    } else {
        workspace_root()?.join("db/content.db")
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
    conn.execute_batch(include_str!("../../../../sql/content_init.sql"))?;
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
        ("ja-ai-kindergarten", _) => "AI 유치원".to_string(),
        ("ja-ai-jlpt-n5", _) => "AI JLPT N5".to_string(),
        ("ja-ai-daily-conversation", _) => "AI 생활 회화".to_string(),
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
    if course_key == "ja-ai-kindergarten" {
        return "DB가 비었을 때 히라가나 중심의 쉬운 단어를 빠르게 보강하는 AI 코스".to_string();
    }
    if course_key == "ja-ai-jlpt-n5" {
        return "JLPT N5 감각에 맞춰 기초 단어를 메꾸고 싶을 때 좋은 AI 코스".to_string();
    }
    if course_key == "ja-ai-daily-conversation" {
        return "생활 회화에서 바로 쓰는 단어를 더 채우고 싶을 때 좋은 AI 코스".to_string();
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
