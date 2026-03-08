use crate::QualityReportCommand;
use crate::migrate::{migrate_content_db, migrate_progress_db, open_db};
use anyhow::Result;
use linguaforge_core::workspace::WorkspaceLayout;
use rusqlite::params;
use serde_json::json;

pub async fn run_quality_report(
    workspace: &WorkspaceLayout,
    command: QualityReportCommand,
) -> Result<()> {
    migrate_content_db(workspace)?;
    migrate_progress_db(workspace)?;

    let content = open_db(&workspace.content_db_path())?;
    let progress = open_db(&workspace.progress_db_path())?;

    let report = json!({
        "lexemes": {
            "total": scalar_i64(&content, "SELECT COUNT(*) FROM lexemes", params![] )?,
            "ja": scalar_i64(&content, "SELECT COUNT(*) FROM lexemes l JOIN languages lang ON lang.id = l.language_id WHERE lang.code = 'ja'", params![] )?,
            "en": scalar_i64(&content, "SELECT COUNT(*) FROM lexemes l JOIN languages lang ON lang.id = l.language_id WHERE lang.code = 'en'", params![] )?,
            "with_senses": scalar_i64(&content, "SELECT COUNT(DISTINCT lexeme_id) FROM lexeme_senses", params![] )?,
            "with_examples": scalar_i64(&content, "SELECT COUNT(DISTINCT lexeme_id) FROM lexeme_examples", params![] )?,
            "with_frequency": scalar_i64(&content, "SELECT COUNT(*) FROM lexemes WHERE frequency_rank IS NOT NULL", params![] )?,
            "with_jlpt": scalar_i64(&content, "SELECT COUNT(*) FROM lexemes WHERE jlpt_level IS NOT NULL", params![] )?,
            "with_cefr": scalar_i64(&content, "SELECT COUNT(*) FROM lexemes WHERE cefr_level IS NOT NULL", params![] )?,
            "with_reading": scalar_i64(&content, "SELECT COUNT(*) FROM lexemes WHERE reading IS NOT NULL AND reading <> ''", params![] )?,
            "low_quality_below_05": scalar_i64(&content, "SELECT COUNT(*) FROM lexemes WHERE quality_score < 0.5", params![] )?,
        },
        "kanji": {
            "total": scalar_i64(&content, "SELECT COUNT(*) FROM kanji", params![] )?,
            "with_readings": scalar_i64(&content, "SELECT COUNT(DISTINCT kanji_id) FROM kanji_readings", params![] )?,
            "with_meanings": scalar_i64(&content, "SELECT COUNT(DISTINCT kanji_id) FROM kanji_meanings", params![] )?,
            "linked_to_lexemes": scalar_i64(&content, "SELECT COUNT(DISTINCT kanji_id) FROM kanji_lexemes", params![] )?,
        },
        "examples": {
            "total": scalar_i64(&content, "SELECT COUNT(*) FROM examples", params![] )?,
            "with_translations": scalar_i64(&content, "SELECT COUNT(DISTINCT example_id) FROM example_translations", params![] )?,
            "linked_lexeme_examples": scalar_i64(&content, "SELECT COUNT(*) FROM lexeme_examples", params![] )?,
            "daily_domain": scalar_i64(&content, "SELECT COUNT(*) FROM examples e JOIN domains d ON d.id = e.domain_id WHERE d.code = 'daily'", params![] )?,
        },
        "courses": {
            "templates": scalar_i64(&content, "SELECT COUNT(*) FROM course_templates", params![] )?,
            "units": scalar_i64(&content, "SELECT COUNT(*) FROM course_units", params![] )?,
            "items": scalar_i64(&content, "SELECT COUNT(*) FROM unit_items", params![] )?,
        },
        "progress": {
            "profiles": scalar_i64(&progress, "SELECT COUNT(*) FROM profiles", params![] )?,
            "review_items": scalar_i64(&progress, "SELECT COUNT(*) FROM review_items", params![] )?,
            "srs_state": scalar_i64(&progress, "SELECT COUNT(*) FROM srs_state", params![] )?,
            "review_events": scalar_i64(&progress, "SELECT COUNT(*) FROM review_events", params![] )?,
            "study_sessions": scalar_i64(&progress, "SELECT COUNT(*) FROM study_sessions", params![] )?,
        }
    });

    if command.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Quality Report");
        println!(
            "- lexemes total: {} (ja {}, en {})",
            report["lexemes"]["total"], report["lexemes"]["ja"], report["lexemes"]["en"]
        );
        println!(
            "- lexemes with senses/examples: {}/{}",
            report["lexemes"]["with_senses"], report["lexemes"]["with_examples"]
        );
        println!(
            "- lexemes with frequency/jlpt/cefr: {}/{}/{}",
            report["lexemes"]["with_frequency"],
            report["lexemes"]["with_jlpt"],
            report["lexemes"]["with_cefr"]
        );
        println!(
            "- low quality lexemes (<0.5): {}",
            report["lexemes"]["low_quality_below_05"]
        );
        println!(
            "- kanji total/linked: {}/{}",
            report["kanji"]["total"], report["kanji"]["linked_to_lexemes"]
        );
        println!(
            "- examples total/translated/linked: {}/{}/{}",
            report["examples"]["total"],
            report["examples"]["with_translations"],
            report["examples"]["linked_lexeme_examples"]
        );
        println!(
            "- courses templates/units/items: {}/{}/{}",
            report["courses"]["templates"], report["courses"]["units"], report["courses"]["items"]
        );
        println!(
            "- progress profiles/review items/events: {}/{}/{}",
            report["progress"]["profiles"],
            report["progress"]["review_items"],
            report["progress"]["review_events"]
        );
    }

    Ok(())
}

fn scalar_i64<P: rusqlite::Params>(
    conn: &rusqlite::Connection,
    sql: &str,
    params: P,
) -> Result<i64> {
    Ok(conn.query_row(sql, params, |row| row.get(0))?)
}
