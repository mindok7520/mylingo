use crate::{DatabaseTarget, MigrateCommand};
use anyhow::{Context, Result};
use linguaforge_core::workspace::WorkspaceLayout;
use rusqlite::{Connection, params};
use std::fs;
use std::path::Path;

pub async fn run_migrate(workspace: &WorkspaceLayout, command: MigrateCommand) -> Result<()> {
    match command.target {
        DatabaseTarget::All => {
            migrate_content_db(workspace)?;
            migrate_progress_db(workspace)?;
        }
        DatabaseTarget::Content => migrate_content_db(workspace)?,
        DatabaseTarget::Progress => migrate_progress_db(workspace)?,
    }

    Ok(())
}

pub(crate) fn migrate_content_db(workspace: &WorkspaceLayout) -> Result<()> {
    let db_path = workspace.content_db_path();
    let sql_path = workspace.sql_path("content_init.sql");
    let conn = open_db(&db_path)?;
    apply_sql_file(&conn, &sql_path)?;
    seed_content_db(&conn)?;
    println!("migrated content DB -> {}", db_path.display());
    Ok(())
}

pub(crate) fn migrate_progress_db(workspace: &WorkspaceLayout) -> Result<()> {
    let db_path = workspace.progress_db_path();
    let sql_path = workspace.sql_path("progress_init.sql");
    let conn = open_db(&db_path)?;
    apply_sql_file(&conn, &sql_path)?;
    seed_progress_db(&conn)?;
    println!("migrated progress DB -> {}", db_path.display());
    Ok(())
}

pub(crate) fn open_db(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create db dir: {}", parent.display()))?;
    }

    let conn = Connection::open(path)
        .with_context(|| format!("failed to open SQLite DB: {}", path.display()))?;
    configure_connection(&conn)?;
    Ok(conn)
}

pub(crate) fn configure_connection(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -32000;
        ",
    )?;
    Ok(())
}

fn apply_sql_file(conn: &Connection, sql_path: &Path) -> Result<()> {
    let sql = fs::read_to_string(sql_path)
        .with_context(|| format!("failed to read SQL file: {}", sql_path.display()))?;
    conn.execute_batch(&sql)
        .with_context(|| format!("failed to apply SQL file: {}", sql_path.display()))?;
    Ok(())
}

fn seed_content_db(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;

    for (code, name_ko, name_native, script_type) in [
        ("en", "영어", "English", "latin"),
        ("ja", "일본어", "日本語", "mixed"),
        ("ko", "한국어", "한국어", "hangul"),
    ] {
        tx.execute(
            "
            INSERT INTO languages (code, name_ko, name_native, script_type)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(code) DO UPDATE SET
                name_ko = excluded.name_ko,
                name_native = excluded.name_native,
                script_type = excluded.script_type
            ",
            params![code, name_ko, name_native, script_type],
        )?;
    }

    for (code, display_name) in [
        ("unknown", "Unknown"),
        ("noun", "Noun"),
        ("verb", "Verb"),
        ("verb-ichidan", "Verb Ichidan"),
        ("verb-godan", "Verb Godan"),
        ("verb-suru", "Verb Suru"),
        ("adjective", "Adjective"),
        ("adjective-i", "Adjective I"),
        ("adjective-na", "Adjective Na"),
        ("adjective-no", "Adjective No"),
        ("adjective-prenoun", "Adjective Prenoun"),
        ("adverb", "Adverb"),
        ("expression", "Expression"),
        ("particle", "Particle"),
        ("pronoun", "Pronoun"),
        ("counter", "Counter"),
        ("conjunction", "Conjunction"),
        ("interjection", "Interjection"),
        ("prefix", "Prefix"),
        ("suffix", "Suffix"),
        ("auxiliary", "Auxiliary"),
    ] {
        tx.execute(
            "
            INSERT INTO pos_tags (code, display_name)
            VALUES (?1, ?2)
            ON CONFLICT(code) DO UPDATE SET display_name = excluded.display_name
            ",
            params![code, display_name],
        )?;
    }

    for (code, display_name) in [
        ("general", "General"),
        ("daily", "Daily"),
        ("travel", "Travel"),
        ("business", "Business"),
        ("academic", "Academic"),
        ("tech", "Tech"),
        ("food", "Food"),
        ("sports", "Sports"),
        ("medical", "Medical"),
        ("legal", "Legal"),
    ] {
        tx.execute(
            "
            INSERT INTO domains (code, display_name)
            VALUES (?1, ?2)
            ON CONFLICT(code) DO UPDATE SET display_name = excluded.display_name
            ",
            params![code, display_name],
        )?;
    }

    for (code, display_name) in [
        ("neutral", "Neutral"),
        ("formal", "Formal"),
        ("informal", "Informal"),
        ("colloquial", "Colloquial"),
        ("slang", "Slang"),
        ("honorific", "Honorific"),
        ("humble", "Humble"),
        ("archaic", "Archaic"),
        ("poetic", "Poetic"),
    ] {
        tx.execute(
            "
            INSERT INTO registers (code, display_name)
            VALUES (?1, ?2)
            ON CONFLICT(code) DO UPDATE SET display_name = excluded.display_name
            ",
            params![code, display_name],
        )?;
    }

    tx.commit()?;
    Ok(())
}

fn seed_progress_db(conn: &Connection) -> Result<()> {
    conn.execute(
        "
        INSERT INTO profiles (profile_key, display_name)
        VALUES (?1, ?2)
        ON CONFLICT(profile_key) DO UPDATE SET display_name = excluded.display_name
        ",
        params!["default", "Default"],
    )?;

    Ok(())
}
