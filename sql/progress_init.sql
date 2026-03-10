PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS profiles (
    id                  INTEGER PRIMARY KEY,
    profile_key         TEXT NOT NULL UNIQUE,
    display_name        TEXT NOT NULL,
    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS study_sessions (
    id                  INTEGER PRIMARY KEY,
    profile_id          INTEGER NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    session_key         TEXT NOT NULL UNIQUE,
    mode                TEXT NOT NULL,
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    device              TEXT,
    metadata_json       TEXT
);

CREATE INDEX IF NOT EXISTS idx_study_sessions_active ON study_sessions(finished_at, started_at);

CREATE TABLE IF NOT EXISTS review_items (
    id                  INTEGER PRIMARY KEY,
    item_type           TEXT NOT NULL,
    item_id             INTEGER NOT NULL,
    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(item_type, item_id)
);

CREATE INDEX IF NOT EXISTS idx_review_items_lookup ON review_items(item_type, item_id);

CREATE TABLE IF NOT EXISTS srs_state (
    review_item_id      INTEGER PRIMARY KEY REFERENCES review_items(id) ON DELETE CASCADE,
    ease_factor         REAL NOT NULL DEFAULT 2.5,
    interval_hours      INTEGER NOT NULL DEFAULT 0,
    repetitions         INTEGER NOT NULL DEFAULT 0,
    lapse_count         INTEGER NOT NULL DEFAULT 0,
    correct_streak      INTEGER NOT NULL DEFAULT 0,
    mastery_level       TEXT NOT NULL DEFAULT 'new',
    scheduled_at        TEXT,
    last_reviewed_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_srs_due ON srs_state(scheduled_at);

CREATE TABLE IF NOT EXISTS review_events (
    id                  INTEGER PRIMARY KEY,
    review_item_id      INTEGER NOT NULL REFERENCES review_items(id) ON DELETE CASCADE,
    session_id          INTEGER REFERENCES study_sessions(id) ON DELETE SET NULL,
    review_type         TEXT NOT NULL,
    grade               TEXT NOT NULL,
    response_time_ms    INTEGER,
    scheduled_before    TEXT,
    scheduled_after     TEXT,
    reviewed_at         TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_review_events_item ON review_events(review_item_id, reviewed_at);

CREATE TABLE IF NOT EXISTS course_progress (
    profile_id          INTEGER NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    course_key          TEXT NOT NULL,
    current_unit_order  INTEGER NOT NULL DEFAULT 1,
    completed_units     INTEGER NOT NULL DEFAULT 0,
    updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, course_key)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS unit_progress (
    profile_id          INTEGER NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    course_key          TEXT NOT NULL,
    unit_order          INTEGER NOT NULL,
    learned_count       INTEGER NOT NULL DEFAULT 0,
    reviewed_count      INTEGER NOT NULL DEFAULT 0,
    is_completed        INTEGER NOT NULL DEFAULT 0,
    updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, course_key, unit_order)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS daily_goals (
    profile_id          INTEGER NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    goal_date           TEXT NOT NULL,
    new_items_goal      INTEGER NOT NULL DEFAULT 20,
    reviews_goal        INTEGER NOT NULL DEFAULT 50,
    PRIMARY KEY (profile_id, goal_date)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS app_settings (
    profile_id          INTEGER NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    setting_key         TEXT NOT NULL,
    setting_value       TEXT NOT NULL,
    PRIMARY KEY (profile_id, setting_key)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS lexeme_ko_cache (
    profile_id          INTEGER NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    lexeme_id           INTEGER NOT NULL,
    meaning_ko          TEXT NOT NULL,
    explanation_ko      TEXT,
    provider_label      TEXT NOT NULL,
    updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, lexeme_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_lexeme_ko_cache_updated ON lexeme_ko_cache(updated_at);

CREATE TABLE IF NOT EXISTS ai_generated_lexeme_feedback (
    profile_id          INTEGER NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
    lexeme_id           INTEGER NOT NULL,
    profile_key         TEXT,
    theme_key           TEXT,
    rating              TEXT NOT NULL,
    note                TEXT,
    updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (profile_id, lexeme_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_ai_generated_feedback_lookup
    ON ai_generated_lexeme_feedback(profile_key, theme_key, rating, updated_at);
CREATE INDEX IF NOT EXISTS idx_review_events_reviewed_at ON review_events(reviewed_at);
CREATE INDEX IF NOT EXISTS idx_course_progress_profile ON course_progress(profile_id, course_key);
CREATE INDEX IF NOT EXISTS idx_unit_progress_profile ON unit_progress(profile_id, course_key);
