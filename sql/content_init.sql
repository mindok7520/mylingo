PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS languages (
    id              INTEGER PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,
    name_ko         TEXT NOT NULL,
    name_native     TEXT NOT NULL,
    script_type     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pos_tags (
    id              INTEGER PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,
    display_name    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS domains (
    id              INTEGER PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,
    display_name    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS registers (
    id              INTEGER PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,
    display_name    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS data_sources (
    id              INTEGER PRIMARY KEY,
    source_key      TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    version         TEXT,
    homepage_url    TEXT,
    license         TEXT,
    imported_at     TEXT
);

CREATE TABLE IF NOT EXISTS source_imports (
    id                  INTEGER PRIMARY KEY,
    source_id           INTEGER NOT NULL REFERENCES data_sources(id),
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    status              TEXT NOT NULL,
    total_records       INTEGER NOT NULL DEFAULT 0,
    inserted_records    INTEGER NOT NULL DEFAULT 0,
    merged_records      INTEGER NOT NULL DEFAULT 0,
    skipped_records     INTEGER NOT NULL DEFAULT 0,
    error_records       INTEGER NOT NULL DEFAULT 0,
    error_log_json      TEXT
);

CREATE TABLE IF NOT EXISTS lexemes (
    id                  INTEGER PRIMARY KEY,
    language_id         INTEGER NOT NULL REFERENCES languages(id),
    lemma               TEXT NOT NULL,
    lemma_normalized    TEXT NOT NULL,
    display_form        TEXT NOT NULL,
    reading             TEXT,
    pronunciation       TEXT,
    primary_pos_id      INTEGER NOT NULL REFERENCES pos_tags(id),
    frequency_rank      INTEGER,
    difficulty_level    INTEGER,
    cefr_level          TEXT,
    jlpt_level          INTEGER,
    register_id         INTEGER REFERENCES registers(id),
    quality_score       REAL NOT NULL DEFAULT 0.0,
    is_ai_enriched      INTEGER NOT NULL DEFAULT 0,
    is_verified         INTEGER NOT NULL DEFAULT 0,
    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(language_id, lemma_normalized, primary_pos_id)
);

CREATE INDEX IF NOT EXISTS idx_lexemes_language ON lexemes(language_id);
CREATE INDEX IF NOT EXISTS idx_lexemes_frequency ON lexemes(frequency_rank);
CREATE INDEX IF NOT EXISTS idx_lexemes_jlpt ON lexemes(jlpt_level) WHERE jlpt_level IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_lexemes_cefr ON lexemes(cefr_level) WHERE cefr_level IS NOT NULL;

CREATE TABLE IF NOT EXISTS lexeme_forms (
    lexeme_id            INTEGER NOT NULL REFERENCES lexemes(id) ON DELETE CASCADE,
    form_type            TEXT NOT NULL,
    form_text            TEXT NOT NULL,
    form_normalized      TEXT NOT NULL,
    reading              TEXT,
    PRIMARY KEY (lexeme_id, form_type, form_text)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS lexeme_senses (
    id                   INTEGER PRIMARY KEY,
    lexeme_id            INTEGER NOT NULL REFERENCES lexemes(id) ON DELETE CASCADE,
    sense_order          INTEGER NOT NULL,
    gloss_ko             TEXT,
    gloss_en             TEXT,
    gloss_detail         TEXT,
    domain_id            INTEGER REFERENCES domains(id),
    register_id          INTEGER REFERENCES registers(id),
    quality_score        REAL NOT NULL DEFAULT 0.0,
    UNIQUE (lexeme_id, sense_order)
);

CREATE TABLE IF NOT EXISTS tags (
    id                   INTEGER PRIMARY KEY,
    code                 TEXT NOT NULL UNIQUE,
    display_name         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lexeme_tag_map (
    lexeme_id            INTEGER NOT NULL REFERENCES lexemes(id) ON DELETE CASCADE,
    tag_id               INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (lexeme_id, tag_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS sense_sources (
    sense_id             INTEGER NOT NULL REFERENCES lexeme_senses(id) ON DELETE CASCADE,
    source_id            INTEGER NOT NULL REFERENCES data_sources(id),
    source_ref           TEXT,
    priority             INTEGER NOT NULL DEFAULT 100,
    PRIMARY KEY (sense_id, source_id, source_ref)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS lexeme_relations (
    lexeme_id_from       INTEGER NOT NULL REFERENCES lexemes(id) ON DELETE CASCADE,
    lexeme_id_to         INTEGER NOT NULL REFERENCES lexemes(id) ON DELETE CASCADE,
    relation_type        TEXT NOT NULL,
    PRIMARY KEY (lexeme_id_from, lexeme_id_to, relation_type)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS examples (
    id                   INTEGER PRIMARY KEY,
    language_id          INTEGER NOT NULL REFERENCES languages(id),
    sentence             TEXT NOT NULL,
    sentence_normalized  TEXT NOT NULL UNIQUE,
    sentence_reading     TEXT,
    difficulty_level     INTEGER,
    domain_id            INTEGER REFERENCES domains(id),
    quality_score        REAL NOT NULL DEFAULT 0.0,
    created_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS example_translations (
    example_id           INTEGER NOT NULL REFERENCES examples(id) ON DELETE CASCADE,
    language_id          INTEGER NOT NULL REFERENCES languages(id),
    translation_text     TEXT NOT NULL,
    is_primary           INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (example_id, language_id, translation_text)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS example_sources (
    example_id           INTEGER NOT NULL REFERENCES examples(id) ON DELETE CASCADE,
    source_id            INTEGER NOT NULL REFERENCES data_sources(id),
    source_ref           TEXT,
    PRIMARY KEY (example_id, source_id, source_ref)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS lexeme_examples (
    lexeme_id            INTEGER NOT NULL REFERENCES lexemes(id) ON DELETE CASCADE,
    example_id           INTEGER NOT NULL REFERENCES examples(id) ON DELETE CASCADE,
    highlight_start      INTEGER,
    highlight_end        INTEGER,
    match_score          REAL NOT NULL DEFAULT 0.0,
    PRIMARY KEY (lexeme_id, example_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS kanji (
    id                   INTEGER PRIMARY KEY,
    character            TEXT NOT NULL UNIQUE,
    stroke_count         INTEGER,
    grade                INTEGER,
    jlpt_level           INTEGER,
    frequency_rank       INTEGER,
    radical              TEXT,
    svg_path             TEXT,
    created_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kanji_readings (
    kanji_id             INTEGER NOT NULL REFERENCES kanji(id) ON DELETE CASCADE,
    reading_type         TEXT NOT NULL,
    reading_text         TEXT NOT NULL,
    PRIMARY KEY (kanji_id, reading_type, reading_text)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS kanji_meanings (
    kanji_id             INTEGER NOT NULL REFERENCES kanji(id) ON DELETE CASCADE,
    language_id          INTEGER NOT NULL REFERENCES languages(id),
    meaning_text         TEXT NOT NULL,
    PRIMARY KEY (kanji_id, language_id, meaning_text)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS kanji_lexemes (
    kanji_id             INTEGER NOT NULL REFERENCES kanji(id) ON DELETE CASCADE,
    lexeme_id            INTEGER NOT NULL REFERENCES lexemes(id) ON DELETE CASCADE,
    position_index       INTEGER NOT NULL,
    PRIMARY KEY (kanji_id, lexeme_id, position_index)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS grammar_points (
    id                   INTEGER PRIMARY KEY,
    language_id          INTEGER NOT NULL REFERENCES languages(id),
    pattern              TEXT NOT NULL,
    pattern_normalized   TEXT NOT NULL,
    meaning_ko           TEXT,
    meaning_en           TEXT,
    structure_text       TEXT,
    jlpt_level           INTEGER,
    difficulty_level     INTEGER,
    notes                TEXT,
    source_id            INTEGER REFERENCES data_sources(id),
    UNIQUE(language_id, pattern_normalized)
);

CREATE TABLE IF NOT EXISTS grammar_examples (
    grammar_id           INTEGER NOT NULL REFERENCES grammar_points(id) ON DELETE CASCADE,
    example_id           INTEGER NOT NULL REFERENCES examples(id) ON DELETE CASCADE,
    PRIMARY KEY (grammar_id, example_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS course_templates (
    id                   INTEGER PRIMARY KEY,
    language_id          INTEGER NOT NULL REFERENCES languages(id),
    course_key           TEXT NOT NULL UNIQUE,
    name                 TEXT NOT NULL,
    description          TEXT,
    category             TEXT NOT NULL,
    target_exam          TEXT,
    target_domain        TEXT,
    difficulty_start     INTEGER,
    difficulty_end       INTEGER,
    auto_generated       INTEGER NOT NULL DEFAULT 1,
    created_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS course_units (
    id                   INTEGER PRIMARY KEY,
    course_id            INTEGER NOT NULL REFERENCES course_templates(id) ON DELETE CASCADE,
    unit_order           INTEGER NOT NULL,
    title                TEXT NOT NULL,
    description          TEXT,
    UNIQUE(course_id, unit_order)
);

CREATE TABLE IF NOT EXISTS unit_items (
    unit_id              INTEGER NOT NULL REFERENCES course_units(id) ON DELETE CASCADE,
    item_type            TEXT NOT NULL,
    item_id              INTEGER NOT NULL,
    item_order           INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (unit_id, item_type, item_id)
) WITHOUT ROWID;

CREATE VIRTUAL TABLE IF NOT EXISTS lexeme_search USING fts5(
    lexeme_id UNINDEXED,
    surface,
    reading,
    gloss_ko,
    gloss_en,
    tokenize = 'unicode61'
);

CREATE VIRTUAL TABLE IF NOT EXISTS example_search USING fts5(
    example_id UNINDEXED,
    sentence,
    translations,
    tokenize = 'unicode61'
);
