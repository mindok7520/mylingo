use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedLexeme {
    pub source_id: String,
    pub source_version: String,
    pub language: String,
    pub record_kind: String,
    pub lemma: String,
    pub lemma_normalized: String,
    pub display_form: String,
    pub reading: Option<String>,
    pub pronunciation: Option<String>,
    pub primary_pos: Option<String>,
    pub gloss_en: Option<String>,
    pub gloss_ko: Option<String>,
    pub frequency_rank: Option<u32>,
    pub tags: Vec<String>,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageManifest {
    pub source_id: String,
    pub source_version: String,
    pub generated_at: DateTime<Utc>,
    pub outputs: Vec<StageOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageOutput {
    pub kind: String,
    pub path: String,
    pub records: u64,
}
