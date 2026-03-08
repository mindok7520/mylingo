use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedLexeme {
    pub source_id: String,
    pub source_version: String,
    pub language: String,
    pub record_kind: String,
    #[serde(default)]
    pub source_ref: Option<String>,
    pub lemma: String,
    pub lemma_normalized: String,
    pub display_form: String,
    #[serde(default)]
    pub reading: Option<String>,
    #[serde(default)]
    pub pronunciation: Option<String>,
    #[serde(default)]
    pub primary_pos: Option<String>,
    #[serde(default)]
    pub gloss_en: Option<String>,
    #[serde(default)]
    pub gloss_ko: Option<String>,
    #[serde(default)]
    pub frequency_rank: Option<u32>,
    #[serde(default)]
    pub cefr_level: Option<String>,
    #[serde(default)]
    pub jlpt_level: Option<u8>,
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub register: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedKanji {
    pub source_id: String,
    pub source_version: String,
    #[serde(default)]
    pub source_ref: Option<String>,
    pub character: String,
    #[serde(default)]
    pub stroke_count: Option<u32>,
    #[serde(default)]
    pub grade: Option<u32>,
    #[serde(default)]
    pub jlpt_level: Option<u8>,
    #[serde(default)]
    pub frequency_rank: Option<u32>,
    #[serde(default)]
    pub radical: Option<String>,
    #[serde(default)]
    pub svg_path: Option<String>,
    #[serde(default)]
    pub readings_on: Vec<String>,
    #[serde(default)]
    pub readings_kun: Vec<String>,
    #[serde(default)]
    pub readings_nanori: Vec<String>,
    #[serde(default)]
    pub meanings_en: Vec<String>,
    #[serde(default)]
    pub meanings_ko: Vec<String>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedExample {
    pub source_id: String,
    pub source_version: String,
    #[serde(default)]
    pub source_ref: Option<String>,
    pub language: String,
    pub sentence: String,
    pub sentence_normalized: String,
    #[serde(default)]
    pub sentence_reading: Option<String>,
    #[serde(default)]
    pub translation_language: Option<String>,
    #[serde(default)]
    pub translation_text: Option<String>,
    #[serde(default)]
    pub difficulty_level: Option<u32>,
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedExampleLink {
    pub source_id: String,
    pub source_version: String,
    #[serde(default)]
    pub source_ref: Option<String>,
    pub example_source_ref: String,
    pub lexeme_language: String,
    pub lemma: String,
    pub lemma_normalized: String,
    #[serde(default)]
    pub surface_form: Option<String>,
    #[serde(default)]
    pub reading: Option<String>,
    #[serde(default)]
    pub highlight_start: Option<u32>,
    #[serde(default)]
    pub highlight_end: Option<u32>,
    #[serde(default)]
    pub match_score: f64,
    #[serde(default)]
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
