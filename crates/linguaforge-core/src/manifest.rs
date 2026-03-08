use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchManifest {
    pub source_id: String,
    pub source_version: String,
    pub generated_at: DateTime<Utc>,
    pub assets: Vec<FetchedAsset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchedAsset {
    pub asset_id: String,
    pub url: String,
    pub download_path: String,
    pub extracted_paths: Vec<String>,
    pub sha256: String,
    pub bytes: u64,
}
