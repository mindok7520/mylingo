use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRegistry {
    #[serde(rename = "source")]
    pub sources: Vec<SourceDefinition>,
}

impl SourceRegistry {
    pub fn load_from_path(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read source registry: {}", path.display()))?;
        let mut registry: Self = toml::from_str(&raw)
            .with_context(|| format!("failed to parse source registry: {}", path.display()))?;
        registry.sources.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(registry)
    }

    pub fn enabled_sources(&self) -> impl Iterator<Item = &SourceDefinition> {
        self.sources.iter().filter(|source| source.enabled)
    }

    pub fn get(&self, id: &str) -> Option<&SourceDefinition> {
        self.sources.iter().find(|source| source.id == id)
    }

    pub fn select<'a>(&'a self, ids: &[String]) -> Result<Vec<&'a SourceDefinition>> {
        if ids.is_empty() {
            return Ok(self.enabled_sources().collect());
        }

        ids.iter()
            .map(|id| {
                self.get(id)
                    .ok_or_else(|| anyhow!("unknown source id: {id}"))
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceDefinition {
    pub id: String,
    pub name: String,
    pub language: String,
    pub category: String,
    pub license: String,
    pub homepage: String,
    pub version: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub files: Vec<SourceFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFile {
    pub asset_id: String,
    pub url: String,
    #[serde(default)]
    pub compression: Compression,
    #[serde(default)]
    pub extract: ExtractMode,
    pub output_name: Option<String>,
    pub extract_name: Option<String>,
    pub extract_dir: Option<String>,
    pub checksum_sha256: Option<String>,
}

impl SourceFile {
    pub fn download_name(&self) -> Result<String> {
        if let Some(name) = &self.output_name {
            return Ok(name.clone());
        }

        let url = Url::parse(&self.url).with_context(|| format!("invalid url: {}", self.url))?;
        let name = url
            .path_segments()
            .and_then(|segments| segments.filter(|segment| !segment.is_empty()).next_back())
            .ok_or_else(|| anyhow!("could not infer filename from url: {}", self.url))?;

        Ok(name.to_string())
    }

    pub fn extracted_label(&self) -> Result<String> {
        if let Some(name) = &self.extract_name {
            return Ok(name.clone());
        }

        if let Some(dir) = &self.extract_dir {
            return Ok(dir.clone());
        }

        let name = self.download_name()?;
        match self.compression {
            Compression::None => Ok(name),
            Compression::Gzip => Ok(name.trim_end_matches(".gz").to_string()),
            Compression::Zip => Ok(self.asset_id.clone()),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if matches!(self.extract, ExtractMode::SingleFile)
            && !matches!(self.compression, Compression::Gzip)
        {
            bail!(
                "asset '{}' uses extract=single_file but compression is not gzip",
                self.asset_id
            );
        }

        if matches!(self.extract, ExtractMode::Archive)
            && !matches!(self.compression, Compression::Zip)
        {
            bail!(
                "asset '{}' uses extract=archive but compression is not zip",
                self.asset_id
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Zip,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtractMode {
    #[default]
    None,
    SingleFile,
    Archive,
}

fn default_enabled() -> bool {
    true
}
