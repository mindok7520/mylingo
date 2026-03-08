use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct WorkspaceLayout {
    root: PathBuf,
}

impl WorkspaceLayout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn registry_path(&self) -> PathBuf {
        self.root.join("config").join("sources.toml")
    }

    pub fn raw_source_dir(&self, source_id: &str, version: &str) -> PathBuf {
        self.root
            .join("data")
            .join("raw")
            .join(source_id)
            .join(version)
    }

    pub fn ensure_runtime_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(self.root.join("data").join("raw"))?;
        std::fs::create_dir_all(self.root.join("data").join("processed"))?;
        Ok(())
    }
}
