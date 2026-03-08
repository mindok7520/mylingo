use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use linguaforge_core::manifest::{FetchManifest, FetchedAsset};
use linguaforge_core::sources::{
    Compression, ExtractMode, SourceDefinition, SourceFile, SourceRegistry,
};
use linguaforge_core::workspace::WorkspaceLayout;
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tracing::info;
use zip::ZipArchive;

mod courses;
mod migrate;
mod publish;
mod quality;
mod stage;

#[derive(Debug, Parser)]
#[command(name = "linguaforge")]
#[command(about = "LinguaForge data platform CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Sources(SourcesCommand),
    Fetch(FetchCommand),
    Stage(StageCommand),
    Migrate(MigrateCommand),
    Publish(PublishCommand),
    GenerateCourses(GenerateCoursesCommand),
    QualityReport(QualityReportCommand),
}

#[derive(Debug, Args)]
struct SourcesCommand {
    #[command(subcommand)]
    action: SourcesAction,
}

#[derive(Debug, Subcommand)]
enum SourcesAction {
    List,
}

#[derive(Debug, Args)]
struct FetchCommand {
    sources: Vec<String>,
    #[arg(long)]
    all: bool,
    #[arg(long)]
    force: bool,
    #[arg(long = "no-extract")]
    no_extract: bool,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct StageCommand {
    sources: Vec<String>,
    #[arg(long)]
    all: bool,
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct MigrateCommand {
    #[arg(value_enum, default_value_t = DatabaseTarget::All)]
    target: DatabaseTarget,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct PublishCommand {
    sources: Vec<String>,
    #[arg(long)]
    all: bool,
    #[arg(long = "no-search-rebuild")]
    no_search_rebuild: bool,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct GenerateCoursesCommand {
    #[arg(long)]
    replace: bool,
    #[arg(long, default_value_t = 20)]
    unit_size: u32,
}

#[derive(Debug, Args, Clone)]
pub(crate) struct QualityReportCommand {
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum DatabaseTarget {
    All,
    Content,
    Progress,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    let workspace = WorkspaceLayout::new(std::env::current_dir()?);
    workspace.ensure_runtime_dirs()?;

    match cli.command {
        Command::Sources(command) => run_sources(&workspace, command).await,
        Command::Fetch(command) => run_fetch(&workspace, command).await,
        Command::Stage(command) => stage::run_stage(&workspace, command).await,
        Command::Migrate(command) => migrate::run_migrate(&workspace, command).await,
        Command::Publish(command) => publish::run_publish(&workspace, command).await,
        Command::GenerateCourses(command) => {
            courses::run_generate_courses(&workspace, command).await
        }
        Command::QualityReport(command) => quality::run_quality_report(&workspace, command).await,
    }
}

async fn run_sources(workspace: &WorkspaceLayout, command: SourcesCommand) -> Result<()> {
    let registry = SourceRegistry::load_from_path(&workspace.registry_path())?;

    match command.action {
        SourcesAction::List => {
            for source in registry.enabled_sources() {
                println!(
                    "{id:20} | {language:5} | {category:14} | v={version:8} | {name}",
                    id = source.id,
                    language = source.language,
                    category = source.category,
                    version = source.version,
                    name = source.name,
                );
            }
        }
    }

    Ok(())
}

async fn run_fetch(workspace: &WorkspaceLayout, command: FetchCommand) -> Result<()> {
    let registry = SourceRegistry::load_from_path(&workspace.registry_path())?;
    let targets = if command.all || command.sources.is_empty() {
        registry.enabled_sources().collect::<Vec<_>>()
    } else {
        registry.select(&command.sources)?
    };

    if targets.is_empty() {
        bail!("no sources selected");
    }

    let client = reqwest::Client::builder()
        .user_agent("LinguaForge/0.1")
        .build()
        .context("failed to build HTTP client")?;

    for source in targets {
        fetch_source(
            &client,
            workspace,
            source,
            command.force,
            !command.no_extract,
        )
        .await?;
    }

    Ok(())
}

async fn fetch_source(
    client: &reqwest::Client,
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    force: bool,
    extract_enabled: bool,
) -> Result<()> {
    info!(source = %source.id, "fetching source");

    let target_dir = workspace.raw_source_dir(&source.id, &source.version);
    fs::create_dir_all(&target_dir)
        .with_context(|| format!("failed to create target dir: {}", target_dir.display()))?;

    let mut assets = Vec::new();
    for file in &source.files {
        file.validate()?;
        let asset = fetch_asset(client, &target_dir, file, force, extract_enabled).await?;
        assets.push(asset);
    }

    let manifest = FetchManifest {
        source_id: source.id.clone(),
        source_version: source.version.clone(),
        generated_at: chrono::Utc::now(),
        assets,
    };

    let manifest_path = target_dir.join("manifest.json");
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    tokio::fs::write(&manifest_path, manifest_bytes)
        .await
        .with_context(|| format!("failed to write manifest: {}", manifest_path.display()))?;

    println!("fetched {} -> {}", source.id, target_dir.display());
    Ok(())
}

async fn fetch_asset(
    client: &reqwest::Client,
    target_dir: &Path,
    file: &SourceFile,
    force: bool,
    extract_enabled: bool,
) -> Result<FetchedAsset> {
    let download_name = file.download_name()?;
    let download_path = target_dir.join(download_name);

    if force || !download_path.exists() {
        download_file(client, &file.url, &download_path).await?;
    }

    let metadata = fs::metadata(&download_path)
        .with_context(|| format!("failed to stat file: {}", download_path.display()))?;
    let sha256 = sha256_file(&download_path)?;

    if let Some(expected) = &file.checksum_sha256 {
        let actual = sha256.to_ascii_lowercase();
        let expected = expected.to_ascii_lowercase();
        if actual != expected {
            bail!(
                "checksum mismatch for {}: expected {}, got {}",
                download_path.display(),
                expected,
                actual
            );
        }
    }

    let extracted_paths = if extract_enabled {
        extract_asset(file, &download_path, target_dir)?
    } else {
        Vec::new()
    };

    Ok(FetchedAsset {
        asset_id: file.asset_id.clone(),
        url: file.url.clone(),
        download_path: download_path.display().to_string(),
        extracted_paths: extracted_paths
            .into_iter()
            .map(|path| path.display().to_string())
            .collect(),
        sha256,
        bytes: metadata.len(),
    })
}

async fn download_file(client: &reqwest::Client, url: &str, destination: &Path) -> Result<()> {
    info!(url = %url, path = %destination.display(), "downloading asset");

    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("request failed: {url}"))?
        .error_for_status()
        .with_context(|| format!("request returned error status: {url}"))?;

    let temp_path = temporary_download_path(destination)?;
    if let Some(parent) = temp_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut file = tokio::fs::File::create(&temp_path)
        .await
        .with_context(|| format!("failed to create temp file: {}", temp_path.display()))?;
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("failed to read download stream: {url}"))?;
        file.write_all(&chunk)
            .await
            .with_context(|| format!("failed to write temp file: {}", temp_path.display()))?;
    }

    file.flush().await?;
    tokio::fs::rename(&temp_path, destination)
        .await
        .with_context(|| format!("failed to finalize download: {}", destination.display()))?;
    Ok(())
}

fn extract_asset(
    file: &SourceFile,
    download_path: &Path,
    target_dir: &Path,
) -> Result<Vec<PathBuf>> {
    match (file.compression, file.extract) {
        (_, ExtractMode::None) | (Compression::None, _) => Ok(Vec::new()),
        (Compression::Gzip, ExtractMode::SingleFile) => {
            let output_path = target_dir.join(file.extracted_label()?);
            extract_gzip(download_path, &output_path)?;
            Ok(vec![output_path])
        }
        (Compression::Zip, ExtractMode::Archive) => {
            let output_dir = target_dir.join(file.extracted_label()?);
            let paths = extract_zip(download_path, &output_dir)?;
            Ok(paths)
        }
        _ => bail!(
            "unsupported extract combination for asset '{}': {:?} + {:?}",
            file.asset_id,
            file.compression,
            file.extract
        ),
    }
}

fn extract_gzip(source: &Path, destination: &Path) -> Result<()> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create extract dir: {}", parent.display()))?;
    }

    let input = File::open(source)
        .with_context(|| format!("failed to open gzip source: {}", source.display()))?;
    let mut decoder = GzDecoder::new(BufReader::new(input));
    let output = File::create(destination)
        .with_context(|| format!("failed to create extracted file: {}", destination.display()))?;
    let mut writer = BufWriter::new(output);
    std::io::copy(&mut decoder, &mut writer)
        .with_context(|| format!("failed to extract gzip: {}", source.display()))?;
    writer.flush()?;
    Ok(())
}

fn extract_zip(source: &Path, destination_dir: &Path) -> Result<Vec<PathBuf>> {
    fs::create_dir_all(destination_dir).with_context(|| {
        format!(
            "failed to create extract dir: {}",
            destination_dir.display()
        )
    })?;

    let file = File::open(source)
        .with_context(|| format!("failed to open zip source: {}", source.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("failed to read zip archive: {}", source.display()))?;

    let mut extracted = Vec::new();
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let enclosed = entry
            .enclosed_name()
            .ok_or_else(|| anyhow!("zip entry has invalid path"))?
            .to_path_buf();
        let out_path = destination_dir.join(enclosed);

        if entry.name().ends_with('/') {
            fs::create_dir_all(&out_path)?;
            continue;
        }

        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut out_file = File::create(&out_path)
            .with_context(|| format!("failed to create extracted file: {}", out_path.display()))?;
        std::io::copy(&mut entry, &mut out_file)
            .with_context(|| format!("failed to extract zip entry to: {}", out_path.display()))?;
        extracted.push(out_path);
    }

    Ok(extracted)
}

fn sha256_file(path: &Path) -> Result<String> {
    let file =
        File::open(path).with_context(|| format!("failed to open file: {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

fn temporary_download_path(destination: &Path) -> Result<PathBuf> {
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("invalid destination path: {}", destination.display()))?;
    Ok(destination.with_file_name(format!("{file_name}.part")))
}
