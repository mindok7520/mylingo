use crate::StageCommand;
use anyhow::{Context, Result, bail};
use linguaforge_core::sources::{SourceDefinition, SourceRegistry};
use linguaforge_core::staging::{StageManifest, StageOutput, StagedLexeme};
use linguaforge_core::workspace::WorkspaceLayout;
use serde_json::json;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use tracing::info;

pub async fn run_stage(workspace: &WorkspaceLayout, command: StageCommand) -> Result<()> {
    let registry = SourceRegistry::load_from_path(&workspace.registry_path())?;
    let targets = if command.all || command.sources.is_empty() {
        registry.enabled_sources().collect::<Vec<_>>()
    } else {
        registry.select(&command.sources)?
    };

    if targets.is_empty() {
        bail!("no sources selected");
    }

    for source in targets {
        match stage_source(workspace, source, command.force)? {
            Some(manifest) => println!(
                "staged {} -> {} output(s)",
                manifest.source_id,
                manifest.outputs.len()
            ),
            None => println!("stage not implemented yet for {}", source.id),
        }
    }

    Ok(())
}

fn stage_source(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    force: bool,
) -> Result<Option<StageManifest>> {
    match source.id.as_str() {
        "google-10000" => stage_google_10000(workspace, source, force).map(Some),
        _ => Ok(None),
    }
}

fn stage_google_10000(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    force: bool,
) -> Result<StageManifest> {
    info!(source = %source.id, "staging source");

    let input_path = raw_asset_path(workspace, source, 0)?;
    if !input_path.exists() {
        bail!(
            "raw asset missing for '{}'. run `cargo run -p linguaforge-cli -- fetch {}` first",
            source.id,
            source.id
        );
    }

    let output_dir = workspace
        .root()
        .join("data")
        .join("processed")
        .join("staging")
        .join(&source.id)
        .join(&source.version);
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create staging dir: {}", output_dir.display()))?;

    let output_path = output_dir.join("lexemes.jsonl");
    if output_path.exists() && !force {
        let manifest_path = output_dir.join("manifest.json");
        if manifest_path.exists() {
            let manifest_raw = fs::read_to_string(&manifest_path).with_context(|| {
                format!("failed to read stage manifest: {}", manifest_path.display())
            })?;
            let manifest: StageManifest =
                serde_json::from_str(&manifest_raw).with_context(|| {
                    format!(
                        "failed to parse stage manifest: {}",
                        manifest_path.display()
                    )
                })?;
            return Ok(manifest);
        }
    }

    let input = File::open(&input_path)
        .with_context(|| format!("failed to open raw asset: {}", input_path.display()))?;
    let reader = BufReader::new(input);
    let output = File::create(&output_path)
        .with_context(|| format!("failed to create staging output: {}", output_path.display()))?;
    let mut writer = BufWriter::new(output);

    let mut seen = HashSet::new();
    let mut records = 0_u64;

    for (index, line) in reader.lines().enumerate() {
        let lemma = line?;
        let lemma = lemma.trim();
        if lemma.is_empty() {
            continue;
        }

        let normalized = normalize_english_lemma(lemma);
        if normalized.is_empty() || !seen.insert(normalized.clone()) {
            continue;
        }

        let staged = StagedLexeme {
            source_id: source.id.clone(),
            source_version: source.version.clone(),
            language: source.language.clone(),
            record_kind: "lexeme".to_string(),
            lemma: lemma.to_string(),
            lemma_normalized: normalized,
            display_form: lemma.to_string(),
            reading: None,
            pronunciation: None,
            primary_pos: None,
            gloss_en: None,
            gloss_ko: None,
            frequency_rank: Some((index + 1) as u32),
            tags: vec!["google-10000".to_string(), "frequency".to_string()],
            metadata: json!({
                "list": "20k",
                "line_number": index + 1,
            }),
        };

        serde_json::to_writer(&mut writer, &staged)?;
        writer.write_all(b"\n")?;
        records += 1;
    }

    writer.flush()?;

    let manifest = StageManifest {
        source_id: source.id.clone(),
        source_version: source.version.clone(),
        generated_at: chrono::Utc::now(),
        outputs: vec![StageOutput {
            kind: "lexeme_jsonl".to_string(),
            path: output_path.display().to_string(),
            records,
        }],
    };

    let manifest_path = output_dir.join("manifest.json");
    fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?).with_context(|| {
        format!(
            "failed to write stage manifest: {}",
            manifest_path.display()
        )
    })?;

    Ok(manifest)
}

fn raw_asset_path(
    workspace: &WorkspaceLayout,
    source: &SourceDefinition,
    file_index: usize,
) -> Result<PathBuf> {
    let file = source
        .files
        .get(file_index)
        .context("source does not have expected asset index")?;
    Ok(workspace
        .raw_source_dir(&source.id, &source.version)
        .join(file.download_name()?))
}

fn normalize_english_lemma(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut last_was_separator = false;

    for ch in input.trim().chars() {
        let ch = ch.to_ascii_lowercase();
        let keep = ch.is_ascii_alphanumeric() || matches!(ch, '-' | '\'' | ' ');
        if !keep {
            continue;
        }

        if ch == ' ' {
            if !last_was_separator {
                output.push(ch);
                last_was_separator = true;
            }
        } else {
            output.push(ch);
            last_was_separator = false;
        }
    }

    output.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::normalize_english_lemma;

    #[test]
    fn normalizes_basic_ascii() {
        assert_eq!(normalize_english_lemma("  Running  "), "running");
    }

    #[test]
    fn drops_symbols_but_keeps_inner_dash() {
        assert_eq!(normalize_english_lemma("co-op!"), "co-op");
    }
}
