//! A zip is a container: a drop of several files, each read in the source's
//! format. Opening one is synchronous; the pipeline runs it off the async
//! runtime and keeps the bookkeeping of the loads it becomes.

use anyhow::Result;

use crate::config::Format;

/// Whether an object is a zip to unpack rather than a file to read.
pub fn is_container(name: &str) -> bool {
    name.to_ascii_lowercase().ends_with(".zip")
}

/// Every file in the zip with its bytes, in archive order; directories, hidden
/// files and `__MACOSX` entries are left out. Every entry is read, whatever its
/// extension, before any is taken: a broken entry fails the whole zip.
pub fn entries(bytes: Vec<u8>) -> Result<Vec<(String, Vec<u8>)>> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes))?;
    let mut out = Vec::new();
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        if entry.is_dir() {
            continue;
        }
        let path = entry.name().to_string();
        let base = path.rsplit('/').next().unwrap_or(&path).to_string();
        if base.starts_with('.') || path.starts_with("__MACOSX") {
            continue;
        }
        let mut data = Vec::with_capacity(entry.size() as usize);
        std::io::Read::read_to_end(&mut entry, &mut data)?;
        out.push((path, data));
    }
    Ok(out)
}

/// Whether a source of this format takes an entry, by its extension.
pub fn takes(format: Format, path: &str) -> bool {
    let wanted = match format {
        Format::Csv => &["csv"][..],
        Format::Ndjson => &["ndjson", "jsonl"][..],
        Format::FhirBundle => &["json"][..],
        Format::Hl7v2 => &["hl7", "txt"][..],
    };
    let extension = path.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    wanted.contains(&extension.as_str())
}
