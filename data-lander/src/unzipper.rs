use std::fs::File;
use std::path::Path;
use tempfile::NamedTempFile;
use zip::ZipArchive;
use std::path::PathBuf;
use std::io::{copy};

pub fn streamed_unzip(
    zip_file: &NamedTempFile,
    extract_to: &Path,
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let file = File::open(zip_file.path())?;
    let mut archive = ZipArchive::new(file)?;
    let mut extracted_files = Vec::new();

    for i in 0..archive.len() {
        let mut zip_file_entry = archive.by_index(i)?;
        let entry_name = zip_file_entry.name().to_owned(); // Store for later use
        let outpath = extract_to.join(&entry_name);

        if entry_name.ends_with('/') {
            // It's a directory
            std::fs::create_dir_all(&outpath)?;
            continue;
        }

        if let Some(parent) = outpath.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut outfile = File::create(&outpath)?;
        copy(&mut zip_file_entry, &mut outfile)?;

        println!("  Extracted: {}", entry_name);
        extracted_files.push(outpath.clone()); // Clone for pushing to extracted_files

        // *** New logic for handling nested zips ***
        if entry_name.to_lowercase().ends_with(".zip") {
            println!("Found nested zip: {}, attempting to extract...", entry_name);
            // We need to create a NamedTempFile from the extracted inner zip
            // so that we can pass it to the recursive call.
            let temp_inner_zip = NamedTempFile::new_in(extract_to)?;
            std::fs::copy(&outpath, temp_inner_zip.path())?; // Copy content to temp file

            // Recursively call streamed_unzip for the inner zip
            let inner_extracted =
                streamed_unzip(&temp_inner_zip, &outpath.with_extension(""))?; // Extract to a new directory named after the zip
            extracted_files.extend(inner_extracted); // Add extracted files from inner zip
        }
    }

    Ok(extracted_files)
}