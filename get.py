import os
import sys
import subprocess
import argparse
import pandas as pd
import json
import gzip
import time
import zipfile
from pathlib import Path

# Define target languages
TARGET_LANGUAGES = [
    "Swift", "Python", "Lua", "C", "C++", "Objective-C", "C#",
    "Ruby", "JavaScript", "TypeScript"
]

def run_command(cmd, cwd=None, shell=False):
    """Run a command and print output"""
    print(f"Running: {' '.join(cmd) if isinstance(cmd, list) else cmd}")
    if shell:
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=cwd)
    else:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=cwd)
    
    for line in iter(process.stdout.readline, b''):
        print(line.decode('utf-8').rstrip())
    
    process.wait()
    if process.returncode != 0:
        print(f"Command failed with return code {process.returncode}")
        return False
    return True

def setup_environment(args):
    """Set up the necessary environment for the pipeline"""
    print("Setting up environment...")
    
    # Create output directories
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, "repo_data"), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, "file_paths"), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, "files_to_download"), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, "file_contents"), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, "detected_licenses"), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, "final_dataset"), exist_ok=True)
    
    # Set up enry for language detection if needed
    if not args.skip_setup_enry:
        run_command(["bash", "the_stack/2_download_files/setup_enry.sh"])
    
    # Install required packages
    if not args.skip_requirements:
        run_command(["pip", "install", "-r", "the_stack/2_download_files/requirements.txt"])
        run_command(["pip", "install", "-r", "kaggle/requirements.txt"])
    
    return True

def download_repo_data(args):
    """Download GitHub Archive data and merge with SWH"""
    print("Downloading repository data...")
    
    # First download GH Archive data if needed
    if not args.skip_gharchive:
        run_command(["python", "the_stack/0_repo_data/download_gharchive.py"])
    
    # Merge GH Archive with SWH data
    cmd = [
        "python", "the_stack/0_repo_data/merge_gha_swh.py",
        "--gharchive_path", args.gharchive_path,
        "--swh_origin_path", args.swh_origin_path,
        "--swh_snapshot_branch_path", args.swh_snapshot_branch_path,
        "--swh_revision_path", args.swh_revision_path,
        "--output_path", os.path.join(args.output_dir, "repo_data")
    ]
    run_command(cmd)
    
    return True

def traverse_directories(args):
    """Traverse directories to find file paths"""
    print("Traversing directories to find file paths...")
    
    cmd = [
        "python", "the_stack/1_directory_traversal/find_file_paths.py",
        "--repo_data_path", os.path.join(args.output_dir, "repo_data"),
        "--swh_directory_entry_path", args.swh_directory_entry_path,
        "--swh_content_path", args.swh_content_path,
        "--cache_path", os.path.join(args.output_dir, "file_paths_cache"),
        "--output_path", os.path.join(args.output_dir, "file_paths")
    ]
    run_command(cmd)
    
    return True

def get_unique_files(args):
    """Get unique files from the traversed directories"""
    print("Getting unique files...")
    
    cmd = [
        "python", "the_stack/1_directory_traversal/get_unique_files.py",
        "--input_path", os.path.join(args.output_dir, "file_paths"),
        "--output_path", os.path.join(args.output_dir, "files_to_download")
    ]
    run_command(cmd)
    
    return True

def download_files_and_filter_languages(args):
    """Download file contents and filter by language"""
    print("Downloading file contents and filtering by language...")
    
    # Loop through blob prefixes
    for blob_prefix in range(256):
        cmd = [
            "python", "the_stack/2_download_files/get_file_contents.py",
            "--blob_prefix", str(blob_prefix),
            "--input_path", os.path.join(args.output_dir, "files_to_download"),
            "--output_path", os.path.join(args.output_dir, "file_contents_raw")
        ]
        run_command(cmd)
    
    # Filter the downloaded files by language
    print("Filtering files by target languages...")
    language_filter(
        input_path=os.path.join(args.output_dir, "file_contents_raw"),
        output_path=os.path.join(args.output_dir, "file_contents"),
        target_languages=TARGET_LANGUAGES
    )
    
    return True

def language_filter(input_path, output_path, target_languages):
    """Filter files by target languages"""
    os.makedirs(output_path, exist_ok=True)
    
    # Read target_languages.csv
    lang_df = pd.read_csv("target_languages.csv")
    allowed_extensions = {}
    
    # Build a lookup dictionary of allowed extensions by language
    for _, row in lang_df.iterrows():
        lang = row['language']
        ext = row['extension']
        if lang in target_languages:
            if lang not in allowed_extensions:
                allowed_extensions[lang] = []
            allowed_extensions[lang].append(ext)
    
    # Function to check if a file should be included based on language and extension
    def should_include_file(file_data):
        try:
            data = json.loads(file_data)
            lang = data.get('language')
            
            # If language is one of our target languages, include it
            if lang in target_languages:
                return True
                
            # If language is None but extension matches one of our target languages, include it
            if lang is None and 'filenames' in data:
                filenames = data.get('filenames', [])
                for filename in filenames:
                    ext = os.path.splitext(filename)[1].lstrip('.')
                    for target_lang, exts in allowed_extensions.items():
                        if ext in exts or ('' in exts and ext == ''):
                            return True
            
            return False
        except:
            return False
    
    # Process each blob_prefix directory
    if os.path.exists(input_path):
        for blob_dir in os.listdir(input_path):
            if blob_dir.startswith("blob_prefix="):
                os.makedirs(os.path.join(output_path, blob_dir), exist_ok=True)
                
                # Process each file in the blob_prefix directory
                for filename in os.listdir(os.path.join(input_path, blob_dir)):
                    if filename.endswith('.json.gz'):
                        input_file = os.path.join(input_path, blob_dir, filename)
                        output_file = os.path.join(output_path, blob_dir, filename)
                        
                        # Read the gzipped JSON file line by line
                        with gzip.open(input_file, 'rt') as infile, gzip.open(output_file, 'wt') as outfile:
                            for line in infile:
                                if should_include_file(line):
                                    outfile.write(line)

def process_licenses(args):
    """Process licenses for the files"""
    print("Processing licenses...")
    
    cmd = [
        "python", "the_stack/3_file_dataset/get_license_types.py",
        "--repo_data_path", os.path.join(args.output_dir, "repo_data"),
        "--file_paths_path", os.path.join(args.output_dir, "file_paths"),
        "--file_contents_path", os.path.join(args.output_dir, "file_contents"),
        "--output_path", os.path.join(args.output_dir, "detected_licenses")
    ]
    run_command(cmd)
    
    return True

def create_final_dataset(args):
    """Create the final dataset with filtered languages and zip it"""
    print("Creating final dataset...")
    
    # Create a README file with information about the filtered dataset
    final_dataset_path = os.path.join(args.output_dir, "final_dataset")
    with open(os.path.join(final_dataset_path, "README.md"), "w") as f:
        f.write("# The Stack v2 - Filtered Dataset\n\n")
        f.write("This dataset contains files from the following languages only:\n")
        for lang in TARGET_LANGUAGES:
            f.write(f"- {lang}\n")
        f.write("\nCreated with get.py on " + time.strftime("%Y-%m-%d %H:%M:%S"))
    
    # Zip the final dataset
    zip_path = os.path.join(args.output_dir, "dataset.zip")
    print(f"Zipping final dataset to {zip_path}...")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(final_dataset_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, final_dataset_path)
                zipf.write(file_path, os.path.join("final_dataset", arcname))
    
    print(f"Final dataset zipped at: {zip_path}")
    return True

def main():
    parser = argparse.ArgumentParser(description="Run The Stack v2 pipeline for specific languages")
    
    # Basic configuration
    parser.add_argument("--output_dir", default="./output", help="Directory to store output data")
    parser.add_argument("--skip_setup_enry", action="store_true", help="Skip setting up enry")
    parser.add_argument("--skip_requirements", action="store_true", help="Skip installing requirements")
    parser.add_argument("--skip_gharchive", action="store_true", help="Skip downloading GH Archive data")
    
    # Pipeline stages to run
    parser.add_argument("--skip_repo_data", action="store_true", help="Skip downloading repo data")
    parser.add_argument("--skip_traverse", action="store_true", help="Skip directory traversal")
    parser.add_argument("--skip_unique_files", action="store_true", help="Skip getting unique files")
    parser.add_argument("--skip_download", action="store_true", help="Skip downloading files")
    parser.add_argument("--skip_licenses", action="store_true", help="Skip license processing")
    parser.add_argument("--skip_final_dataset", action="store_true", help="Skip creating final dataset")
    
    # Path configuration (now local paths)
    parser.add_argument("--gharchive_path", default="./data/gharchive/",
                        help="Path to GH Archive data")
    parser.add_argument("--swh_origin_path", default="./data/swh/origin_visit_status/",
                        help="Path to SWH origin visit status")
    parser.add_argument("--swh_snapshot_branch_path", default="./data/swh/snapshot_branch/",
                        help="Path to SWH snapshot branch")
    parser.add_argument("--swh_revision_path", default="./data/swh/revision/",
                        help="Path to SWH revision")
    parser.add_argument("--swh_directory_entry_path", default="./data/swh/directory_entry/",
                        help="Path to SWH directory entry")
    parser.add_argument("--swh_content_path", default="./data/swh/content/",
                        help="Path to SWH content")
    
    args = parser.parse_args()
    
    # Run the pipeline stages
    if not setup_environment(args):
        return 1
        
    if not args.skip_repo_data:
        if not download_repo_data(args):
            return 1
    
    if not args.skip_traverse:
        if not traverse_directories(args):
            return 1
    
    if not args.skip_unique_files:
        if not get_unique_files(args):
            return 1
    
    if not args.skip_download:
        if not download_files_and_filter_languages(args):
            return 1
    
    if not args.skip_licenses:
        if not process_licenses(args):
            return 1
    
    if not args.skip_final_dataset:
        if not create_final_dataset(args):
            return 1
    
    print("Pipeline completed successfully!")
    return 0

if __name__ == "__main__":
    sys.exit(main())