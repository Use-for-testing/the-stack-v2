# The Stack v2 - Language Filtered Pipeline

This README explains how to use the modified scripts to generate a filtered version of The Stack v2 dataset that only includes the following languages:

- Swift
- Python
- Lua
- C
- C++
- Objective-C
- C#
- Ruby
- JavaScript
- TypeScript

## Files Overview

1. `target_languages.csv` - A filtered version of the language labels file, containing only the languages we want to include
2. `stack_pipeline.py` - A unified Python script that runs the entire pipeline for the selected languages
3. `run_stack_pipeline.sh` - A shell script wrapper to easily run the pipeline with common settings

## Prerequisites

- Python 3.6+
- Spark (for distributed processing)
- Go (for enry language detection)
- Sufficient disk space for the dataset

## Quick Start

The easiest way to run the pipeline is using the shell wrapper:

```bash
./run_stack_pipeline.sh --output-dir ./output
```

This will:
1. Set up the necessary environment
2. Download and process repository data
3. Traverse directories to find files
4. Get unique files
5. Download file contents
6. Filter files based on our target languages
7. Process licenses
8. Create the final dataset

## Configuration Options

The shell wrapper provides several options to customize the pipeline:

```
Usage: run_stack_pipeline.sh [options]

Options:
  --output-dir DIR      Set output directory (default: ./output)
  --local               Use local paths instead of S3 paths
  --skip-setup          Skip setup steps (enry setup and requirements installation)
  --resume-from STEP    Resume pipeline from a specific step
                        (repo_data, traverse, unique_files, download, licenses)
  --help                Show this help message
```

## Advanced Usage

If you need more control, you can run the Python script directly with additional options:

```bash
python stack_pipeline.py --output_dir ./output [options]
```

Run `python stack_pipeline.py --help` for a full list of options.

## Processing Pipeline Steps

1. **Setup Environment**: Installs requirements and sets up the enry language detection tool
2. **Download Repo Data**: Downloads GitHub Archive data and merges it with Software Heritage data
3. **Traverse Directories**: Finds file paths in repositories
4. **Get Unique Files**: Identifies unique files to download
5. **Download & Filter**: Downloads file contents and filters by language
6. **Process Licenses**: Processes license information for the files
7. **Create Final Dataset**: Creates the final filtered dataset

## Language Filtering

The language filtering happens in two ways:

1. Files are detected with their language using the `enry` library
2. The dataset is filtered to only include files with languages in our target list

This ensures that the final dataset only contains the languages we're interested in.

## Memory and Disk Requirements

This pipeline processes a large amount of data and may require significant resources:

- Disk space: 100GB+ for a complete run
- Memory: 16GB+ recommended
- CPU: Multi-core system recommended for faster processing

If you're running locally with limited resources, consider using the `--local` option and processing a subset of the data.

## Troubleshooting

- **Memory Issues**: Try reducing batch sizes or processing fewer blob prefixes at a time
- **Missing Files**: If resuming from a previous step, ensure all required files exist
