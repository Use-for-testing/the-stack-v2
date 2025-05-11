#!/bin/bash
# Simple wrapper script to run The Stack v2 pipeline for specific languages

# Default output directory
OUTPUT_DIR="./output"

# Parse arguments
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --output-dir)
      OUTPUT_DIR="$2"
      shift
      shift
      ;;
    --local)
      # Use local paths instead of S3
      LOCAL_PATHS="--gharchive_path ./data/gharchive/ \
                  --swh_origin_path ./data/swh/origin_visit_status/ \
                  --swh_snapshot_branch_path ./data/swh/snapshot_branch/ \
                  --swh_revision_path ./data/swh/revision/ \
                  --swh_directory_entry_path ./data/swh/directory_entry/ \
                  --swh_content_path ./data/swh/content/"
      shift
      ;;
    --skip-setup)
      SKIP_SETUP="--skip_setup_enry --skip_requirements"
      shift
      ;;
    --resume-from)
      case "$2" in
        repo_data)
          SKIP_FLAGS="--skip_repo_data"
          ;;
        traverse)
          SKIP_FLAGS="--skip_repo_data --skip_traverse"
          ;;
        unique_files)
          SKIP_FLAGS="--skip_repo_data --skip_traverse --skip_unique_files"
          ;;
        download)
          SKIP_FLAGS="--skip_repo_data --skip_traverse --skip_unique_files --skip_download"
          ;;
        licenses)
          SKIP_FLAGS="--skip_repo_data --skip_traverse --skip_unique_files --skip_download --skip_licenses"
          ;;
        *)
          echo "Invalid resume point: $2"
          echo "Valid options: repo_data, traverse, unique_files, download, licenses"
          exit 1
          ;;
      esac
      shift
      shift
      ;;
    --help)
      echo "Usage: run_stack_pipeline.sh [options]"
      echo ""
      echo "Options:"
      echo "  --output-dir DIR      Set output directory (default: ./output)"
      echo "  --local               Use local paths instead of S3 paths"
      echo "  --skip-setup          Skip setup steps (enry setup and requirements installation)"
      echo "  --resume-from STEP    Resume pipeline from a specific step"
      echo "                        (repo_data, traverse, unique_files, download, licenses)"
      echo "  --help                Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Run the pipeline
echo "Starting The Stack v2 pipeline for selected languages..."
echo "Output directory: $OUTPUT_DIR"

python stack_pipeline.py --output_dir "$OUTPUT_DIR" $SKIP_SETUP $SKIP_FLAGS $LOCAL_PATHS

# Check if pipeline completed successfully
if [ $? -eq 0 ]; then
    echo "Pipeline completed successfully!"
    echo "Final dataset is available in: $OUTPUT_DIR/final_dataset"
else
    echo "Pipeline failed! Check logs for errors."
    exit 1
fi
