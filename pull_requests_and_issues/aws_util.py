import ray
from pathlib import Path
import glob
import shutil

def list_parquet_files_local(data_dir, path_prefix):
    """
    Lists parquet files in a local directory with a given prefix.
    Replacement for the S3 version without AWS dependencies.
    
    Args:
        data_dir: Local directory to search in (replaces bucket_name)
        path_prefix: Path prefix to filter by
        
    Returns:
        List of file paths that match the criteria
    """
    base_path = Path(data_dir) / path_prefix
    pattern = f"{base_path}/**/*.parquet"
    res = []
    for file_path in glob.glob(pattern, recursive=True):
        res.append(str(Path(file_path).relative_to(data_dir)))
    return res


@ray.remote
def copy_files_local(data_dir, keys, dst):
    """
    Copies files from a local source directory to a destination.
    Replacement for the S3 download function without AWS dependencies.
    
    Args:
        data_dir: Source directory (replaces bucket_name)
        keys: List of file paths to copy
        dst: Destination directory to copy files to
    """
    dst = Path(dst)
    dst.mkdir(parents=True, exist_ok=True)
    
    for key in keys:
        key_path = Path(key)
        src_path = Path(data_dir) / key_path
        dst_path = dst / key_path.name
        
        # Ensure source exists
        if src_path.exists():
            shutil.copy2(src_path, dst_path)