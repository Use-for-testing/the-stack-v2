from datetime import date
from pathlib import Path

root_path = Path('./output')  # Changed to match get.py output path
# Path to local data that was previously stored in S3
local_data_path = Path('./data')

gharchives_path = root_path / "gharchives"
sdate = date(2015, 1, 1)  # start date
edate = date(2023, 7, 26)  # end date

parsed_issues_prs_path = root_path / 'issues_prs'
issues_prs_grouped_path  = root_path /  'issues_prs_grouped'
prs_grouped_path  = root_path /  'prs_grouped'
prs_grouped_filtered_path  = root_path /  'prs_grouped_filtered'
prs_renders_path  = root_path /  'prs_renders'


# Replaced S3 config with local data paths
repo_licenses_local = {
    'data_dir': local_data_path,
    'path': 'swh_2023_09_06/stats/repo_licenses/part-00000-474605ad-e5ce-4d86-bf45-acaac7241ba1-c000.snappy.parquet',
}

opt_outs_dataset_name = 'bigcode-data/opt-out'

repo_licenses_path = root_path / 'repo_licenses'
repo_licenses_sqlite_file = root_path / 'repo_licenses' / 'repo_licenses.db'

# Replaced S3 config with local data paths
commit_paris_files_local = {
    'data_dir': local_data_path,
    'path': 'swh_2023_09_06_prs/file_dataset',
    'ext': 'parquet'
}
pr_commit_pairs_files_path = root_path / 'pr_commit_pairs_files'
pr_commid_pairs_files_filtered_path = root_path / 'pr_commit_pairs_files_filtered'
pr_commid_pairs_files_filtered_cleaned_path = root_path / 'pr_commit_pairs_files_filtered_cleaned'
pr_commid_pairs_files_filtered_cleaned_grouped_path = root_path / 'pr_commit_pairs_files_filtered_cleaned_grouped'
