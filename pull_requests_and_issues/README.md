This folder contains steps necessary to reproduce dataset of Issues and Pull Requests.

## Ray cluster

Most of the steps are designed to be executed on a Ray cluster. If the code is not run on the AI Toolkit, one must implement its own cluster provisioning and management. Specifically, the scaling up and down of the Ray cluster should be implemented in `ray_server.py`, or the cluster needs to be scaled up elsewhere, and the `scale_cluster` function may not have any effect. Additionally, all paths are intended to be accessible from all cluster nodes.

Most of the processing was done on 60 nodes cluster with 4 cores and 128Gb of RAM each.

## Configuration

`secrets.yaml` file needs to be created and put in this folder with the following format for HuggingFace Hub credentials:
```
hf_api_key: 
```

All configuration is in the `cfg.py`. Configs needed to change would be:
- `root_path` - a path for all processing steps and results to be written to
- `local_data_path` - path to the local data directory where dataset files are stored
- `repo_licenses_local` - local location of license per repository information
- `commit_paris_files_local` - local location of commit pairs files for PRs data
- `opt_outs_dataset_name` - location of opt out data

NOTE: Dataset files should be downloaded separately and placed in your local data directory. These data sets are compiled in other parts of the Stack V2 data pipeline. `opt_outs_dataset_name` will not be released as it is confidential data, so it is needed to compile such data for your project. Please ask on BigCode community general forums on Slack for more details.


## 0_get_gharchive_events.py
Downloads evnets from the GHArchive. Done on one thread and with a delay in order to not overvelm the server.

## 1_parse_issue_and_pr_events.ipynb
Extracts Issues and PRs information from the events, groups events by Issue or PR id, combines them into Issues or PR and splits to Issue dataset and PRs data for further processing.
- `issues` dataset is stored by default in `root_path/issues_prs_grouped`
- `pull requests` are stored by default in `root_path/pr_grouped` for further processing

## 2_process_commit_pairs.ipynb
Downloads repo licenses and pull request commit pair files. Filters non permissive licenses opt outs non changed files and reformats data.

## 3_filter_and_render_pr.ipynb
Filters opt outs and non permissive licenses on PR data also adds various stats per pull request, computes number of pull requests per repo and renders pull requests to the final format. Rendered pull reaquest are in `root_path/prs_renders` by default.

