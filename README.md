# MTA Ridership Pipeline
This repository contains the data pipeline behind OSDC's NYC Subway Recovery Tracker.
It pulls monthly subway ridership from NY Open Data, calculates historical baseline values, and publishes cleaned outputs for station, PUMA, and citywide analysis.

## What This Repo Is For
Use this repository to:
- refresh monthly ridership data
- rebuild baseline comparison files when needed
- generate the production CSVs used by downstream analysis and reporting

## Data Sources
The current pipeline is built primarily from MTA subway ridership datasets published through NY Open Data (Socrata), including:
- `wujg-7c2s` - MTA Subway Hourly Ridership 2020-2024
- `5wq4-mkjj` - MTA Subway Hourly Ridership Beginning 2025
- `t69i-h2reme` - MTA Subway Hourly Ridership 2017-2019, used for baseline calculations

## Maintained By
This project was made for and is maintained by OSDC for the NYC Subway Recovery Tracker workflow.

## How To Use It

### Running in GitHub Codespaces (recommended)
No local setup required. Everything is pre-configured.

1. Go to: https://codespaces.new/Daniel322111/mta_ridership
2. Click **Create codespace** and wait for setup to complete
3. In the terminal, run: `python run_pipeline.py`

The environment, dependencies, and API credentials are all handled automatically.

### Running locally
For setup steps, common run options, and troubleshooting, see:
- [How to run the pipeline](docs/run-pipeline.md)

## Repository Notes
- Final published outputs are written to `data/production/`.
- The active production workflow is API-first.
- Older local raw-file and turnstile scripts are still in the repo for reference, but they are not the main production path.

## Project Structure
For a simple guide to the repository layout, see:
- [Project structure](docs/project-structure.md)
