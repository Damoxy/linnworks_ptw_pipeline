# Linnworks PTW Pipeline

## Overview
This project implements an ETL pipeline that extracts data from the Linnworks API, transforms it via SQL scripts, and loads it into a target data warehouse.

## Structure
- **docs/**: Architecture diagrams and documentation.
- **schema/**: Base DDL SQL scripts.
- **migrations/**: Incremental database changes (versioned).
- **seed/**: Initial or sample data scripts.
- **sql/**: ETL SQL scripts grouped by extraction, transformation, and loading stages.
- **notebooks/**: Jupyter notebooks for data exploration and validation.
- **src/**: Python ETL source code (Extract, Transform, Load).
- **config/**: Configuration files and environment templates.
- **scripts/**: Utility scripts (backup, restore, pipeline run, etc.).
- **tests/**: Unit tests and SQL tests.
- **ci/**: CI/CD configurations.
- **logs/**: Execution logs.

## Usage
1. Clone this repository.
2. Configure your environment in `config/config.yaml` and `credentials_template.env`.
3. Run the pipeline using:
   ```bash
   bash scripts/run_pipeline.sh
   ```
4. Check logs under `logs/pipeline_logs.txt`.

## Requirements
Install dependencies using:
```bash
pip install -r requirements.txt
```

## Author
`ptw_etl <liinworks2ptw@pipeline.com>`
