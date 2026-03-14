# databricks-project-2

A personal Databricks data engineering project that implements a **medallion-architecture sales data pipeline** using PySpark and Delta Lake.

## Project Overview

The pipeline ingests raw sales CSV data, applies data quality checks and enrichment transformations, and produces aggregated datasets for downstream analytics.

```
raw CSV  →  [Ingestion]  →  Raw Delta  →  [Transformation]  →  Processed Delta  →  [Analysis]
```

## Repository Structure

```
databricks-project-2/
├── config/
│   └── settings.yaml          # Pipeline configuration
├── data/
│   └── sample/
│       └── sales_sample.csv   # Sample input data
├── notebooks/
│   ├── 01_data_ingestion.py   # Notebook: ingest CSV → raw Delta
│   ├── 02_data_transformation.py  # Notebook: clean, enrich, aggregate
│   └── 03_data_analysis.py    # Notebook: business-level insights
├── src/
│   ├── config.py              # Configuration loader
│   ├── ingestion.py           # Read/write helpers (CSV, JSON, Delta)
│   ├── transformation.py      # PySpark transformation functions
│   └── utils.py               # Shared utilities
├── tests/
│   ├── test_config.py         # Unit tests for config module
│   └── test_pipeline.py       # PySpark tests for transformation & utils
└── requirements.txt
```

## Getting Started

### Prerequisites

- Python 3.9+
- Java 11+ (required by PySpark)

### Installation

```bash
pip install -r requirements.txt
```

### Running Tests

```bash
pytest tests/ -v
```

### Notebooks

Import the notebooks from the `notebooks/` directory into your Databricks workspace and run them in order:

1. **01_data_ingestion.py** – Reads raw CSV files and writes a Delta table to the raw layer.
2. **02_data_transformation.py** – Cleans and enriches the raw data, writes to the processed layer.
3. **03_data_analysis.py** – Produces monthly revenue trends, top products, and customer summaries.

### Configuration

Edit `config/settings.yaml` to customise storage paths and Spark settings before running the notebooks.

## Pipeline Details

### Ingestion (`src/ingestion.py`)
- Reads CSV files with an enforced schema
- Supports JSON and Delta Lake sources
- Writes data to Delta tables (with optional partitioning)

### Transformation (`src/transformation.py`)
- `cast_and_clean` – type casting, null filling, invalid-row removal
- `add_revenue_columns` – derives `gross_revenue` and `net_revenue`
- `add_date_parts` – extracts `year`, `month`, `day` from `order_date`
- `aggregate_sales_by_category` – monthly category-level aggregation
- `aggregate_sales_by_region` – region-level aggregation
- `top_products_by_revenue` – ranked product list
- `customer_summary` – per-customer purchase statistics

### Utilities (`src/utils.py`)
- `validate_not_null` – drops rows with nulls in specified columns
- `deduplicate` – removes duplicate rows
- `rename_columns` – batch column renaming
- `add_ingestion_timestamp` – appends a current-timestamp column
- `get_row_count` – returns the row count of a DataFrame
- `log_dataframe_info` – prints schema and row count for debugging