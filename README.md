# Summarizing Landsat with BigQuery

This is a demo project using the [Earth Engine-BigQuery connector](https://cloud.google.com/blog/products/data-analytics/new-bigquery-connector-to-google-earth-engine) to export every Landsat scene to a BigQuery table where they can be queried, summarized, and visualized.

![Clear Landsat scenes by path, row, and year, 1972 - 2023](/output/clear_scenes_1972-2023.mp4)

## Setup

Install Python dependencies from PyPI.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

You'll need `gcloud` to authenticate BigQuery. If you have `snap`, you can install it with:

```bash
sudo snap install google-cloud-cli --classic
```

Otherwise, follow the instructions [here](https://cloud.google.com/sdk/docs/install).

Then run:

```bash
gcloud init && gcloud auth application-default login
```

Set the following in `src/config.py`:
- `CLOUD_PROJECT`: The name of the Google Cloud Project to store tables under.
- `DATASET_ID`: The name of the BigQuery dataset that you manually created in that cloud project.
- `TABLE_ID`: The name of the table that will be generated on export.

## Usage

1. Run `python export.py` to generate the BigQuery table with all Landsat scenes. Wait for the task to complete.
1. Run `python queries.py` to execute queries and generate outputs.