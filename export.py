"""
Export all Landsat image centroids and metadata to a BigQuery table.
"""

from __future__ import annotations

import ee
import src.config as config


def collect_landsat_missions() -> ee.ImageCollection:
    """Merge all Landsat Collection 2 Tier 1 and Tier 2 raw collections."""
    merged_collections = ee.ImageCollection([])

    for platform in ["LM01", "LM02", "LM03", "LM04", "LT04", "LT05", "LE07", "LC08", "LC09"]:
        for tier in ["T1", "T2"]:
            col = ee.ImageCollection(f"LANDSAT/{platform}/C02/{tier}")
            merged_collections = merged_collections.merge(col)

    return merged_collections


def images_to_points(col: ee.ImageCollection) -> ee.FeatureCollection:
    """Convert images to their centroids with relevant properties."""
    keep_cols = [
        "SPACECRAFT_ID", 
        "DATE_ACQUIRED", 
        "WRS_PATH", 
        "WRS_ROW",
        "COLLECTION_CATEGORY",
        "CLOUD_COVER_LAND",
        "CLOUD_COVER",
        "SUN_ELEVATION",
    ]
    def centroid(img: ee.Image) -> ee.Feature:
        return ee.Feature(img.geometry().centroid()).copyProperties(img, keep_cols)
    
    return col.map(centroid)


if __name__ == "__main__":
    """
    Export all Landsat image centroids to a BigQuery table.
    """

    ee.Initialize(project=config.CLOUD_PROJECT)

    all_landsat = collect_landsat_missions()
    landsat_points = images_to_points(all_landsat)

    task = ee.batch.Export.table.toBigQuery(
        collection=landsat_points,
        description="export_scenes_to_bigquery",
        table=config.FULL_TABLE_ID,
    )

    task.start()
    check_url = f"https://console.cloud.google.com/earth-engine/tasks?project={config.CLOUD_PROJECT}"
    print(f"Export task started. Check the status at {check_url}.")
