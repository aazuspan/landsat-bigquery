"""Run BigQuery queries and export result figures."""

import numpy as np
import pandas as pd
from google.cloud import bigquery
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import plotly.express as px

from src import bq_utils, config


def count_total_scenes(client: bigquery.Client) -> None:
    """Count the total number of scenes in the table."""
    print("\nQuerying total scenes...")
    QUERY = f"""
    SELECT
        COUNT(*) AS scenes
    FROM
        `{config.FULL_TABLE_ID}`
    """

    total_scenes = (
        bq_utils.run_query(client=client, query=QUERY)
        .result()
    )

    print(f"Total scenes: {next(total_scenes)[0]:,}")


def plot_cumulative_scenes(client: bigquery.Client) -> None:
    """Plot the cumulative number of scenes acquired by each spacecraft."""
    print("\nQuerying cumulative scenes by spacecraft...")
    QUERY = f"""
    -- Count the number of scenes acquired by each spacecraft in each year
    WITH year_count AS (
    SELECT
        SPACECRAFT_ID,
        EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', DATE_ACQUIRED)) AS year_acquired,
        COUNT(*) AS scenes
    FROM
        `{config.FULL_TABLE_ID}`
    GROUP BY
        SPACECRAFT_ID, year_acquired
    )
    SELECT
    SPACECRAFT_ID,
    year_acquired,
    SUM(scenes) OVER (
        PARTITION BY SPACECRAFT_ID
        ORDER BY year_acquired
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_count
    FROM
    year_count
    ORDER BY
    SPACECRAFT_ID, year_acquired;
    """

    cumulative_scenes = (
        bq_utils.run_query(client=client, query=QUERY)
        .result()
        .to_dataframe()
    )

    # Make spacecraft categorical and re-group to fill missing years with zeros
    cumulative_scenes["SPACECRAFT_ID"] = pd.Categorical(cumulative_scenes["SPACECRAFT_ID"])
    cumulative_scenes = (
        cumulative_scenes
        .groupby(["SPACECRAFT_ID", "year_acquired"], observed=False)
        .sum()
        .reset_index()
    )

    # Set zeros to NaN to prepare for filling
    cumulative_scenes["cumulative_count"] = cumulative_scenes["cumulative_count"].replace(0, np.nan)

    # Fill NaNs with the last non-NaN value, by spacecraft. This means that cumulative
    # counts persist after a spacecraft stops acquiring new scenes.
    cumulative_scenes["cumulative_count"] = (
        cumulative_scenes
        .groupby("SPACECRAFT_ID", observed=True)["cumulative_count"]
        .ffill()
        .fillna(0)
    )

    fig = px.bar(
        cumulative_scenes, 
        x="year_acquired", y="cumulative_count", 
        color="SPACECRAFT_ID", 
        barmode="stack",
        template="plotly_white",
        labels={"cumulative_count": "Cumulative Scenes"},
        color_discrete_sequence=px.colors.qualitative.Prism_r,
        width=900,
    )

    fig.update_xaxes(dtick=5, title=None, tickangle=25, ticks="outside")
    fig.update_layout(
        font=dict(size=24),
        legend=dict(traceorder="reversed", title="", x=0.02, y=0.94),
        margin=dict(l=24, r=24, t=18, b=18),
    )

    dst = "./output/cumulative_scenes.png"
    fig.write_image(dst, scale=2)
    print(f"Wrote figure to {dst}.")



def visualize_clear_scenes_by_location(client: bigquery.Client) -> None:
    """Animate number of clear scenes by path and row by year."""
    print("\nQuerying clear scenes by path and row...")
    QUERY = f"""
    SELECT
        EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', DATE_ACQUIRED)) AS year,
        COUNT(*) AS num_scenes,
        WRS_PATH, WRS_ROW,
        -- Get the centroid of all scenes in the path/row
        ST_CENTROID(ST_UNION_AGG(geo)) AS geo

    FROM `{config.FULL_TABLE_ID}`
    WHERE 
        -- Exclude ocean scenes
        CLOUD_COVER_LAND <> -1 
        -- Exclude night scenes
        AND SUN_ELEVATION > 0 
        -- Exclude cloudy scenes
        AND CLOUD_COVER < 20
    GROUP BY year, WRS_PATH, WRS_ROW
    """

    num_scenes = (
        bq_utils.run_query(client=client, query=QUERY)
        .result()
        .to_geodataframe()
    )

    fig, ax = plt.subplots()
    cmap = mpl.cm.Reds

    def update(frame):
        year = 1972 + frame
        # Sorting path/rows by number of scenes prevents overlapping low counts from 
        # incorrectly appearing as data gaps.
        year_scenes = (
            num_scenes
            .query(f"year == {year}")
            .sort_values(by=["num_scenes"], ascending=True)
        )

        ax.clear()

        year_scenes.plot(
            ax=ax, 
            column="num_scenes", 
            cmap=cmap,
            vmin=1,
            vmax=50, 
            markersize=2,
            marker="H",
        )

        ax.set_aspect("equal")
        ax.set(xlim=[-180, 180], ylim=[-60, 86])
        ax.set_title(year, fontdict={"fontsize": 14}, pad=10)
        ax.set_axis_off()

    fig.set_size_inches(8, 4.5, forward=True)
    fig.subplots_adjust(left=0.05, right=0.95, bottom=0, top=1.0)
    norm = mpl.colors.Normalize(vmin=1, vmax=50)
    cb = fig.colorbar(
        mpl.cm.ScalarMappable(norm=norm, cmap=cmap),
        ax=ax, orientation='horizontal', 
        label='Clear Landsat Scenes Per Year', 
        shrink=0.6, pad=0.1, 
        aspect=90,
    )

    cb.outline.set_visible(False)

    # update(51)
    ani = FuncAnimation(fig, update, frames=52, interval=30)
    dst = "./output/clear_scenes_1972-2023.mp4"
    ani.save(dst, fps=2.5, dpi=150)
    print(f"Wrote video to {dst}.")


if __name__ == "__main__":
    client = bigquery.Client(project=config.CLOUD_PROJECT)

    count_total_scenes(client=client)
    plot_cumulative_scenes(client=client)
    visualize_clear_scenes_by_location(client=client)