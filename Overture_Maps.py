import duckdb
from pathlib import Path

THEME_MAP = {
    "locality": "admins",
    "locality_area": "admins",
    "administrative_boundary": "admins",
    "building": "buildings",
    "building_part": "buildings",
    "place": "places",
    "segment": "transportation",
    "connector": "transportation",
    "infrastructure": "base",
    "land": "base",
    "land_use": "base",
    "water": "base",
}


def overture_buildings(
    bbox: tuple[float, float, float, float],
    overture_type: str,
    dst_parquet: str | Path,
) -> None:
    """Query a subset of Overture's buildings data and save it as a GeoParquet file.

    Parameters
    ----------
    bbox : tuple
        A tuple of floats representing the bounding box of the area of interest
        in the format (xmin, ymin, xmax, ymax) and 4326 coordinate reference system.
    overture_type : str
        The type of Overture data to query. Valid options are:

        - ``locality``
        - ``locality_area``
        - ``administrative_boundary``
        - ``building``
        - ``building_part``
        - ``place``
        - ``segment``
        - ``connector``
        - ``infrastructure``
        - ``land``
        - ``land_use``
        - ``water``

    dst_parquet : str or Path
        The path to the output GeoParquet file.
    """
    s3_region = "us-west-2"
    base_url = f"s3://overturemaps-{s3_region}/release"
    version = "2024-04-16-beta.0"
    if overture_type not in THEME_MAP:
        raise ValueError(f"Valid Overture types are: {list(THEME_MAP)}")
    theme = THEME_MAP[overture_type]
    remote_path = f"{base_url}/{version}/theme={theme}/type={overture_type}/*"

    conn = duckdb.connect()
    conn.execute("INSTALL httpfs;")
    conn.execute("INSTALL spatial;")
    conn.execute("LOAD httpfs;")
    conn.execute("LOAD spatial;")
    conn.execute(f"SET s3_region='{s3_region}';")

    read_parquet = f"read_parquet('{remote_path}', filename=true, hive_partitioning=1);"
    conn.execute(f"CREATE OR REPLACE VIEW data_view AS SELECT * FROM {read_parquet}")

    query = f"""
    SELECT
        data.*,
    FROM data_view AS data
    WHERE data.bbox.xmin <= {bbox[2]} AND data.bbox.xmax >= {bbox[0]}
    AND data.bbox.ymin <= {bbox[3]} AND data.bbox.ymax >= {bbox[1]}
    """

    file = str(Path(dst_parquet).resolve())
    conn.execute(f"COPY ({query}) TO '{file}' WITH (FORMAT PARQUET);")

    conn.close()


# Manhattan bbox
bbox_example = (-74.02169, 40.696423, -73.891338, 40.831263)
overture_buildings(bbox_example, "building", "nyc_buildings_subset.parquet")


import geopandas as gpd
import pandas as pd
import shapely.wkb


manhattan = pd.read_parquet("nyc_buildings_subset.parquet")
manhattan = gpd.GeoDataFrame(
    manhattan.drop(columns="geometry"),
    geometry=shapely.wkb.loads(manhattan["geometry"]),
    crs=4326,
)
