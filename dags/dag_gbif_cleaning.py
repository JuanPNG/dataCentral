from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from shapely.geometry import Point
import geopandas as gpd
import json
import math


# from src.gbif_utils import (
#     read_jsonl,
#     deduplicate_coords,
#     filter_zero_coords,
#     filter_invalid_coords,
#     filter_high_uncertainty,
#     filter_sea,
#     filter_centroid,
# )


def deduplicate_coords(record, seen_coords):
    """
        Identify and filter out duplicate occurrence records based on latitude/longitude pairs.

        Args:
            record (dict): A single GBIF occurrence record with at least the keys
                           'decimalLatitude' and 'decimalLongitude'.
            seen_coords (frozenset or set of tuples): The set of (lat, lon) pairs
                           already encountered.

        Returns:
            tuple:
              - the original record (dict) if this record's (lat, lon) pair has not been seen before;
                otherwise None.
              - updated_seen (set of tuples): A new set including this record's coordinates
                if it was not a duplicate, or the original set if it was.

        Usage:
            seen = set()
            cleaned = []
            for rec in records:
                kept, seen = deduplicate_coords(rec, seen)
                if kept is not None:
                    cleaned.append(kept)
        """

    try:
        lat = float(record.get('decimalLatitude', None))
        lon = float(record.get('decimalLongitude', None))
    except (TypeError, ValueError):
        # If lat/lon are missing or invalid, treat as duplicate to drop the record.
        return None, seen_coords

    coords = (lat, lon)

    # If this exact coordinate has been already seen in the dataset, drop it.
    if coords in seen_coords:
        return None, seen_coords

    # Otherwise, keep it and add to the set
    updated_seen = set(seen_coords)
    updated_seen.add(coords)

    return record, updated_seen


def filter_zero_coords(record):
    """
    Filter out records with both latitude and longitude equal to zero (0,0).

    Args:
        record (dict): A single GBIF occurrence record with keys 'decimalLatitude' and 'decimalLongitude'.

    Returns:
        dict or None: The original record if it does not have both coordinates at zero,
                      otherwise None to indicate filtering out.
    """

    if record is None:
        return None

    try:
        lat = float(record.get('decimalLatitude', None))
        lon = float(record.get('decimalLongitude', None))
    except (TypeError, ValueError):
        # If lat/lon are missing or cannot be parsed, drop the record
        return None

    # Filter out exact zero-zero coordinate
    if lat == 0.0 and lon == 0.0:
        return None

    return record


def filter_invalid_coords(record):
    """
    Filter out records with invalid latitude or longitude values.

    Args:
        record (dict): A single GBIF occurrence record with keys 'decimalLatitude' and 'decimalLongitude'.

    Returns:
        dict or None: The original record if latitude is between -90 and 90 and longitude is between -180 and 180;
                      otherwise None to indicate filtering out.
    """

    if record is None:
        return None

    try:
        lat = float(record.get('decimalLatitude', None))
        lon = float(record.get('decimalLongitude', None))
    except (TypeError, ValueError):
        # If lat/lon are missing or invalid, drop the record
        return None

    # Check latitude and longitude bounds
    if not (-90.0 <= lat <= 90.0):
        return None
    if not (-180.0 <= lon <= 180.0):
        return None

    return record


def filter_high_uncertainty(record, max_uncertainty):
    """
    Filter out records with coordinate uncertainty above a specified threshold.

    Args:
        record (dict): A single GBIF occurrence record with key 'coordinateUncertaintyInMeters'.
        max_uncertainty (float or int): Maximum allowed uncertainty in meters.

    Returns:
        dict or None: The original record if its uncertainty is <= max_uncertainty;
                      otherwise None to indicate filtering out.
    """

    if record is None:
        return None

    # Attempt to parse the uncertainty value
    raw_uncert = record.get('coordinateUncertaintyInMeters')
    try:
        # Some records may have empty strings or None
        uncert = float(raw_uncert) if raw_uncert not in (None, "") else None
    except (TypeError, ValueError):
        # If unparsable, drop the record
        return None

    # If no uncertainty reported, drop as well
    if uncert is None:
        return None

    # Filter based on threshold
    if uncert > max_uncertainty:
        return None

    return record


def filter_sea(record, land_gdf):
    """
    Drop records that fall outside all land polygons (i.e., over sea).

    Args:
        record (dict): Occurrence with 'decimalLatitude' and 'decimalLongitude'.
        land_gdf (GeoDataFrame): Pre-loaded Natural Earth land polygons with spatial index.

    Returns:
        dict or None: record if on land, else None.
    """
    if record is None:
        return None

    try:
        lat = float(record['decimalLatitude'])
        lon = float(record['decimalLongitude'])
    except (KeyError, TypeError, ValueError):
        return None

    pt = Point(lon, lat)
    possible = list(land_gdf.sindex.query(pt, predicate="intersects"))

    for idx in possible:
        if pt.within(land_gdf.geometry.iloc[idx]):
            return record

    return None


def haversine_dist(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points on a sphere using the haversine formula.

    Args:
        lat1, lon1: Latitude and longitude of point 1 in decimal degrees.
        lat2, lon2: Latitude and longitude of point 2 in decimal degrees.

    Returns:
        Distance in meters.

    Reference:
        https://community.esri.com/t5/coordinate-reference-systems-blog/distance-on-a-sphere-the-haversine-formula/ba-p/902128#:~:text=For%20example%2C%20haversine(θ),longitude%20of%20the%20two%20points.
    """
    # Earth radius in meters
    R = 6371000

    # Convert degrees to radians
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    # Haversine formula components
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Output distance in meters
    distance = R * c

    return distance


def filter_centroid(record, centroids, max_dist=5000):
    """
    Drop records within max_dist (in meters) of any administrative centroid.

    Args:
        record (dict): Occurrence with 'decimalLatitude', 'decimalLongitude'.
        centroids (list of (lat, lon) tuples): Pre-loaded centroid coordinates.
        max_dist (float): Buffer distance in meters (default 5000m).

    Returns:
        dict or None: The record if no centroid is within max_dist, else None.
    """
    if record is None:
        return None

    try:
        lat = float(record['decimalLatitude'])
        lon = float(record['decimalLongitude'])
    except (KeyError, TypeError, ValueError):
        return None

    for cen_lat, cen_lon in centroids:
        if haversine_dist(lat, lon, cen_lat, cen_lon) <= max_dist:
            return None

    return record


def read_jsonl(file_path):
    with open(file_path, 'r') as fh:
        for line in fh:
            yield json.loads(line)


default_args = {
    'owner': 'jpng',
    'start_date': days_ago(1),
    'retries': 1,
}


@dag(
    dag_id='gbif_cleaning_taskflow',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['gbif', 'cleaning'],
)
def gbif_cleaning():
    @task
    def read_records(input_path: str):
        return list(read_jsonl(input_path))

    @task
    def deduplicate(records):
        seen = set()
        output = []
        for rec in records:
            rec, seen = deduplicate_coords(rec, seen)
            if rec:
                output.append(rec)
        return output

    @task
    def zero_filter(records):
        return [rec for rec in records if filter_zero_coords(rec)]

    @task
    def bounds_filter(records):
        return [rec for rec in records if filter_invalid_coords(rec)]

    @task
    def uncertainty_filter(records, max_uncertainty: float):
        return [rec for rec in records if filter_high_uncertainty(rec, max_uncertainty)]

    @task
    def sea_filter(records, land_shapefile: str):
        land_gdf = gpd.read_file(land_shapefile)
        land_gdf.sindex
        return [rec for rec in records if filter_sea(rec, land_gdf)]

    @task
    def centroid_filter(records, centroid_shapefile: str, max_centroid_dist: float):
        gdf_c = gpd.read_file(centroid_shapefile)
        centroids = list(zip(gdf_c.geometry.y, gdf_c.geometry.x))
        return [rec for rec in records if filter_centroid(rec, centroids, max_centroid_dist)]

    @task
    def write_records(records, output_path: str):
        with open(output_path, 'w') as f:
            for rec in records:
                f.write(json.dumps(rec) + '\n')
        print(f'Wrote {len(records)} cleaned records to {output_path}')

    # TaskFlow dependency graph
    raw = read_records('/Users/juann/PycharmProjects/dataCentral/data/mock/mock_gbif.jsonl')
    deduped = deduplicate(raw)
    zero_filtered = zero_filter(deduped)
    bounds_filtered = bounds_filter(zero_filtered)
    uncert_filtered = uncertainty_filter(bounds_filtered, max_uncertainty=1000)
    sea_filtered = sea_filter(
        uncert_filtered,
        land_shapefile='/Users/juann/PycharmProjects/dataCentral/data/spatial_processing/ne_10m_land.zip')
    centroid_filtered = centroid_filter(
        sea_filtered,
        centroid_shapefile='/Users/juann/PycharmProjects/dataCentral/data/spatial_processing/ne_10m_admin_0_label_points.zip',
        max_centroid_dist=5000
    )
    write_records(centroid_filtered, '/Users/juann/PycharmProjects/dataCentral/out/mock/cleaned_gbif.jsonl')


dag = gbif_cleaning()
