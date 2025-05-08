import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import geopandas as gpd
from apache_beam.transforms.combiners import Top
from shapely.geometry import Point
import math

# from src.gbif_utils import (
#     filter_zero_coords,
#     filter_invalid_coords,
#     filter_high_uncertainty,
#     filter_sea,
#     filter_centroid,
# )


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


def _load_land_gdf(path):
    gdf = gpd.read_file(path)
    gdf.sindex  # build spatial index
    return gdf


def _load_centroid_list(path):
    gdf = gpd.read_file(path)
    return list(zip(gdf.geometry.y, gdf.geometry.x))


def run():
    parser = argparse.ArgumentParser(description='GBIF Cleaning Pipeline')
    parser.add_argument('--input', required=True, help='Path to input JSONL')
    parser.add_argument('--output', required=True, help='Path prefix for output JSONL')
    parser.add_argument('--land_shapefile', required=True, help='Path to Natural Earth land shapefile')
    parser.add_argument('--centroid_shapefile', required=True, help='Path to admin-0 label points shapefile')
    parser.add_argument('--max_uncertainty', type=float, default=1000, help='Maximum coordinate uncertainty in meters')
    parser.add_argument('--max_centroid_dist', type=float, default=5000, help='Maximum distance to centroid in meters')
    known_args, beam_args = parser.parse_known_args()

    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:
        # Side inputs
        land_si = (
                p
                | 'CreateLandSI' >> beam.Create([known_args.land_shapefile])
                | 'LoadLandSI' >> beam.Map(_load_land_gdf)
        )
        cen_si = (
                p
                | 'CreateCentroidSI' >> beam.Create([known_args.centroid_shapefile])
                | 'LoadCentroidSI' >> beam.Map(_load_centroid_list)
        )

        # Occurrence records
        records = (
                p
                | 'ReadJSONL' >> beam.io.ReadFromText(known_args.input)
                | 'ParseJSON' >> beam.Map(json.loads)
        )

        filtered = (
                records
                | 'FilterZero' >> beam.Filter(lambda r: filter_zero_coords(r) is not None)
                | 'FilterBounds' >> beam.Filter(lambda r: filter_invalid_coords(r) is not None)
                | 'FilterUncert' >> beam.Filter(lambda r: filter_high_uncertainty(r, known_args.max_uncertainty) is not None)
                | 'FilterSea' >> beam.Filter(
                                    lambda r, land: filter_sea(r, land) is not None,
                                    land=beam.pvalue.AsSingleton(land_si)
                                )
                | 'FilterCentroid' >> beam.Filter(
                                    lambda r, cents: filter_centroid(r, cents, known_args.max_centroid_dist) is not None,
                                    cents=beam.pvalue.AsSingleton(cen_si)
                                )
        )

        # Deduplicated records: selecting top per coord by lowest uncertainty
        deduplicated = (
                filtered
                | 'KeyByCoords' >> beam.Map(lambda r: ((r['decimalLatitude'], r['decimalLongitude']), r))
                | 'TopByUncertainty' >> Top.PerKey(
                                    1,
                                    key=lambda rec: float(rec.get('coordinateUncertaintyInMeters', float('inf')))
                                )
                | 'ExtractRecord' >> beam.Map(lambda kv: kv[1][0])
        )

        # Serialize and write output as single shard without suffix
        (
                deduplicated
                | 'ToJSON' >> beam.Map(json.dumps)
                | 'Write' >> beam.io.WriteToText(
                    known_args.output,
                    file_name_suffix='.jsonl',
                    num_shards=1,
                    shard_name_template=''
                )
        )


if __name__ == '__main__':
    run()

# python ./dataCentral/beam/beam_gbif_cleaning.py \
#   --input ./dataCentral/out/occurrences/raw/occ_Abrostola_tripartita.jsonl \
#   --output ./dataCentral/out/mock/cleaned_gbif_beam2 \
#   --land_shapefile ./dataCentral/data/spatial_processing/ne_10m_land.zip \
#   --centroid_shapefile ./dataCentral/data/spatial_processing/ne_10m_admin_0_label_points.zip \
#   --max_uncertainty 1000 \
#   --max_centroid_dist 5000
