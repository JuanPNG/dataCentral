import json
import math
from pathlib import Path
from pygbif import occurrences as gbif_occ
from pygbif import species as gbif_spp
from shapely.geometry import Point


def get_occurrences_gbif(path, limit=150):
    """
    Get occurrences and selected fields for species listed on the taxonomy_ena_gbif.jsonl file.
    Using pygbif occurrence search. This function is meant for prototyping.
    :param path: Path to the directory with the jsonl file containing the gbif usageKey and the
    annotations accession number.
    :param limit: maximum number of occurrences to retrieve in the search. Default is 150.
    GBIF defaults to 300.
    :return: No return object. File occ_sp_name.jsonl saved to path.
    """

    Path(f'{path}/occurrences/raw').mkdir(parents=True, exist_ok=True)

    with open(f'{path}/annotations/annotation_taxonomy_upload.jsonl', 'r') as tax:

        for i, line in enumerate(tax):
            data = json.loads(line)
            species_name = data['species']
            gbif_usage_key = data['gbif_usageKey']
            accession = data['accession']
            tax_id = data['tax_id']

            print(f'Working on {i}. {species_name}')

            results = gbif_occ.search(
                # scientificName=species_name,
                taxonKey=gbif_usage_key,
                basisOfRecord=[
                    'PRESERVED_SPECIMEN',
                    'MATERIAL_SAMPLE',
                    # 'HUMAN_OBSERVATION',
                    # 'MACHINE_OBSERVATION',
                    # 'LIVING_SPECIMEN',
                    # 'FOSSIL_SPECIMEN',
                    # 'MATERIAL_CITATION',
                    # 'OCCURRENCE'
                ],
                occurrenceStatus='PRESENT',
                hasCoordinate=True,
                hasGeospatialIssue=False,
                limit=limit
            )

            occurrence_records = results.get('results')

            sp_name = species_name.replace(' ', '_')

            with open(f'{path}/occurrences/raw/occ_{sp_name}.jsonl', 'w') as sp_file:

                for record in occurrence_records:
                    record_to_return = {
                        'accession': accession,
                        'tax_id': tax_id,
                        'gbif_usageKey': record.get('taxonKey', None),
                        'species': record.get('species', None),
                        'decimalLatitude': record.get('decimalLatitude',  None),
                        'decimalLongitude': record.get('decimalLongitude', None),
                        'geodeticDatum': record.get('geodeticDatum', None),
                        'coordinateUncertaintyInMeters': record.get('coordinateUncertaintyInMeters', None),
                        'eventDate': record.get('eventDate', None),
                        'continent': record.get('continent', None),
                        'gadm': record.get('gadm', None),
                        'countryCode': record.get('countryCode', None),
                        'basisOfRecord': record.get('basisOfRecord', None),
                        'occurrenceStatus': record.get('occurrenceStatus'),
                        'occurrenceID': record.get('occurrenceID', None),
                        'gbifID': record.get('gbifID', None),
                        'issues': record.get('issues', 'NO_ISSUES_RETRIEVED'),
                        'isSequenced': record.get('isSequenced', None),
                        'iucnRedListCategory': record.get('iucnRedListCategory', None)
                    }

                    sp_file.write(f'{json.dumps(record_to_return)}\n')


def get_occurrence_count(occ_count_file, taxonomy_file, basis_of_record):
    """
    Get the number of occurrences for the species listed on taxonomy_file.
    :param occ_count_file: File path to save the occurrence counts.
    :param taxonomy_file: File path with the taxonomy containing accession, species, and gbif_usageKey.
    :param basis_of_record: The basis of record for the species occurrence.
    :return: No return object.
    """
    with open(occ_count_file, 'w') as meta:
        with open(taxonomy_file, 'r') as tax:
            for i, line in enumerate(tax):
                data = json.loads(line)
                print(f'Working on {i}. {data['species']}')
                count = gbif_occ.count(
                    taxonKey=data['gbif_usageKey'],
                    isGeoreferenced=True,
                    basisOfRecord=basis_of_record
                )
                print(f'{count} {basis_of_record} occurrences found.')
                to_return = dict(
                    accession=data['accession'],
                    species=data['species'],
                    gbif_usageKey=data['gbif_usageKey'],
                    occ_count=count
                )
                meta.write(f'{json.dumps(to_return)}\n')


def request_download_gbif(species_name: str, gbif_usage_key: str) -> dict:

    request_response = gbif_occ.download([
        f'taxonKey = {gbif_usage_key}',
        'hasCoordinate = TRUE',
        'basisOfRecord = OCCURRENCE',
        'occurrenceStatus = PRESENT',
        'hasGeospatialIssue = False'
    ])

    metadata_to_return = dict(
        species=species_name,
        gbif_usage_key=gbif_usage_key,
        download_key=request_response[0],
        request_metadata=gbif_occ.download_meta(key=request_response[0])
    )

    # with open('./data/metadata/meta_down_species_name.jsonl', 'w') as outfile:
    #     outfile.write(f'{json.dumps(metadata_to_return)}\n')

    return metadata_to_return


# with open('./data/metadata/meta_downloads.jsonl', 'w') as outfile:
#     with open('./out/taxonomy_ena_gbif.jsonl', 'r') as tax:
#
#         dict_to_return = dict()
#
#         for line in tax:
#             data = json.loads(line)
#
#             request_response = gbif_occ.download([
#                 f'taxonKey = {data['gbif_usage_key']}',
#                 'hasCoordinate = TRUE',
#                 'basisOfRecord = OCCURRENCE',
#                 'occurrenceStatus = PRESENT',
#                 'hasGeospatialIssue = False'
#             ])
#
#             dict_to_return[data['species']] = gbif_occ.download_meta(key=request_response[0])
#
#         outfile.write(f'{json.dumps(dict_to_return)}\n')
# meta_test = request_download_gbif(species_name='Bignonia aequinoctialis', gbif_usage_key='3172559')
# test = {'Bignonia aequinoctialis': gbif_occ.download_meta(key='0002056-241024112534372')}
# print(json.dumps(test, indent=4))
# print()
# print(test['Bignonia aequinoctialis']['downloadLink'])


def validate_names_gbif(ena_taxonomy_file, validated_file):
    """
    Validates species names from the ena taxonomy with the GBIF taxonomic backbone.
    usageKey, scientificName, status, confidence and match type are provided for each species name.
    Names with matchType different from 'EXACT' are saved in './annotation_taxonomy_gbif_check.jsonl'
    :param validated_file: File path to the validated file.
    :param ena_taxonomy_file: File path to the ena taxonomy file.
    :return: The same ena_taxonomy_file file but complemented with GBIF usageKey, scientificName, status, confidence and
    match type.
    """

    to_check = []

    with open(validated_file, 'w') as val:

        with open(ena_taxonomy_file, 'r') as tax:

            for i, line in enumerate(tax):
                data = json.loads(line)
                print(f'Working on {i}. {data["species"]}')

                gbif_record = gbif_spp.name_backbone(
                    name=data['species'],
                    rank='species',
                    strict=False,
                    verbose=True
                )

                if gbif_record['matchType'] == 'NONE':

                    data['matchType'] = gbif_record['matchType']
                    data['gbif_confidence'] = gbif_record['confidence']

                    to_check.append(data)

                else:

                    data['gbif_matchType'] = gbif_record['matchType']
                    data['gbif_confidence'] = gbif_record['confidence']
                    data['gbif_usageKey'] = gbif_record['usageKey']
                    data['gbif_scientificName'] = gbif_record['scientificName']
                    data['gbif_canonicalName'] = gbif_record['canonicalName']
                    data['gbif_rank'] = gbif_record['rank']
                    data['gbif_status'] = gbif_record['status']

                    if gbif_record['status'] == 'SYNONYM' or gbif_record['matchType'] != 'EXACT':

                        if gbif_record.get('acceptedUsageKey') is not None:
                            data['gbif_acceptedUsageKey'] = gbif_record['acceptedUsageKey']
                        if gbif_record.get('species') is not None:
                            data['gbif_accepted_species'] = gbif_record['species']

                        if gbif_record.get('alternatives') is not None:
                            data['alternatives'] = gbif_record['alternatives']

                        to_check.append(data)

                    else:

                        val.write(f"{json.dumps(data)}\n")

    with open('./out/annotations/annotation_taxonomy_gbif_check.jsonl', 'w') as nm:

        for record in to_check:
            nm.write(f'{json.dumps(record)}\n')

        print(f'Not matching taxonomies found for {len(to_check)} species.\n'
              f'Records saved to annotation_taxonomy_gbif_check.jsonl\n'
              f'Check for Synonyms, Fuzzy matches, and Higher rank matches.\n')


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