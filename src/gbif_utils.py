import json
from pathlib import Path
from pygbif import occurrences as gbif_occ


def get_occurrences_gbif(path, limit=150):
    """
    Get occurrences and selected fields for species listed on the taxonomy_ena_gbif.jsonl file.
    Using pygbif occurrence search. This function is meant for prototyping.
    :param path: Path to the jsonl file containing the gbif usageKey and the
    annotations accession number.
    :param limit: maximum number of occurrences to retrieve in the search. Default is 150.
    GBIF defaults to 300.
    :return: No return object. File occ_sp_name.jsonl saved to path.
    """

    Path(f'{path}/occurrences/raw').mkdir(parents=True, exist_ok=True)

    with open(f'{path}/taxonomy_ena_gbif.jsonl', 'r') as tax:

        for i, line in enumerate(tax):
            data = json.loads(line)
            species_name = data['species']
            gbif_usage_key = data['gbif_usageKey']
            accession = data['accession']

            print(f'Working on {i}. {species_name}')

            results = gbif_occ.search(
                # scientificName=species_name,
                taxonKey=gbif_usage_key,
                basisOfRecord='OCCURRENCE',
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
                        'gbif_usageKey': record.get('taxonKey', 'UNAVAILABLE'),
                        'species': record.get('species', 'UNAVAILABLE'),
                        'decimalLatitude': record.get('decimalLatitude', 'UNAVAILABLE'),
                        'decimalLongitude': record.get('decimalLongitude', 'UNAVAILABLE'),
                        'geodeticDatum': record.get('geodeticDatum', 'UNAVAILABLE'),
                        'coordinateUncertaintyInMeters': record.get('coordinateUncertaintyInMeters', 'UNAVAILABLE'),
                        'eventDate': record.get('eventDate', 'UNAVAILABLE'),
                        'continent': record.get('continent', 'UNAVAILABLE'),
                        'gadm': record.get('gadm', 'UNAVAILABLE'),
                        'countryCode': record.get('countryCode', 'UNAVAILABLE'),
                        # 'country': record.get('gadm').get('level0').get('name', 'UNAVAILABLE'),
                        # 'province': record.get('gadm').get('level1').get('name', 'UNAVAILABLE'),
                        # 'county': record.get('gadm').get('level2').get('name', 'UNAVAILABLE'),
                        # 'municipality': record.get('gadm').get('level3').get('name', 'UNAVAILABLE'),
                        'basisOfRecord': record.get('basisOfRecord', 'UNAVAILABLE'),
                        'occurrenceStatus': record.get('occurrenceStatus'),
                        'occurrenceID': record.get('occurrenceID', 'UNAVAILABLE'),
                        'gbifID': record.get('gbifID', 'UNAVAILABLE'),
                        'issues': record.get('issues', 'NO_ISSUES_RETRIEVED'),
                        'kingdom': record.get('kingdom', 'UNAVAILABLE'),
                        'phylum': record.get('phylum', 'UNAVAILABLE'),
                        'order': record.get('order', 'UNAVAILABLE'),
                        'family': record.get('family', 'UNAVAILABLE'),
                        'genus': record.get('genus', 'UNAVAILABLE'),
                        'scientificName': record.get('scientificName', 'UNAVAILABLE'),
                        'acceptedScientificName': record.get('acceptedScientificName', 'UNAVAILABLE'),
                        'taxonomicStatus': record.get('taxonomicStatus', 'UNAVAILABLE'),
                        # 'identifiedByIDs': record.get('identifiedByIDs', 'UNAVAILABLE'),
                        'isSequenced': record.get('isSequenced', 'UNAVAILABLE'),
                        'iucnRedListCategory': record.get('iucnRedListCategory', 'UNAVAILABLE')
                    }

                    sp_file.write(f'{json.dumps(record_to_return)}\n')


def get_sp_occurrences_count(path):
    """
    Get the number of occurrences for the species listed on taxonomy_ena_gbif.jsonl file
    :return: No return object. File meta_sp_occs_count.jsonl saved to path.
    """
    with open(f'{path}/meta_sp_occs_count.jsonl', 'w') as meta:
        with open(f'.{path}/taxonomy_ena_gbif.jsonl', 'r') as tax:
            for line in tax:
                data = json.loads(line)
                count = gbif_occ.count(taxonKey=data['gbif_usageKey'], isGeoreferenced=True, basisOfRecord='OCCURRENCE')
                to_return = dict(
                    accession=data['accession'],
                    species=data['species'],
                    gbif_usageKey=data['gbif_usageKey'],
                    occ_count=count
                )
                meta.write(f'{json.dumps(to_return)}\n')