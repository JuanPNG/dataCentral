import json
from pathlib import Path
from pygbif import occurrences as gbif_occ
from pygbif import species as gbif_spp


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

    with open(f'{path}/annotations/taxonomy_ena_gbif.jsonl', 'r') as tax:

        for i, line in enumerate(tax):
            data = json.loads(line)
            species_name = data['species']
            gbif_usage_key = data['gbif_usageKey']
            accession = data['accession']

            print(f'Working on {i}. {species_name}')

            results = gbif_occ.search(
                # scientificName=species_name,
                taxonKey=gbif_usage_key,
                basisOfRecord=[
                    'PRESERVED_SPECIMEN',
                    'MATERIAL_SAMPLE',
                    # 'HUMAN_OBSERVATION',
                    # 'MACHINE_OBSERVATION',
                    'LIVING_SPECIMEN',
                    'FOSSIL_SPECIMEN',
                    'MATERIAL_CITATION',
                    'OCCURRENCE'
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
                        # 'country': record.get('gadm').get('level0').get('name', None),
                        # 'province': record.get('gadm').get('level1').get('name', None),
                        # 'county': record.get('gadm').get('level2').get('name', None),
                        # 'municipality': record.get('gadm').get('level3').get('name', None),
                        'basisOfRecord': record.get('basisOfRecord', None),
                        'occurrenceStatus': record.get('occurrenceStatus'),
                        'occurrenceID': record.get('occurrenceID', None),
                        'gbifID': record.get('gbifID', None),
                        'issues': record.get('issues', 'NO_ISSUES_RETRIEVED'),
                        'kingdom': record.get('kingdom', None),
                        'phylum': record.get('phylum', None),
                        'order': record.get('order', None),
                        'family': record.get('family', None),
                        'genus': record.get('genus', None),
                        'scientificName': record.get('scientificName', None),
                        'acceptedScientificName': record.get('acceptedScientificName', None),
                        'taxonomicStatus': record.get('taxonomicStatus', None),
                        # 'identifiedByIDs': record.get('identifiedByIDs', None),
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