import json
import warnings


def get_species_data_es(index_name, es_conn):
    """
    Get data for all the species from the Elasticsearch Biodiversity Portal database
    :param index_name: Index in Elasticsearch database.
    :param es: Connection to the Elasticsearch
    :return: Dictionary with species names as keys.
    """
    portal_data = es_conn.search(index=index_name, size=100000)
    data_to_return = dict()
    for hit in portal_data['hits']['hits']:
        data_to_return[hit['_id']] = hit['_source']
    return data_to_return


def get_genome_note_title(spp_name, species_portal_data):
    """
    Get genome note from local copy of the data portal with species information only.
    :param spp_name: String with the species binomial name
    :param species_portal_data: A dictionary with the species data from the ElasticSearch database
    :return: String with the genome note title.
    """

    for species, record in species_portal_data.items():
        if spp_name not in species_portal_data.keys():
            warnings.warn(f'Species name {spp_name} is not in Data Portal.')
            title = 'CHECK_SPECIES_RECORD_DATA_PORTAL'
            return title
        else:
            if spp_name == species:
                if 'genome_notes' in record and len(record['genome_notes']) != 0:
                    title = record['genome_notes'][0]['title']
                else:
                    title = 'NOT_AVAILABLE'

                return title


def get_annotation_accessions(index_name, es, size, pages, file_path=None):
    """
    Get accession numbers, species names, and taxon id for genomes with annotations
    from the Biodiversity Data Portal ElasticSearch database. If a file path is given,
    it will save the results to a file. Otherwise, it will return a list of dictionaries
    with the result.
    :param index_name: index in Elasticsearch database
    :param es: Elasticsearch client
    :param size: Maximum size of the response
    :param pages: Maximum pages for pagination of the response
    :param file_path: full path to JSONL file to save the results.
    :return: List of dictionaries with accession numbers, species, and taxon id.
    """
    accessions = []
    after = None

    for page in range(pages):
        body = {
            'size': size,
            'sort': {'tax_id': 'asc'},
            'query': {
                'match': {
                    'annotation_complete': 'Done'
                }
            }
        }

        if after:
            body['search_after'] = after

        data = es.search(
            index=index_name,
            body=body
        )

        hits = data['hits']['hits']

        for record in hits:
            accession = {
                'accession': record['_source']['annotation'][0]['accession'],
                'tax_id': record['_source']['tax_id'],
                'species': record['_source']['annotation'][0]['species']
            }
            accessions.append(accession)

        if hits:
            after = hits[-1]['sort']

    if file_path is not None:
        with open(file_path, 'w') as f:
            for accession in accessions:
                f.write(f'{json.dumps(accession)}\n')

        return print(f'Saved {len(accessions)} accessions to {file_path}')

    else:

        return accessions
