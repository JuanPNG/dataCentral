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