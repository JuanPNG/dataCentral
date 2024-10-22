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