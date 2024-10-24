from src.annotations_utils import *
from src.gbif_utils import *
from src.names_utils import complement_taxonomy_gbif_id


def extract_annotations(url, path):

    parse_annotations(url=url, path=path)

    get_annotation_taxonomy_ena(path=path)

    complement_taxonomy_gbif_id(path=path)

    get_occurrences_gbif(path=path, limit=100)




