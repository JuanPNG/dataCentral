from src.annotations_utils import *
from src.data_manipulation_utils import *
from src.gbif_utils import *
from src.gcp_utils import *
from src.names_utils import *


def extract_annotations(url, path):

    # 1. Get annotation accessions and ftp links
    parse_annotations(url=url, path=path)

    # 2. Get taxonomy per species with annotations from ENA
    get_annotation_taxonomy_ena(path=path)

    # 3. Get GBIF usageKey and other taxon metadata
    complement_taxonomy_gbif_id(path=path)

    # 4. Get occurrences sample for prototyping
    get_occurrences_gbif(path=path, limit=100)

    load_file_to_table_bq(
        table_id=f'{os.getenv('GCP_PROJECT')}.{os.getenv('BQ_DATASET_NAME')}.gbif_occurrences',
        path_data_file='./out/occurrences/raw/all_species_occurrences.jsonl',
        path_schema_json='./gbif_schema.json'
    )

    # 5. Get range size
    # Done in BigQuery

    # 6. Annotate with climate
    xy_climate_extraction_batch(
        path_occ_data_file='./out/occurrences/raw/all_species_occurrences.jsonl',
        path_climate_layer_dir='./data/climate/',
        path_to_save_dir='./out/'
    )

    # 6. Annotate with spatial classification
    xy_vector_annotation(
        path_occ_data_file='./out/occurrences/raw/all_species_occurrences.jsonl',
        path_vector_file='./data/bioregions/Ecoregions2017.zip',
        path_to_save_dir='./out/'
    )

    # 7. Concatenate annotations
    concat_spatial_annotations(path_annotation_dir="./out/spatial_annotation/", save_file=True)

    load_file_to_table_bq(
        table_id=f'{os.getenv('GCP_PROJECT')}.{os.getenv('BQ_DATASET_NAME')}.bd_spatial_annotations',
        path_data_file='./out/spatial_annotation/concat_spatial_annotations_20241122_131003.jsonl',
        path_schema_json='./schema_concat_annotations.jsonl'
    )


