import os

from elasticsearch import Elasticsearch

from src.annotations_utils import *
from src.data_manipulation_utils import *
from src.es_utils import *
from src.gbif_utils import *
from src.gcp_utils import *
from src.names_utils import *


def extract_annotations(url, path):
    # 1. Get annotation accession numbers
    get_annotation_accessions(
        es=Elasticsearch(
            os.getenv('ES_HOST'),
            basic_auth=(
                os.getenv('ES_USER'),
                os.getenv('ES_PASSWORD')
            )
        ),
        index_name='data_portal',
        size=100,
        pages=11,
        file_path='./out/annotations/annotation_accessions.jsonl'
    )

    # 2. Get taxonomy per species with annotations from ENA
    get_ena_taxonomy(
        accessions_file='./out/annotations/annotation_accessions.jsonl',
        taxonomy_file='./out/annotations/annotation_taxonomy_ena.jsonl'
    )

    # 3. Get GBIF usageKey and other taxon metadata
    validate_names_gbif(
        ena_taxonomy_file='./out/annotations/annotation_taxonomy_ena.jsonl',
        validated_file='./out/annotations/annotation_taxonomy_ena_gbif_validated.jsonl'
    )

    taxonomy_to_load(
        file_validated_taxonomy='./out/annotations/annotation_taxonomy_ena_gbif_validated.jsonl',
        file_taxonomy_upload='./out/annotations/annotation_taxonomy_upload.jsonl'
    )

    load_file_to_table_bq(
        table_id=f'{os.getenv('GCP_PROJECT_PROD')}.{os.getenv('GCP_DATASET')}.taxonomy',
        path_data_file='out/annotations/annotation_taxonomy_upload.jsonl',
        path_schema_json='./schema_taxonomy_upload.jsonl'
    )

    # 4. Get occurrences sample for prototyping

    get_occurrence_count(
        occ_count_file='./out/occurrences/occurrence_count_preserved_specimen.jsonl',
        taxonomy_file='./out/annotations/annotation_taxonomy_ena_gbif_validated.jsonl',
        basis_of_record='PRESERVED_SPECIMEN'
    )

    get_occurrence_count(
        occ_count_file='./out/occurrences/occurrence_count_material_sample.jsonl',
        taxonomy_file='./out/annotations/annotation_taxonomy_ena_gbif_validated.jsonl',
        basis_of_record='MATERIAL_SAMPLE'
    )


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
        path_occ_file='./out/occurrences/raw/all_species_occurrences.jsonl',
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
        path_data_file='out/concat_spatial_annotations_20241122_131003.jsonl',
        path_schema_json='./schema_concat_annotations.jsonl'
    )

