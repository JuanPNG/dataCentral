import json
import os

import pandas as pd
import rasterio


def transform_jsonl_to_pandas(path, occ_file):
    """
    Transforms jsonl file produced by get_occurrences_gbif into a pandas dataframe.
    :param path: Path to the root directory containing occurrences/raw/occ_sp_name.jsonl file.
    :param occ_file: occ_sp_name.jsonl with occurrence records from GBIF.
    :return: pandas data frame
    >>> # transform_jsonl_to_pandas(path='./out', occ_file=occ_Abrostola_tripartita.jsonl)
    """
    with open(f'{path}/occurrences/raw/{occ_file}', 'r') as f:
        list_of_records = []

        for line in f:
            record = json.loads(line)
            list_of_records.append(record)

        records = pd.json_normalize(list_of_records)

        return records


def xy_climate_extraction_batch(path_occ_data_file, path_climate_layer_dir):
    """
    Extracts data from climatic layers using geographic coordinates and create a jsonl file containing accession,
    species, decimalLongitude, decimalLatitude, and field for climatic data.
    Both geographic coordinates and climatic layer must have the same coordinate reference system. In this
    case we assume that the coordinate reference system is WGS84.
    :param path_occ_data_file: relative or absolute path to the occurrence data jsonl file containing the fields:
    accession, species, decimalLongitude, and decimalLatitude.
    :param path_climate_layer_dir: relative or absolute path to the climatic layer directory in TIFF (*.tif) format.
    the jsonl file with the extracted data will be saved in this directory.
    :return: No return.
    >>> # Example of usage:
    >>> # xy_climate_extraction_batch(
    >>> #     path_occ_data_file='./out/occurrences/raw/all_species_occurrences.jsonl',
    >>> #     path_climate_layer_dir='./data/climate/'
    >>> #)
    """
    with open(path_occ_data_file, 'r') as f:

        list_of_records = []

        for line in f:
            record = json.loads(line)
            data_to_return = dict()
            data_to_return['accession'] = record['accession']
            data_to_return['species'] = record['species']
            data_to_return['decimalLongitude'] = record['decimalLongitude']
            data_to_return['decimalLatitude'] = record['decimalLatitude']
            list_of_records.append(data_to_return)

        data_df = pd.json_normalize(list_of_records)

        for file in os.listdir(path_climate_layer_dir):
            if file.endswith(".tif"):
                print(f'Extracting {file.rsplit("_")[1]}')

                with rasterio.open(f'{path_climate_layer_dir}{file}') as cli_var:
                    # Extraction produce a generator of arrays. Unpacked using list.
                    extracted_vals = list(
                        cli_var.sample(
                            xy=[(data_df['decimalLongitude'], data_df['decimalLatitude'])]
                        )
                    )
                    # Getting values from array
                    data_df[file.rsplit("_")[1]] = [val.item() for val in extracted_vals]

                print(f'Extraction for {file.rsplit("_")[1]} completed.')

        with open(f'{path_climate_layer_dir}climate_dataset.jsonl', 'w') as jsonl_file:
            for r in range(0, data_df.shape[0]):
                row_dict = data_df.iloc[r].to_dict()
                jsonl_file.write(f'{json.dumps(row_dict)}\n')
            print(f'Climate data extraction file climate_dataset.jsonl saved to {path_climate_layer_dir}')