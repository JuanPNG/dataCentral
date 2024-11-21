import json
import os
from pathlib import Path

import pandas as pd
import geopandas as gpd
import rasterio


def transform_jsonl_to_pandas(path_occ_file):
    """
    Transforms jsonl file with occurrence data into a pandas dataframe.
    :param path_occ_file: Path to a JSONL file containing the data.
    :return: pandas data frame
    >>> # transform_jsonl_to_pandas(path_occ_file='./out/occ_Abrostola_tripartita.jsonl')
    """
    with open(f'{path_occ_file}', 'r') as f:
        list_of_records = []

        for line in f:
            record = json.loads(line)
            list_of_records.append(record)

        records = pd.json_normalize(list_of_records)

        return records


def xy_climate_extraction_batch(path_occ_data_file, path_climate_layer_dir):
    """
    Extracts data from CHELSA bioclimatic layers using geographic coordinates and create a jsonl file containing
    accession, species, decimalLongitude, decimalLatitude, and field for climatic data.
    Both geographic coordinates and climatic layer must have the same coordinate reference system. In this case we
    assume that the coordinate reference system is WGS84.
    Climatic layer names must follow the default name given by CHELSA: "CHELSA_{your_var}_1981-2010_V.2.1.tif"
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

                with rasterio.open(f'{path_climate_layer_dir}{file}') as clim_var:

                    if clim_var.crs != 'EPSG:4326':
                        raise TypeError(
                            'Climatic layer must have the EPSG:4326 coordinate reference system. Got {}'.format(
                                clim_var.crs)
                        )

                    # Extraction produce a generator of arrays. Unpacked using list.
                    extracted_vals = list(
                        clim_var.sample(
                            xy=[(data_df['decimalLongitude'], data_df['decimalLatitude'])]
                        )
                    )
                    # Getting values from array and saving them as vars in data frame
                    data_df[file.rsplit("_")[1]] = [val.item() for val in extracted_vals]

                print(f'{file.rsplit("_")[1]} extraction completed.')

        with open(f'{path_climate_layer_dir}climate_dataset.jsonl', 'w') as jsonl_file:

            for r in range(0, data_df.shape[0]):
                row_dict = data_df.iloc[r].to_dict()
                jsonl_file.write(f'{json.dumps(row_dict)}\n')

            print(f'Climate data extraction file climate_dataset.jsonl saved to {path_climate_layer_dir}')


def xy_vector_annotation(path_occ_data_file, path_vector_file, path_to_save_dir):
    """
    Execute a spatial join of occurrences geographic points with spatial vector layers.
    :param path_occ_data_file: path to the JSONL file containing GBIF occurrence data.
    :param path_vector_file: path to the file containing the spatial vector layer.
    :param path_to_save_dir: path to directory to save the spatially annotated occurrences.
    :return: No return
    >>> # Example of usage:
    >>> # xy_vector_annotation(
    >>> # path_occ_data_file='./out/occurrences/raw/all_species_occurrences.jsonl',
    >>> # path_vector_file='./data/bioregions/Ecoregions2017.zip',
    >>> # path_to_save_dir='./out/'
    >>> # )
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

        geom = gpd.points_from_xy(data_df['decimalLongitude'], data_df['decimalLatitude'], crs='EPSG:4326')

        data_gpd = gpd.GeoDataFrame(data_df, geometry=geom)

        spatial_layer = gpd.read_file(path_vector_file)

        if spatial_layer.crs != 'EPSG:4326':
            raise TypeError(
                'Vector layer must have the EPSG:4326 coordinate reference system. Got {}'.format(
                    spatial_layer.crs)
            )

        layer_name = path_vector_file.rsplit('/')[-1].split('.')[0]
        print(f'Extracting data from {layer_name}')
        data_annotation = data_gpd.sjoin(spatial_layer, how='left')
        print('Extraction completed.')

        columns_to_drop = ['geometry', 'index_right', 'OBJECTID', 'SHAPE_LENG', 'SHAPE_AREA',
                           'COLOR', 'COLOR_BIO', 'COLOR_NNH']

        data_annotation = data_annotation.drop(columns=columns_to_drop)

        print(data_annotation.columns)

        data_annotation = data_annotation.fillna('NaN')

        Path(f'{path_to_save_dir}spatial_annotation/').mkdir(parents=True, exist_ok=True)

        with open(f'{path_to_save_dir}spatial_annotation/{layer_name}_annotation_dataset.jsonl', 'w') as jsonl_file:

            for r in range(0, data_annotation.shape[0]):
                row_dict = data_annotation.iloc[r].to_dict()
                jsonl_file.write(f'{json.dumps(row_dict)}\n')

            print(f'Vector data extraction file saved to '
                  f'{path_to_save_dir}spatial_annotation/{layer_name}_annotation_dataset.jsonl')