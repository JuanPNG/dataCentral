import json
import pandas as pd


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