import json
import pandas as pd


def transform_jsonl_to_pandas(path, occ_file):
    with open(f'{path}/occurrences/raw/{occ_file}', 'r') as f:
        list_of_records = []

        for line in f:
            record = json.loads(line)
            list_of_records.append(record)

        records = pd.json_normalize(list_of_records)

        return records