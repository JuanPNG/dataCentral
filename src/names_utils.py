import json
import re
import warnings

from pygbif import species as gbif_spp
from src.es_utils import get_species_data_es


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


def extract_name_gnote_title(x):
    """
    Extract the full scientific name of the species with author and date from the genome note title.
    :param x: A string containing the title of the genome note
    :return: A dictionary containing the title, the extracted scientific name, and tax_id
    >>> # title =  "The genome sequence of the starlet sea anemone, Nematostella vectensis (Stephenson, 1935)"
    >>> # extract_scientific_name_gnote_title(title)
    Expected output:
    {
        "title": "The genome sequence of the starlet sea anemone, Nematostella vectensis (Stephenson, 1935)",
        "extracted_name": "Nematostella vectensis (Stephenson, 1935)"
    }
    """
    if type(x) is not str:
        raise Exception(f'Invalid input {print(x)}. Please provide a string.')

    name_cat = dict()
    name_cat['title'] = x

    shortened_title = x.replace('The genome sequence of ', '')

    ext_name = re.findall(
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s\([A-zÀ-ȕ]+,\s[0-9]+\)$|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s[A-zÀ-ȕ]+\s[0-9]+|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s[A-zÀ-ȕ]+,\s[0-9]+|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s[A-zÀ-ȕ]+\.|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s\([A-zÀ-ȕ]+\.\)\s[A-zÀ-ȕ]+.,\s[0-9]+|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s\([A-zÀ-ȕ]+\s[0-9]+\)|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s\([A-zÀ-ȕ\s&.,]+[0-9]+\)|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s\([A-zÀ-ȕ\s&.,]+\)[A-zÀ-ȕ\s&.]+|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s\([A-zÀ-ȕ\s&.,]+\)[A-zÀ-ȕ\s&.,]+[0-9]+|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ-,]+\s\([A-zÀ-ȕ\s&.,]+\s[0-9]+\)|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s\([A-zÀ-ȕ\s&.,]+\)[A-zÀ-ȕ\s&.,]+|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s\([A-zÀ-ȕ\s&.,0-9\)]+|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s[A-zÀ-ȕ-]+,\s[0-9]+|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s[A-zÀ-ȕ-]+\s[A-zÀ-ȕ-]+,\s[0-9]+|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s[A-zÀ-ȕ\.,]+\s[0-9]+|'
        r'[A-zÀ-ȕ]+\s\([A-zÀ-ȕ]+\)\s[a-zÀ-ȕ]+\s\([A-zÀ-ȕ\s&.,0-9]+\)|'
        r'[A-zÀ-ȕ]+\s\([A-zÀ-ȕ\s&.,0-9]+\)|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s$|'
        r'[A-zÀ-ȕ]+\s[a-zÀ-ȕ]+\s\([\(\)[A-zÀ-ȕ\s&.,0-9]+',
        shortened_title)

    try:
        name_cat['extracted_name'] = ext_name[0]
    except IndexError:
        name_cat['extracted_name'] = 'NOT_AVAILABLE'

    return name_cat


def complement_taxonomy_gnote(path, es_conn):
    """
    Complements the ena taxonomy with the authority scientific name from a genome note title
    :param path: Path to the taxonomy_ena.jsonl file.
    :param es_conn: Connection to the Elasticsearch
    :return: The same taxonomy_ena.jsonl file but with genome_note key containing a dictionary with title
    and extracted name.
    """
    with open(f'{path}/taxonomy_ena_aut.jsonl', 'w') as aut:
        with open(f'{path}/taxonomy_ena.jsonl', 'r') as tax:
            species_data = get_species_data_es(
                index_name='data_portal',
                es_conn=es_conn
            )

            for line in tax:
                data = json.loads(line)
                title = get_genome_note_title(spp_name=data['species'], species_portal_data=species_data)
                data['genome_note'] = extract_name_gnote_title(x=title)
                aut.write(f"{json.dumps(data)}\n")


def complement_taxonomy_gbif_id(path):
    """
    Complements the ena taxonomy with the GBIF usageKey for the species name.
    :param path: Path to the taxonomy_ena.jsonl file.
    :return: The same taxonomy_ena.jsonl file but with GBIF usageKey
    """
    with open(f'{path}/taxonomy_ena_gbif.jsonl', 'w') as aut:
        with open(f'{path}/taxonomy_ena.jsonl', 'r') as tax:
            for i, line in enumerate(tax):
                data = json.loads(line)
                print(f'Working on {i}. {data["species"]}')
                gbif_record = gbif_spp.name_backbone(
                    name=data['species'],
                    family=data['family']
                )
                data['gbif_usageKey'] = gbif_record['usageKey']
                data['gbif_scientificName'] = gbif_record['scientificName']
                data['gbif_status'] = gbif_record['status']
                data['gbif_confidence'] = gbif_record['confidence']
                data['gbif_matchType'] = gbif_record['matchType']
                aut.write(f"{json.dumps(data)}\n")
