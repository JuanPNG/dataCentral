import json
import re

import requests
from lxml import etree
from pygbif import species as gbif_spp
from src.es_utils import get_species_data_es, get_genome_note_title


def get_annotation_taxonomy_ena(path):
    with open(f'{path}/taxonomy_ena.jsonl', 'w') as tax:
        with open(f'{path}/annotations_parsed.jsonl', 'r') as f:
            for i, line in enumerate(f):
                print(f"Working on: {i}")
                sample_to_return = dict()
                data = json.loads(line.rstrip())
                sample_to_return["accession"] = data["accession"]

                response = requests.get(
                    f"https://www.ebi.ac.uk/ena/browser/api/xml/{sample_to_return['accession']}")
                root = etree.fromstring(response.content)
                sample_to_return['tax_id'] = root.find("ASSEMBLY").find("TAXON").find("TAXON_ID").text

                phylogenetic_ranks = ('kingdom', 'phylum', 'class', 'order', 'family', 'genus')

                for rank in phylogenetic_ranks:
                    sample_to_return[rank] = None

                response = requests.get(f"https://www.ebi.ac.uk/ena/browser/api/xml/{sample_to_return['tax_id']}")
                root = etree.fromstring(response.content)

                sample_to_return['species'] = root.find('taxon').get('scientificName')

                try:
                    for taxon in root.find('taxon').find('lineage').findall('taxon'):
                        rank = taxon.get('rank')
                        if rank in phylogenetic_ranks:
                            scientific_name = taxon.get('scientificName')
                            sample_to_return[rank] = scientific_name if scientific_name else None
                except AttributeError:
                    pass
                tax.write(f"{json.dumps(sample_to_return)}\n")


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
