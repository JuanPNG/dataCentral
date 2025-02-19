import json
import re

import requests
from lxml import etree
from pygbif import species as gbif_spp
from src.es_utils import get_species_data_es, get_genome_note_title


def get_ena_taxonomy(accessions_file, taxonomy_file):
    """
    Get taxonomy information from ENA API using tax_id from Biodiversity Portal
    :param accessions_file: JSONL file path with a list of dictionaries containing accession numbers and tax_id keys
    :param taxonomy_file: file path to save the retrieved taxonomy in JSONL format
    :return: No return.
    """

    with open(taxonomy_file, 'w') as tax:

        with open(accessions_file, 'r') as f:

            phylogenetic_ranks = ('kingdom', 'phylum', 'class', 'order', 'family', 'genus')

            for i, line in enumerate(f):
                print(f'Working on {i}')
                accession_tax = {}
                data = json.loads(line.rstrip())
                accession_tax['accession'] = data["accession"]
                accession_tax['tax_id'] = data["tax_id"]

                for rank in phylogenetic_ranks:
                    accession_tax[rank] = None

                ena_resp = requests.get(f'https://www.ebi.ac.uk/ena/browser/api/xml/{data['tax_id']}')
                root = etree.fromstring(ena_resp.content)

                accession_tax['species'] = root.find('taxon').get('scientificName')

                try:
                    for taxon in root.find('taxon').find('lineage').findall('taxon'):
                        rank = taxon.get('rank')
                        if rank in phylogenetic_ranks:
                            scientific_name = taxon.get('scientificName')
                            accession_tax[rank] = scientific_name if scientific_name else None
                except AttributeError:
                    pass

                tax.write(f"{json.dumps(accession_tax)}\n")


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
