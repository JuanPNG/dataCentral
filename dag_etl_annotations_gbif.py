from airflow.decorators import dag, task
from bs4 import BeautifulSoup

from src.annotations_utils import *


@dag()
def etl_annotations_gbif():
    @task
    def parse_annotations(url, path):
        print(f'Parsing annotations urls from {url}')

        # Create an HTTP GET request
        annotations = requests.get(url)

        # Raise an exception if we made a request resulting in an error
        annotations.raise_for_status()

        # Access the content of the response in Unicode
        annotations_text = annotations.text

        # Use BeautifulSoup to parse the HTML
        soup_archive = BeautifulSoup(annotations_text, 'html.parser')
        rows = soup_archive.find_all("tr")

        # Saving JSON as an object in GCS
        # client = storage.Client(project=GCP_PROJECT)
        # bucket = client.get_bucket(GCP_PROJECT)
        # blob = bucket.blob(json_file_name)
        # print(f'{blob.name} storage object created.')

        # with blob.open("w") as f:
        with open(f'{path}/annotations_parsed.jsonl', 'w') as f:
            for row in rows:
                try:
                    cells = row.find_all("td")
                    links = cells[4].find_all("a")
                    link = links[0].get("href").strip()
                    accession = cells[2].text.strip()
                    record = dict()
                    record['accession'] = accession
                    record['link'] = link
                    f.write(f"{json.dumps(record)}\n")
                except IndexError:
                    continue

        print(f'Annotations urls saved to {path}/annotations_parsed.jsonl')

    @task
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


    @task
    def complement_taxonomy_gbif_id(path):
        """
        Complements the ena taxonomy with the GBIF usageKey for the species name.
        :param path: Path to the taxonomy_ena.jsonl file.
        :return: The same taxonomy_ena.jsonl file but with GBIF usageKey
        """
        with open(f'{path}/taxonomy_ena_gbif.jsonl', 'w') as aut:
            with open(f'{path}/taxonomy_ena.jsonl', 'r') as tax:
                for line in tax:
                    data = json.loads(line)
                    data['gbif_usageKey'] = gbif_spp.name_backbone(data['species'])['usageKey']
                    aut.write(f"{json.dumps(data)}\n")