
import json
import requests
from bs4 import BeautifulSoup
from lxml import etree


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
