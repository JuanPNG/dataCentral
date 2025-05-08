import json
import re
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems
from elasticsearch import Elasticsearch
import requests
from lxml import etree
from pygbif import species as gbif_spp
from pygbif import occurrences as gbif_occ


# -----------------------
# Utility Functions
# -----------------------

def sanitize_species_name(species: str) -> str:
    """
    Extract the genus and species epithet (first two words) and
    sanitize into a filesystem-safe name.
    """
    parts = species.strip().split()
    if not parts:
        return ''
    genus_species = '_'.join(parts[:2])
    safe = re.sub(r'[^A-Za-z0-9_]', '_', genus_species)
    safe = re.sub(r'_+', '_', safe).strip('_')
    return safe


# -----------------------
# Beam DoFn Classes
# -----------------------

class FetchESFn(beam.DoFn):
    """
    Fetch annotated species records from Elasticsearch using pagination.
    Each output is a dictionary with accession, species name, and tax_id.
    """
    def __init__(self, host, user, password, index, page_size, max_pages):
        self.host = host
        self.user = user
        self.password = password
        self.index = index
        self.page_size = page_size
        self.max_pages = max_pages

    def setup(self):
        self.es = Elasticsearch(
            hosts=[{'host': self.host}],
            basic_auth=(self.user, self.password)
        )

    def process(self, element):
        after = None
        for _ in range(self.max_pages):
            query = {
                'size': self.page_size,
                'sort': {'tax_id': 'asc'},
                'query': {'match': {'annotation_complete': 'Done'}}
            }
            if after:
                query['search_after'] = after

            response = self.es.search(index=self.index, body=query)
            hits = response.get('hits', {}).get('hits', [])
            if not hits:
                break

            for hit in hits:
                ann = hit['_source']['annotation'][-1]
                yield {
                    'accession': ann['accession'],
                    'species': ann['species'],
                    'tax_id': hit['_source']['tax_id']
                }

            after = hits[-1].get('sort')


class ENATaxonomyFn(beam.DoFn):
    """
    Retrieve taxonomy information from ENA for each species record.
    Optionally enrich with full lineage ranks.
    """
    def __init__(self, include_lineage=False):
        self.include_lineage = include_lineage

    def process(self, record):
        tax_id = record.get('tax_id')
        if not tax_id:
            yield record
            return

        url = f'https://www.ebi.ac.uk/ena/browser/api/xml/{tax_id}'
        resp = requests.get(url)
        root = etree.fromstring(resp.content)

        record['scientificName'] = root.find('taxon').get('scientificName')
        if self.include_lineage:
            ranks = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus']
            for rank in ranks:
                record[rank] = None
            lineage = root.find('taxon/lineage')
            if lineage is not None:
                for tx in lineage.findall('taxon'):
                    r = tx.get('rank')
                    if r in ranks:
                        record[r] = tx.get('scientificName')
        yield record


class ValidateNamesFn(beam.DoFn):
    """
    Validate species names using GBIF's taxonomic backbone.
    Splits into 'validated' and 'to_check' based on match type and synonym status.
    """
    VALIDATED = 'validated'
    TO_CHECK = 'to_check'

    def process(self, record):
        name = record.get('species')
        if not name:
            yield beam.pvalue.TaggedOutput(self.TO_CHECK, record)
            return

        gb = gbif_spp.name_backbone(name=name, rank='species', strict=False, verbose=False)
        record.update({
            'gbif_matchType': gb.get('matchType'),
            'gbif_confidence': gb.get('confidence'),
            'gbif_scientificName': gb.get('scientificName'),
            'gbif_usageKey': gb.get('usageKey'),
            'gbif_status': gb.get('status'),
            'gbif_rank': gb.get('rank')
        })

        mt = gb.get('matchType')
        st = gb.get('status')
        if mt == 'NONE' or mt != 'EXACT' or st == 'SYNONYM':
            if gb.get('acceptedUsageKey'):
                record['gbif_acceptedUsageKey'] = gb.get('acceptedUsageKey')
            if gb.get('alternatives'):
                record['gbif_alternatives'] = gb.get('alternatives')
            yield beam.pvalue.TaggedOutput(self.TO_CHECK, record)
        else:
            yield record


class WriteSpeciesOccurrencesFn(beam.DoFn):
    """
    Download GBIF occurrences for a species and write to a JSONL file.
    Uses idempotent write pattern and emits metrics and dead-letter output.
    """
    SUCCESS = beam.metrics.Metrics.counter('WriteOcc', 'success')
    SKIPPED = beam.metrics.Metrics.counter('WriteOcc', 'skipped')
    FAILURES = beam.metrics.Metrics.counter('WriteOcc', 'failures')

    def __init__(self, output_dir, max_records=150):
        super().__init__()
        self.output_dir = output_dir
        self.max_records = max_records

    def setup(self):
        self.gbif_client = gbif_occ

    def process(self, record):
        from apache_beam import pvalue
        species = record.get('species')
        key = record.get('gbif_usageKey')
        if not species or key is None:
            return

        safe_name = sanitize_species_name(species)
        filename = f"occ_{safe_name}.jsonl"
        out_path = f"{self.output_dir}/{filename}"
        tmp_path = out_path + '.tmp'

        if FileSystems.exists(out_path):
            self.SKIPPED.inc()
            yield {'species': species, 'status': 'skipped'}
            return

        try:
            resp = self.gbif_client.search(
                taxonKey=key,
                basisOfRecord=['PRESERVED_SPECIMEN', 'MATERIAL_SAMPLE'],
                occurrenceStatus='PRESENT',
                hasCoordinate=True,
                hasGeospatialIssue=False,
                limit=self.max_records
            )
            results = resp.get('results', [])

            lines = [json.dumps({
                'accession': record.get('accession'),
                'tax_id': record.get('tax_id'),
                'gbif_usageKey': o.get('taxonKey'),
                'species': o.get('species'),
                'decimalLatitude': o.get('decimalLatitude'),
                'decimalLongitude': o.get('decimalLongitude'),
                'coordinateUncertaintyInMeters': o.get('coordinateUncertaintyInMeters'),
                'eventDate': o.get('eventDate'),
                'countryCode': o.get('countryCode'),
                'basisOfRecord': o.get('basisOfRecord'),
                'occurrenceID': o.get('occurrenceID'),
                'gbifID': o.get('gbifID')
            }) for o in results]

            with FileSystems.create(tmp_path) as fh:
                for line in lines:
                    fh.write((line + '\n').encode('utf-8'))

            FileSystems.rename([tmp_path], [out_path])
            self.SUCCESS.inc()
            yield {'species': species, 'count': len(results)}

        except Exception as e:
            self.FAILURES.inc()
            yield pvalue.TaggedOutput('dead', {
                'species': species,
                'error': str(e)
            })

# -----------------------
# Pipeline Definitions
# -----------------------

def taxonomy_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as p:
        es_records = (
            p
            | 'StartESFetch' >> beam.Create([None])
            | 'FetchFromES' >> beam.ParDo(FetchESFn(
                host=args.host,
                user=args.user,
                password=args.password,
                index=args.index,
                page_size=args.size,
                max_pages=args.pages
            ))
        )

        enriched = es_records | 'FetchENATaxonomy' >> beam.ParDo(ENATaxonomyFn(args.descendants))

        validated_outputs = enriched | 'ValidateGBIFNames' >> beam.ParDo(ValidateNamesFn()).with_outputs(
            ValidateNamesFn.TO_CHECK,
            main=ValidateNamesFn.VALIDATED
        )

        validated_outputs[ValidateNamesFn.VALIDATED] \
            | 'ToJsonValidated' >> beam.Map(json.dumps) \
            | 'WriteValidated' >> beam.io.WriteToText(
                args.output + '_validated', file_name_suffix='.jsonl', num_shards=1, shard_name_template='')

        validated_outputs[ValidateNamesFn.TO_CHECK] \
            | 'ToJsonToCheck' >> beam.Map(json.dumps) \
            | 'WriteToCheck' >> beam.io.WriteToText(
                args.output + '_tocheck', file_name_suffix='.jsonl', num_shards=1, shard_name_template='')


def occurrences_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as p:
        records = (
            p
            | 'ReadValidatedFile' >> beam.io.ReadFromText(args.validated_input)
            | 'ParseValidatedJson' >> beam.Map(json.loads)
        )

        _ = records | 'FetchAndWriteOccurrences' >> beam.ParDo(
            WriteSpeciesOccurrencesFn(
                output_dir=args.output_dir,
                max_records=args.limit
            ).with_outputs('dead', main='success')
        )


# -----------------------
# Entry Point
# -----------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GBIF Occurrence Pipeline')
    subs = parser.add_subparsers(dest='command')

    tax = subs.add_parser('taxonomy')
    tax.add_argument('--host', required=True)
    tax.add_argument('--user', required=True)
    tax.add_argument('--password', required=True)
    tax.add_argument('--index', required=True)
    tax.add_argument('--size', type=int, default=1000)
    tax.add_argument('--pages', type=int, default=10)
    tax.add_argument('--descendants', action='store_true')
    tax.add_argument('--output', required=True)

    occ = subs.add_parser('occurrences')
    occ.add_argument('--validated_input', required=True)
    occ.add_argument('--output_dir', required=True)
    occ.add_argument('--limit', type=int, default=150)

    args, beam_args = parser.parse_known_args()

    if args.command == 'taxonomy':
        taxonomy_pipeline(args, beam_args)
    elif args.command == 'occurrences':
        occurrences_pipeline(args, beam_args)
    else:
        parser.print_help()
