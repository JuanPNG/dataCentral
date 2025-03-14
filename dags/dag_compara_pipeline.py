import json
import os
import re

from airflow.decorators import dag, task, task_group
from airflow.models import Variable

from datetime import datetime

from dotenv import load_dotenv

from elasticsearch import Elasticsearch

from google.cloud import storage

from sqlalchemy import create_engine, text

load_dotenv()


@dag(
    dag_id='compara_data_ingestion',
    schedule_interval=None
)
def compara_data_ingestion():
    @task
    def get_gbdp_taxon_ids(size=100, pages=11):
        taxon_ids = set()
        after = None

        # For local machine
        # es = Elasticsearch(
        #     hosts=os.getenv('ES_HOST'),
        #     basic_auth=(
        #         os.getenv('ES_USERNAME'),
        #         os.getenv('ES_PASSWORD')
        #     )
        # )

        es = Elasticsearch(
            hosts=Variable.get('ES_HOST'),
            basic_auth=(
                Variable.get('ES_USERNAME'),
                Variable.get('ES_PASSWORD')
            )
        )

        for page in range(pages):
            body = {
                'size': size,
                'sort': {'tax_id': 'asc'},
                'query': {
                    'match': {
                        'annotation_complete': 'Done'
                    }
                }
            }

            if after:
                body['search_after'] = after

            data = es.search(
                index='data_portal',
                body=body
            )

            hits = data['hits']['hits']

            for record in hits:
                taxon_ids.add(record['_source']['tax_id'])

            if hits:
                after = hits[-1]['sort']

        else:

            return taxon_ids

    @task
    def get_compara_taxon_ids(connection_string):
        db = create_engine(connection_string)
        with db.connect() as connection:
            taxon_ids = set()
            query = text('SELECT DISTINCT taxon_id FROM genome_db;')
            result = connection.execute(query)
            for row in result:
                taxon_ids.add(row[0])

        return taxon_ids

    @task_group
    def compara_taxon_ids():

        all_taxon_ids = {
            # 'FUNG': get_compara_taxon_ids(os.getenv('ENS_COMPARA_FUNG')),
            # 'META': get_compara_taxon_ids(os.getenv('ENS_COMPARA_META')),
            # 'PANH': get_compara_taxon_ids(os.getenv('ENS_COMPARA_PANH')),
            # 'PLAN': get_compara_taxon_ids(os.getenv('ENS_COMPARA_PLAN')),
            # 'PROT': get_compara_taxon_ids(os.getenv('ENS_COMPARA_PROT')),
            # 'VERT': get_compara_taxon_ids(os.getenv('ENS_COMPARA_VERT'))
            'FUNG': get_compara_taxon_ids(Variable.get('ENS_COMPARA_FUNG')),
            'META': get_compara_taxon_ids(Variable.get('ENS_COMPARA_META')),
            'PANH': get_compara_taxon_ids(Variable.get('ENS_COMPARA_PANH')),
            'PLAN': get_compara_taxon_ids(Variable.get('ENS_COMPARA_PLAN')),
            'PROT': get_compara_taxon_ids(Variable.get('ENS_COMPARA_PROT')),
            'VERT': get_compara_taxon_ids(Variable.get('ENS_COMPARA_VERT'))
        }

        return all_taxon_ids

    @task
    def get_shared_taxon_ids(gbdp_set, compara_sets: dict):
        shared_taxon_ids = []
        timestamp = datetime.now().strftime('%Y%m%d-%H%M')

        with open(f'/Users/juann/PycharmProjects/dataCentral/out/compara/gbpd_compara_taxon_ids_{timestamp}.jsonl',
                  'w') as f:
            for db, vals in compara_sets.items():
                db_ids = dict()
                ids = gbdp_set.intersection(vals)
                db_ids[db] = {
                    # 'database': db,
                    'n_shared': len(ids),
                    'ids': list(ids)
                }
                shared_taxon_ids.append(db_ids)
                f.write(f'{json.dumps(db_ids)}\n')

        return shared_taxon_ids

    @task
    def get_compara_hom_stats(connection_string, taxon_ids):

        database = re.search('^.*[0-9]+/(.*)$', connection_string).group(1)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if len(taxon_ids) == 0:
            return print(f'Database {database} does not have shared taxon ids.')
        elif len(taxon_ids) == 1:
            string_taxon_ids = str(taxon_ids[0])
        else:
            string_taxon_ids = ', '.join(str(x) for x in taxon_ids)

        query_string = text(
            f'SELECT DISTINCT m.stable_id AS gene_stable_id, '  # g.genome_db_id, g.assembly, 
            f'm.description, h.families, h.gene_trees, h.gene_gain_loss_trees, '
            f'h.orthologues, h.paralogues, h.homoeologues '
            f'FROM genome_db as g '
            f'JOIN gene_member as m '
            f'USING(genome_db_id, taxon_id) '
            f'JOIN gene_member_hom_stats as h  '
            f'USING(gene_member_id) '
            f'WHERE taxon_id IN ({string_taxon_ids})'
            f'LIMIT 10;'
        )

        print(f'Querying {database}')
        db = create_engine(connection_string)
        with db.connect() as connection:
            query = query_string
            result = connection.execute(query)

            # with open(f'./out/compara/homology_stats_{database}_{timestamp}.jsonl', 'w') as outfile:
            #     for row in result:
            #         # print(row._asdict())
            #         outfile.write(f'{json.dumps(row._asdict())}\n')

            print(f'Uploading {database} results to Google Storage')
            storage_client = storage.Client(os.getenv("GCP_PROJECT_PROD"))
            bucket_name = os.getenv("GCS_BUCKET")
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(f'homology_stats_{database}_{timestamp}.jsonl')

            with blob.open(mode='w') as f:
                for row in result:
                    f.write(f'{json.dumps(row._asdict())}\n')
            print(f'Query results saved in {blob.name}')

    @task
    def get_compara_gene_trees(connection_string):
        database = re.search('^.*[0-9]+/(.*)$', connection_string).group(1)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        query_string = text(
            "SELECT gtr.clusterset_id, "  # gtr.member_type,
            "IFNULL(CONCAT(gtr.stable_id, '.', gtr.version), CONCAT('Node_', gtr.root_id)) AS gene_tree_id, "
            "gm.stable_id AS gene_stable_id, gm.description "
            "FROM gene_tree_root gtr "
            "JOIN gene_tree_node gtn ON gtn.root_id = gtr.root_id "
            "JOIN gene_member gm on gtn.seq_member_id = gm.canonical_member_id "
            "WHERE gtr.tree_type = 'tree' AND gtr.ref_root_id IS NULL;"
        )

        print(f'Querying {database}')
        db = create_engine(connection_string)
        with db.connect() as connection:
            query = query_string
            result = connection.execute(query)

            # with open(f'./out/compara/gene_trees_{database}_{timestamp}.jsonl', 'w') as outfile:
            #     for row in result:
            #         # print(row._asdict())
            #         outfile.write(f'{json.dumps(row._asdict())}\n')

            print(f'Uploading {database} results to Google Storage')
            storage_client = storage.Client(os.getenv("GCP_PROJECT_PROD"))
            bucket_name = os.getenv("GCS_BUCKET")
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(f'gene_trees_{database}_{timestamp}.jsonl')

            with blob.open(mode='w') as f:
                for row in result:
                    f.write(f'{json.dumps(row._asdict())}\n')
            print(f'Query results saved in {blob.name}')

    # TO DO: add airflow variables
    @task_group(name='gene_trees')
    def compara_gene_trees():
        get_compara_gene_trees(connection_string=os.getenv('ENS_COMPARA_FUNG'))
        get_compara_gene_trees(connection_string=os.getenv('ENS_COMPARA_META'))
        get_compara_gene_trees(connection_string=os.getenv('ENS_COMPARA_PLAN'))
        get_compara_gene_trees(connection_string=os.getenv('ENS_COMPARA_VERT'))

    @task_group(name='gene_trees')
    def compara_homology_stats(taxon_ids):
        get_compara_hom_stats(
            connection_string=os.getenv('ENS_COMPARA_FUNG'),
            taxon_ids=taxon_ids[0]['FUNG']['ids']
        )

        get_compara_hom_stats(
            connection_string=os.getenv('ENS_COMPARA_META'),
            taxon_ids=taxon_ids[1]['META']['ids']
        )

        get_compara_hom_stats(
            connection_string=os.getenv('ENS_COMPARA_PLAN'),
            taxon_ids=taxon_ids[3]['PLAN']['ids']
        )

        get_compara_hom_stats(
            connection_string=os.getenv('ENS_COMPARA_VERT'),
            taxon_ids=taxon_ids[5]['VERT']['ids']
        )

    gbdp_taxon_ids = get_gbdp_taxon_ids()

    compara_taxon_ids = compara_taxon_ids()

    gpbp_compara_ids = get_shared_taxon_ids(
        gbdp_set=gbdp_taxon_ids,
        compara_sets=compara_taxon_ids
    )

    compara_homology_stats(taxon_ids=gpbp_compara_ids)

    # TO DO: Define dependencies for:
    get_compara_gene_trees()

    print(gpbp_compara_ids)


compara_pipeline = compara_data_ingestion()
