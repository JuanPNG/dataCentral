import os

from airflow.decorators import dag, task, task_group
from airflow.models import Variable

from datetime import datetime

from dotenv import load_dotenv

from elasticsearch import Elasticsearch

from sqlalchemy import create_engine, text

load_dotenv()

@dag(
    dag_id="compara_data_ingestion",
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

        with open(f'/Users/juann/PycharmProjects/dataCentral/out/compara/gbpd_compara_taxon_ids_{timestamp}.jsonl', 'w') as f:
            for db, vals in compara_sets.items():

                ids = gbdp_set.intersection(vals)
                db_ids = {
                    'database': db,
                    'n_shared': len(ids),
                    'ids': ids
                }
                shared_taxon_ids.append(db_ids)
                f.write(f'{db_ids}\n')

        return shared_taxon_ids

    gbdp_taxon_ids = get_gbdp_taxon_ids()
    compara_taxon_ids = compara_taxon_ids()

    gpbp_compara_ids = get_shared_taxon_ids(
        gbdp_set=gbdp_taxon_ids,
        compara_sets=compara_taxon_ids
    )

    print(gpbp_compara_ids)


compara_pipeline = compara_data_ingestion()
