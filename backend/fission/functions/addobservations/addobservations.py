'''
 === Cluster Cloud Computing Project | Team 60 ===
 This function is implemented by Team 60
- Angqi Meng - 1268867
- Yichen Long - 1497321
- Xuan Wu - 1483104
- Zining Zhang - 1508501
- Jingqiu Meng - 1506602
'''
import logging
import json
import os
from typing import Dict, List, Any
from flask import current_app, request
from elasticsearch8 import Elasticsearch


def config(k: str) -> str:
    secret_path = f'/secrets/default/es-credentials/{k}'
    if os.path.exists(secret_path):
        with open(secret_path, 'r') as f:
            return f.read().strip()
    with open(f'/configs/default/addobservations-config/{k}', 'r') as f:
        return f.read().strip()

def main():
    """Process and index social-media observations into Elasticsearch.

    Handles:
    - Elasticsearch client initialization with security credentials
    - Suppression of SSL warnings for self-signed certificates
    - Bulk indexing of observation records
    - Document ID generation using station ID and timestamp
    - Request payload validation and logging

    Returns:
        'ok' on successful processing of all observations

    Connection and indexing failures are returned to the message-queue caller
    as explicit error responses so Fission can retry the message.
    """
    # Initialize Elasticsearch client
    try:
        es_client: Elasticsearch = Elasticsearch(
            'https://elasticsearch-master.elastic.svc.cluster.local:9200',
            verify_certs=False,
            ssl_show_warn=False,
            basic_auth=(config("ES_USERNAME"), config("ES_PASSWORD"))
        )
        current_app.logger.info("Elasticsearch client initialised")
    except Exception:
        current_app.logger.exception("Elasticsearch client initialisation failed")
        return "ERROR", 503

    # Validate and parse request payload
    request_data: List[Dict[str, Any]] = request.get_json(force=True)
    current_app.logger.info(f'Processing {len(request_data)} observations')

    # Index each observation
    for observation in request_data:
        doc_id: str = f'{observation["post_id"]}-{observation["created_at"]}'
        try:
            index_response: Dict[str, Any] = es_client.index(
                index='observations',
                id=doc_id,
                body=observation
            )
            current_app.logger.info(
                f'Indexed observation {doc_id} - '
                f'Version: {index_response["_version"]}'
            )
        except Exception:
            current_app.logger.exception(
                "Failed to index observation %s", doc_id
            )
            return "ERROR"

    return 'OK'
