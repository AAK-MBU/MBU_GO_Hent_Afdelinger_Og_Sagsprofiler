"""This module contains the main process of the robot."""
import json
from typing import Dict, Any

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from robot_framework.sub_processes.term_data_handler import pull_term_data_from_go_to_sql
from robot_framework.sub_processes.taxonomy import get_taxononmy


def process(orchestrator_connection: OrchestratorConnection) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")
    credentials = get_credentials_and_constants(orchestrator_connection)
    oc_args_json = json.loads(orchestrator_connection.process_arguments)

    if oc_args_json['process'] == 'taxonomy':
        orchestrator_connection.log_trace("Pull taxononmy data from GO.")
        get_taxononmy(credentials, oc_args_json['caseType'], oc_args_json['viewId'],oc_args_json['baseUrl'])
        orchestrator_connection.log_trace("Taxonomy data was successfully pulled from GO.")

    if oc_args_json['process'] == 'term':
        orchestrator_connection.log_trace("Pull term data from GO.")
        pull_term_data_from_go_to_sql(credentials, oc_args_json['baseUrl'], oc_args_json['caseType'], oc_args_json['startTermId'], oc_args_json['storedProcedure'], oc_args_json['termSetUuid'])
        orchestrator_connection.log_trace("Term data was successfully pulled from GO.")


def get_credentials_and_constants(orchestrator_connection: OrchestratorConnection) -> Dict[str, Any]:
    """Retrieve necessary credentials and constants."""
    orchestrator_connection.log_trace("Retrieve credentials and constants.")
    try:
        credentials = {
            "go_api_username": orchestrator_connection.get_credential('go_api').username,
            "go_api_password": orchestrator_connection.get_credential('go_api').password,
            "sql_conn_string": orchestrator_connection.get_constant('DbConnectionString').value
        }
        return credentials
    except AttributeError as e:
        raise SystemExit(e) from e


if __name__ == "__main__":
    oc = OrchestratorConnection.create_connection_from_args()
    process(oc)
