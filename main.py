
from simple_salesforce import Salesforce
from utils.salesforce_utils import authenticate_salesforce, build_relationship_graph, get_direct_dependencies, get_edge_field_name, get_object_fields, get_reference_fields, process_record_with_dependencies, visualize_graph, selectObject

from config import sandbox_credentials, dev_credentials

import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="Migração de dados do Salesforce")
    parser.add_argument('--object', required=True,help="Nome do objeto a ser migrado")
    return parser.parse_args()

def main():
    args = parse_args()
    object_name = args.object

    # Autenticar nas duas orgs
    sf_sandbox = authenticate_salesforce(**sandbox_credentials)
    sf_dev = authenticate_salesforce(**dev_credentials)

    if not sf_sandbox or not sf_dev:
        print("Falha na autenticação. Verifique as credenciais e tente novamente.")
        return
    

    relationship_graph = build_relationship_graph(sf_sandbox, object_name)

    direct_dependencies= get_direct_dependencies(relationship_graph, object_name)

    rootObjectFields= get_object_fields(sf_sandbox, sf_dev, object_name)

    records= selectObject(sf_sandbox, object_name, rootObjectFields)


    for record in records:
        result = process_record_with_dependencies(sf_sandbox, sf_dev, object_name, record, direct_dependencies)
        print(f"Registro inserido com sucesso: {result['id']}")
    # Processar migração
    #process_object_with_dependencies(sf_sandbox, sf_dev, allObjects)

if __name__ == '__main__':
    main()
