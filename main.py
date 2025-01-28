
from simple_salesforce import Salesforce
from utils.salesforce_utils import authenticate_salesforce
from services.migration_service import process_object_with_dependencies
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

    # Objeto a ser migrado
    ordered_objects = [object_name]  # Você pode expandir isso para incluir dependências

    # Processar migração
    process_object_with_dependencies(sf_sandbox, sf_dev, object_name, ordered_objects)

if __name__ == '__main__':
    main()
