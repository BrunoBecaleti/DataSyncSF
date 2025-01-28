# controller.py

from utils.salesforce_utils import authenticate_salesforce, find_all_dependencies
from services.migration_service import process_object_with_dependencies
from config import sandbox_credentials, dev_credentials  # Usando sandbox_credentials

# Controlador para iniciar a migração de um objeto e suas dependências
def start_migration(object_name):
    # Autenticação na sandbox e na org de desenvolvimento
    sf_sandbox = authenticate_salesforce(**sandbox_credentials)
    sf_dev = authenticate_salesforce(**dev_credentials)

    # Buscar todas as dependências do objeto, incluindo dependências de dependências
    dependencies = find_all_dependencies(sf_sandbox, object_name)
    
    # Inclui o objeto principal no final (depois das dependências)
    ordered_objects = dependencies + [object_name]
    
    # Remover duplicatas mantendo a ordem
    ordered_objects = list(dict.fromkeys(ordered_objects))
    
    # Processar o objeto e suas dependências
    process_object_with_dependencies(sf_sandbox, sf_dev, object_name, ordered_objects)
