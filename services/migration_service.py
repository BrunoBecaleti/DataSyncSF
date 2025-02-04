from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceMalformedRequest, SalesforceError
from colorama import init, Fore, Style

from utils.salesforce_utils import get_edge_field_name, get_object_fields, system_fields

from simple_salesforce.exceptions import SalesforceMalformedRequest

from utils.storage_utils import estimate_data_size, get_storage_limits


# Inicializa o Colorama
init(autoreset=True)


def selectObject(sf_sandbox, object_name, fields, where_clause=""):
    """Seleciona um objeto no Salesforce e retorna os registros."""
    fields_str = ', '.join(fields)

    query = f"SELECT {fields_str} FROM {object_name} {where_clause} LIMIT 1"  # Limitando a 10 registros
    print(Fore.GREEN + f"Executando query para buscar registros de {object_name}: {query}")
    records = sf_sandbox.query(query)['records']
    records = [{k: v for k, v in record.items() if k != 'attributes'} for record in records]

    print(Fore.BLUE + f"Número de registros encontrados em {sf_sandbox._instance_url}: {len(records)} para o objeto {object_name}")
    return records


def process_object_with_dependencies(sf_sandbox, sf_dev, object_name, ordered_objects):
    """Processa um objeto no Salesforce e migra os dados, lidando com dependências e erros de permissão."""
    storage_remaining_mb = get_storage_limits(sf_dev)

    # Lista de campos de sistema a serem sempre ignorados

    for obj in ordered_objects:
        fields_info_sandbox = get_object_fields(sf_sandbox, obj)
        fields_info_dev = get_object_fields(sf_dev, obj)
        
        # Usar apenas campos que existem em ambos os ambientes
        common_fields = set(fields_info_sandbox.keys()) & set(fields_info_dev.keys()) - set(system_fields)
        fields_str = ', '.join(common_fields)

        query = f"SELECT {fields_str} FROM {obj} LIMIT 1"  # Limitando a 10 registros
        print(Fore.GREEN + f"Executando query para buscar registros de {obj}: {query}")
        records = sf_sandbox.query(query)['records']
        records = [{k: v for k, v in record.items() if k != 'attributes'} for record in records]

        print(Fore.BLUE + f"Número de registros encontrados em {sf_sandbox._instance_url}: {len(records)} para o objeto {obj}")

        for record in records:
            # Remover campos do sistema
            record = {k: v for k, v in record.items() if k not in system_fields}

            problematic_fields = set()
            max_attempts = 3
            attempt = 0

            while attempt < max_attempts:
                try:
                    # Remover campos problemáticos identificados em tentativas anteriores
                    record = {k: v for k, v in record.items() if k not in problematic_fields}

                    # Remover campos de lookup (que terminam com '__c' e contêm IDs)
                    record = {k: v for k, v in record.items() if not (k.endswith('__c') and isinstance(v, str) and v.startswith('001'))}

                    # Tentar inserir o registro
                    print(Fore.YELLOW + f"Tentativa {attempt + 1} de inserir registro para o objeto {obj}: {record}")
                    result = sf_dev.__getattr__(obj).create(record)
                    
                    if result['success']:
                        print(Fore.GREEN + f"Registro inserido com sucesso: {result['id']}")
                        print(Fore.MAGENTA + f"Dados inseridos: {record}")  # Novo print colorido
                        break  # Sair do loop se a inserção for bem-sucedida
                    else:
                        print(Fore.RED + f"Erro ao inserir registro: {result['errors']}")
                        # Adicionar campos problemáticos à lista
                        for error in result['errors']:
                            if 'fields' in error:
                                problematic_fields.update(error['fields'])
                
                except SalesforceMalformedRequest as e:
                    print(Fore.RED + f"Erro ao processar registro: {str(e)}")
                    # Extrair campos problemáticos da mensagem de erro
                    if 'Unable to create/update fields:' in str(e):
                        error_fields = str(e).split('Unable to create/update fields:')[1].split('.')[0].strip().split(', ')
                        problematic_fields.update(error_fields)
                    elif 'INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY' in str(e):
                        print(Fore.YELLOW + "Erro de permissão detectado. Ignorando este registro.")
                        break  # Sair do loop, pois não podemos inserir este registro devido a restrições de permissão
                
                except SalesforceError as e:
                    print(Fore.RED + f"Erro do Salesforce: {str(e)}")
                    if 'INSUFFICIENT_ACCESS_OR_READONLY' in str(e):
                        print(Fore.YELLOW + "Erro de permissão detectado. Ignorando este registro.")
                        break  # Sair do loop, pois não podemos inserir este registro devido a restrições de permissão
                    
                attempt += 1

                if attempt == max_attempts:
                    print(Fore.RED + f"Falha ao inserir registro após {max_attempts} tentativas.")

            storage_remaining_mb -= estimate_data_size([record])
            print(Fore.CYAN + f"Registro de {obj} processado. Espaço restante: {storage_remaining_mb} MB.")

        print(Fore.GREEN + f"Processamento concluído para o objeto {obj}")