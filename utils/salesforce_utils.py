from simple_salesforce import Salesforce
import networkx as nx
from colorama import Fore

import matplotlib.pyplot as plt

from simple_salesforce.exceptions import SalesforceMalformedRequest, SalesforceError

# Inicializar o cache globalmente
cache = {}

# Lista de campos do sistema que devem ser ignorados
system_fields = {
    'CreatedDate', 'LastModifiedDate', 'SystemModstamp', 
    'IsDeleted', 'LastModifiedById', 'CreatedById', 'LastViewedDate', 'LastReferencedDate', 'IsActive', 'NamespacePrefix'
}

# Lista de objetos de sistema que devem ser ignorados
system_objects = ['UserRole', 'ApexClass', 'NamedCredential', 'EmailTemplate', 'Task', 'Case', 'EmailMessage', 'ContentAsset', 'AuthProvider', 'ExternalDataSource', 'StaticResource', 'AuthProvider', 'Network', 'Document', 'Folder', 'Network', 'BrandTemplate', 'ContentVersion', 'ContentBody', 'ContentDocument', 'ContentFolder', 'ContentWorkspace','UserLicense', 'Profile', 'PermissionSetAssignment', 'User' ]  # Exemplo de objetos de sistema
def authenticate_salesforce(username, password, security_token, domain='login'):
    # O argumento 'domain' especifica se estamos usando um ambiente de produção ('login') ou sandbox ('test')
    sf = Salesforce(username=username, password=password, security_token=security_token, domain=domain)
    return sf

def get_object_fields(sf_sandbox, sf_dev, object_name):
    """Obtém os campos do objeto especificado."""
    fields_info_sandbox = sf_sandbox.__getattr__(object_name).describe()
    fields_info_dev = sf_dev.__getattr__(object_name).describe()

    # Usar apenas campos que existem em ambos os ambientes
    sandbox_fields = {field['name'] for field in fields_info_sandbox['fields']}
    dev_fields = {field['name'] for field in fields_info_dev['fields']}
    common_fields = sandbox_fields & dev_fields - system_fields

    return list(common_fields)

def selectObject(sf_sandbox, object_name, fields, where_clause=""):
    """Seleciona um objeto no Salesforce e retorna os registros."""
    fields_str = ', '.join(fields)

    query = f"SELECT {fields_str} FROM {object_name} {where_clause} LIMIT 1"  # Limitando a 10 registros
    print(Fore.GREEN + f"Executando query para buscar registros de {object_name}: {query}")
    records = sf_sandbox.query(query)['records']
    records = [{k: v for k, v in record.items() if k != 'attributes'} for record in records]

    print(Fore.BLUE + f"Número de registros encontrados em {sf_sandbox.sf_instance}: {len(records)} para o objeto {object_name}")
    return records

def get_reference_fields(sf, object_name):
    """
    Retorna um dicionário com o nome do campo do objeto e o objeto de referência,
    excluindo os campos e objetos de sistema especificados.
    """
    # Verificar se o objeto já está no cache
    if object_name in cache:
        return cache[object_name]

    try:
        metadata = sf.__getattr__(object_name).describe()
        reference_fields = {}
        for field in metadata['fields']:
            if field['type'] == 'reference' and field['referenceTo'] and field['name'] not in system_fields:
                if field['referenceTo'][0] not in system_objects:
                    reference_fields[field['name']] = field['referenceTo'][0]
        cache[object_name] = reference_fields
        return reference_fields
    except Exception as e:
        print(f"Erro ao obter metadados de {object_name}: {e}")
        return {}

def build_relationship_graph(sf, root_object):
    """Cria um grafo de relacionamento a partir de um objeto raiz, excluindo campos e objetos de sistema especificados."""
    graph = nx.MultiDiGraph()
    visited = set()

    def traverse(object_name):
        reference_fields = get_reference_fields(sf, object_name)
        for field_name, related_object in reference_fields.items():
            if (object_name, field_name) not in visited:
                visited.add((object_name, field_name))
                graph.add_edge(object_name, related_object, field=field_name)
                traverse(related_object)

    traverse(root_object)
    return graph

def get_edge_field_name(graph, source, target):
    """
    Retorna o nome do campo de relacionamento entre dois nós no grafo.
    """
    if graph.has_edge(source, target):
        return [data['field'] for u, v, data in graph.edges(source, data=True) if v == target]
    return None

def get_direct_dependencies(graph, root_object):
    """
    Retorna um dicionário com as dependências diretas de cada objeto no grafo.
    """
    visited = set()
    direct_dependencies = {}

    def traverse(object_name):
        reference_fields = graph.successors(object_name)
        for successor in reference_fields:
            field_names = get_edge_field_name(graph, object_name, successor)
            if field_names:
                for field_name in field_names:
                    if (object_name, field_name) in visited:
                        continue
                    print(f"Campo de relacionamento entre {object_name} e {successor}: {field_name}")
                    visited.add((object_name, field_name))
                    if object_name not in direct_dependencies:
                        direct_dependencies[object_name] = []
                    direct_dependencies[object_name].append((field_name, successor))
                    traverse(successor)

    traverse(root_object)
    return direct_dependencies

def process_record_with_dependencies(sf_sandbox, sf_dev, object_name, record, direct_dependencies):
    """
    Processa um registro no Salesforce, navegando e inserindo dependências antes de inserir o registro principal.
    """
    for field, value in record.items():
        # Verificar se o campo está em direct_dependencies
        if object_name in direct_dependencies:
            for dep_field, dep_successor in direct_dependencies[object_name]:
                if field == dep_field and value:
                    print(f"  -> Campo de relacionamento encontrado: {dep_field} (Objeto: {dep_successor})")
                    # Obter os campos do objeto de dependência
                    fields = get_object_fields(sf_sandbox, sf_dev, dep_successor)
                    # Selecionar o registro de dependência
                    dependent_records = selectObject(sf_sandbox, dep_successor, fields, where_clause=f" WHERE Id = '{value}' ")
                    for dep_record in dependent_records:
                        # Processar o registro de dependência recursivamente
                        process_record_with_dependencies(sf_sandbox, sf_dev, dep_successor, dep_record, direct_dependencies)
                        # Verificar se o registro de dependência já existe no Salesforce de destino
                        existing_records = selectObject(sf_dev, dep_successor, fields, where_clause=f" WHERE Name = '{dep_record['Name']}' ")
                        if existing_records:
                            print(f"Registro de dependência já existe: {existing_records}")
                            new_id = existing_records[0]['Id']
                        else:
                            # Inserir o registro de dependência no Salesforce de destino
                            dep_record['Id'] = None

                            result = try_insert_record(sf_dev, dep_successor, dep_record)
                            new_id = result['id']
                        # Atualizar o campo de dependência no registro principal com o novo ID
                        record[field] = new_id
    
    # Inserir o registro principal no Salesforce de destino
    try:
        record['Id'] = None

        result = try_insert_record(sf_dev, object_name, record)
        return result
    except SalesforceMalformedRequest as e:
        print(f"Erro ao inserir registro: {e}")
        return None

def remove_cycles(graph):
    """Remove ciclos do grafo."""
    try:
        while True:
            cycle = nx.find_cycle(graph, orientation='original')
            if not cycle:
                break
            graph.remove_edge(cycle[0][0], cycle[0][1])
    except nx.NetworkXNoCycle:
        pass

def find_insertion_order(graph):
    """Determina a ordem de inserção dos objetos com base na ordenação topológica."""
    remove_cycles(graph)
    if not nx.is_directed_acyclic_graph(graph):
        raise ValueError("Dependências circulares detectadas no grafo!")
    return list(nx.topological_sort(graph))

def visualize_graph(graph, filename="./path.png"):
    """Desenha e salva o grafo."""
    import matplotlib.pyplot as plt
    plt.figure(figsize=(10, 6))
    pos = nx.spring_layout(graph)
    edge_labels = {(u, v): data['field'] for u, v, key, data in graph.edges(data=True)}
    nx.draw(graph, pos, with_labels=True, node_color="lightblue", edge_color="gray", node_size=3000, font_size=10)
    nx.draw_networkx_edge_labels(graph, pos, edge_labels=edge_labels)
    plt.savefig(filename)
    plt.show()

def try_insert_record(sf, object_name, record, max_attempts=3):
    """
    Tenta inserir um registro no Salesforce até um máximo de tentativas.
    Se ocorrer um erro de campo, remove o campo problemático e tenta novamente.
    """
    attempt = 0
    problematic_fields = set()

    while attempt < max_attempts:
        try:
            record = {k: v for k, v in record.items() if k not in problematic_fields}

            result = sf.__getattr__(object_name).create(record)
            if result['success']:
                print(Fore.GREEN + f"Registro inserido com sucesso: {result['id']}")
                print(Fore.MAGENTA + f"Dados inseridos: {record}")  # Novo print colorido
                return result;  # Sair do loop se a inserção for bem-sucedida
            else:
                print(Fore.RED + f"Erro ao inserir registro: {result['errors']}")
                # Adicionar campos problemáticos à lista
                for error in result['errors']:
                    if 'fields' in error:
                        problematic_fields.update(error['fields'])
            return result
        except SalesforceMalformedRequest as e:
            print(Fore.RED + f"Erro ao processar registro: {str(e)}")
            # Extrair campos problemáticos da mensagem de erro
            if 'Unable to create/update fields:' in str(e):
                error_fields = str(e).split('Unable to create/update fields:')[1].split('.')[0].strip().split(', ')
                problematic_fields.update(error_fields)
            elif 'INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY' in str(e):
                print(Fore.YELLOW + "Erro de permissão detectado. Ignorando este registro.")
                break  # Sair do loop, pois não podemos inserir este registro devido a restrições de permissão
            elif 'DUPLICATE_VALUE' in str(e):
                print(Fore.YELLOW + "Valor duplicado detectado. Ignorando este registro.")
                return selectObject(sf, object_name, ['Id'], where_clause=f" WHERE Name = '{record['Name']}' ")[0]
        except SalesforceError as e:
            print(Fore.RED + f"Erro do Salesforce: {str(e)}")
            if 'INSUFFICIENT_ACCESS_OR_READONLY' in str(e):
                print(Fore.YELLOW + "Erro de permissão detectado. Ignorando este registro.")
                break  # Sair do loop, pois não podemos inserir este registro devido a restrições de permissão
            else:
                print("Erro desconhecido, não há campos para remover.")
                break
        attempt += 1
    raise Exception(f"Falha ao inserir o registro após {max_attempts} tentativas.")