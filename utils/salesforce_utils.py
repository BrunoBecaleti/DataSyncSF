# utils/salesforce_utils.py

from simple_salesforce import Salesforce

# Função para autenticar no Salesforce com a opção de especificar o domínio (ex.: 'login' para produção, 'test' para sandbox)
def authenticate_salesforce(username, password, security_token, domain='login'):
    # O argumento 'domain' especifica se estamos usando um ambiente de produção ('login') ou sandbox ('test')
    sf = Salesforce(username=username, password=password, security_token=security_token, domain=domain)
    return sf

# Função para buscar todos os campos de um objeto dinamicamente
def get_object_fields(sf, object_name):
    metadata = sf.__getattr__(object_name).describe()
    fields_info = {field['name']: field['externalId'] for field in metadata['fields']}
    return fields_info

# Função para estimar o tamanho dos dados em MB
def estimate_data_size(records):
    # Estimar o tamanho de cada registro em 3 KB
    estimated_size_per_record_kb = 3  # Assumido como uma média
    total_records = len(records)
    total_size_kb = total_records * estimated_size_per_record_kb
    total_size_mb = total_size_kb / 1024  # Converter para MB
    return total_size_mb

# Função para encontrar todas as dependências de um objeto
def find_all_dependencies(sf, object_name):
    # Exemplo simplificado de como buscar dependências de um objeto
    metadata = sf.__getattr__(object_name).describe()
    dependencies = []
    
    # Verifica se algum campo é uma referência a outro objeto (lookup, master-detail)
    for field in metadata['fields']:
        if field['type'] in ['reference', 'lookup']:
            related_object = field['referenceTo'][0]
            dependencies.append(related_object)
    
    return dependencies


def get_external_id_field(sf, object_name):
    """
    Retorna o campo de External ID para um objeto.
    """
    # Obtém a descrição do objeto
    object_description = sf.__getattr__(object_name).describe()
    # Busca por campos que têm a propriedade 'externalId' definida como True
    external_id_fields = [field['name'] for field in object_description['fields'] if field.get('externalId')]
    
    # Retorna o primeiro campo de External ID encontrado, ou None se não houver
    return external_id_fields[0] if external_id_fields else None

def get_reference_fields(sf, object_name):
    metadata = sf.__getattr__(object_name).describe()
    reference_fields = {field['name']: field['referenceTo'] for field in metadata['fields'] if field['type'] == 'reference'}
    return reference_fields