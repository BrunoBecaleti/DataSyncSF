# utils/storage_utils.py

# Função para obter o armazenamento restante na org de destino
def get_storage_limits(sf):
    # A consulta da API 'limits' retorna vários limites, incluindo armazenamento
    limits = sf.limits()
    storage_used = limits['DataStorageMB']['Max'] - limits['DataStorageMB']['Remaining']
    storage_remaining = limits['DataStorageMB']['Remaining']
    print(f"Armazenamento utilizado: {storage_used} MB")
    print(f"Armazenamento restante: {storage_remaining} MB")
    return storage_remaining

# Função para estimar o tamanho dos dados (em MB)
def estimate_data_size(records):
    # Estimar o tamanho de cada registro em 3 KB
    estimated_size_per_record_kb = 3  # Assumido como uma média
    total_records = len(records)
    total_size_kb = total_records * estimated_size_per_record_kb
    total_size_mb = total_size_kb / 1024  # Converter para MB
    return total_size_mb

def get_storage_limits(sf):
    """Obtém o limite de armazenamento restante em MB."""
    # Esta é uma implementação simplificada. Você pode precisar ajustar conforme a API do Salesforce
    limits = sf.limits()
    return limits['DataStorageMB']['Remaining']

def estimate_data_size(records):
    """Estima o tamanho dos dados em MB."""
    # Esta é uma estimativa muito simplificada. Você pode precisar de uma implementação mais precisa
    return len(str(records)) / (1024 * 1024)
