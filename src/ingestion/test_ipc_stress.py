import sys
import os
import json
import tempfile
import time

# Adicionar root ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

def test_large_ipc_payload():
    print("--- Iniciando Teste de Estresse IPC (Payload Grande) ---")
    
    # 1. Simular um mapeamento GIGANTE (ex: 500 colunas)
    large_mapping = {
        "table_main": "LFCES004",
        "columns_main": {f"TARGET_COL_{i}": f"SOURCE_COL_{i}" for i in range(500)}
    }
    
    payload = {
        "layout_id": "11.1",
        "dsn": "C:/fake/path.gdb",
        "user": "SYSDBA",
        "password": "masterkey",
        "output_db": "C:/fake/output.db",
        "mapping": large_mapping,
        "competencia": "202405"
    }

    # 2. Salvar e medir tamanho
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as tf:
        json.dump(payload, tf)
        temp_path = tf.name
    
    file_size_kb = os.path.getsize(temp_path) / 1024
    print(f"[OK] Arquivo temporário criado: {temp_path} ({file_size_kb:.2f} KB)")
    
    # 3. Validar Leitura (Simulando o Worker 32-bit)
    try:
        with open(temp_path, 'r', encoding='utf-8') as f:
            data_read = json.load(f)
            
        if len(data_read['mapping']['columns_main']) == 500:
            print("[OK] Integridade do payload mantida após leitura.")
        else:
            print("[!] Falha na integridade do payload.")
            
    except Exception as e:
        print(f"[!] Erro ao ler payload gigante: {e}")
        
    finally:
        # 4. Testar Limpeza
        os.remove(temp_path)
        if not os.path.exists(temp_path):
            print("[OK] Arquivo temporário deletado com sucesso (Higiene IPC).")
        else:
            print("[!] Falha ao deletar arquivo temporário.")

if __name__ == "__main__":
    test_large_ipc_payload()
