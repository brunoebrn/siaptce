import sys
import os
import time

# Adicionar root ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.ingestion.connector import FirebirdConnector

def simulate_crash_with_lock(dsn):
    print(f"--- Iniciando Teste de Resource Locking em {dsn} ---")
    
    try:
        # Usando o Context Manager que implementamos (Ponto 4 do Plano)
        with FirebirdConnector(dsn, 'SYSDBA', 'masterkey') as conn:
            print("[1] Conectado ao banco.")
            
            # Simulando uma query que "demora" ou um processamento pesado
            print("[2] Simulando processamento... (3 segundos)")
            time.sleep(3)
            
            # Forçando um erro crítico no meio da transação
            print("[3] Forçando erro catastrófico (Simulação de Crash)...")
            raise RuntimeError("CRASH SIMULADO NO WORKER")
            
    except RuntimeError as e:
        print(f"[4] Capturada exceção esperada: {e}")
    except Exception as e:
        # Se o banco não existir no ambiente de teste, ele cairá aqui
        print(f"[!] Erro de conexão (esperado se banco não existir): {e}")
    finally:
        print("--- Fim do Teste ---")

if __name__ == "__main__":
    # Usamos um path fictício ou real para testar apenas o fluxo do código
    dummy_path = "C:/temp/lock_test.gdb"
    simulate_crash_with_lock(dummy_path)
