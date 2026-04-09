import subprocess
import json
import os
import sys
import pandas as pd
from src.utils.logger import setup_logger

logger = setup_logger("FirebirdEngine")

class HealthDataIngestor:
    """
    Classe responsável por gerenciar conexões com bancos de dados de saúde legados.
    Utiliza subprocessos 32-bit para compatibilidade com drivers Firebird antigos.
    """
    
    @staticmethod
    def _get_worker_executable():
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        worker_path = os.path.join(base_dir, 'python_worker', 'python.exe')
        return worker_path if os.path.exists(worker_path) else sys.executable

    @staticmethod
    def _run_32bit_worker(script_name: str, args: list) -> tuple[bool, dict]:
        """Auxiliar genérico para rodar workers utilitários 32-bit com captura de erro robusta."""
        worker_script = os.path.join(os.path.dirname(__file__), script_name)
        cmd = [HealthDataIngestor._get_worker_executable(), worker_script] + args
        
        try:
            # check=True levantará CalledProcessError se o status != 0
            proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
            output = proc.stdout.strip()
            
            lines = output.split('\n')
            last_line = lines[-1] if lines else ""
            
            data = json.loads(last_line)
            if data.get("success"):
                return True, data
            return False, data.get("error", "Erro desconhecido no worker.")
        except subprocess.CalledProcessError as e:
            # Captura o traceback real do worker 32-bit
            error_msg = e.stderr.strip() or e.stdout.strip() or str(e)
            logger.error(f"Worker {script_name} crashou: {error_msg}")
            return False, f"Erro no Worker 32-bit: {error_msg}"
        except Exception as e:
            return False, f"Falha na execução do worker {script_name}: {str(e)}"

    @staticmethod
    def check_connection(path: str, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, str]:
        """Valida a conexão com o banco de dados."""
        if not path: return False, "Caminho vazio."
        path = path.strip().strip('"')
        if not os.path.exists(path): return False, f"Arquivo não encontrado: {path}"
        
        success, result = HealthDataIngestor._run_32bit_worker('validate_conn.py', ["--path", path, "--user", user, "--password", password])
        return success, "Conectado com sucesso." if success else result

    @staticmethod
    def get_schema(path: str, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, dict]:
        """Obtém o schema (Tabelas e Colunas) do banco."""
        success, result = HealthDataIngestor._run_32bit_worker('worker_schema.py', ["--dsn", path.strip().strip('"'), "--user", user, "--password", password])
        return success, result.get("schema") if success else result

    @staticmethod
    def get_table_preview(path: str, table_name: str, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, object]:
        """Busca as primeiras 50 linhas de uma tabela para visualização."""
        success, result = HealthDataIngestor._run_32bit_worker('worker_query.py', ["--dsn", path.strip().strip('"'), "--user", user, "--password", password, "--table", table_name])
        if success:
            return True, pd.DataFrame(result.get("data"))
        return False, result

    @staticmethod
    def generate_layout(layout_id: str, path: str, mapping: dict, competencia: str = None, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, str]:
        """Executa o pipeline ETL genérico para um layout."""
        import tempfile
        if not path or not os.path.exists(path): return False, f"Caminho inválido: {path}"
            
        worker_script = os.path.join(os.path.dirname(__file__), 'generic_worker.py')
        layout_slug = layout_id.replace('.', '_')
        output_db = os.path.abspath(os.path.join(os.path.dirname(__file__), f'../../data/sqlite/layout_{layout_slug}.db'))
        os.makedirs(os.path.dirname(output_db), exist_ok=True)
        
        # IPC Seguro via Arquivo Temporário
        config_payload = {
            "layout_id": layout_id, "dsn": path.strip().strip('"'), "user": user, "password": password,
            "output_db": output_db, "mapping": mapping, "competencia": competencia
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as tf:
            json.dump(config_payload, tf)
            temp_config_path = tf.name

        cmd = [HealthDataIngestor._get_worker_executable(), worker_script, "--config", temp_config_path]
        
        try:
            logger.info(f"Gerando Layout {layout_id} via IPC Seguro")
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            if os.path.exists(temp_config_path): os.remove(temp_config_path)
            return True, output_db
        except subprocess.CalledProcessError as e:
            if os.path.exists(temp_config_path): os.remove(temp_config_path)
            logger.error(f"Worker {layout_id} falhou: {e.stderr}")
            return False, f"Erro no ETL: {e.stderr}"
        except Exception as e:
            if os.path.exists(temp_config_path): os.remove(temp_config_path)
            return False, f"Erro: {str(e)}"

    # Aliases para manter compatibilidade com a UI atual
    @staticmethod
    def generate_layout_11_1(p, m, u='SYSDBA', pw='masterkey'): return HealthDataIngestor.generate_layout("11.1", p, m, user=u, password=pw)
    @staticmethod
    def generate_layout_11_2(p, m, u='SYSDBA', pw='masterkey'): return HealthDataIngestor.generate_layout("11.2", p, m, user=u, password=pw)
    @staticmethod
    def generate_layout_11_3(p, m, u='SYSDBA', pw='masterkey'): return HealthDataIngestor.generate_layout("11.3", p, m, user=u, password=pw)
    @staticmethod
    def generate_layout_11_4(p, m, u='SYSDBA', pw='masterkey'): return HealthDataIngestor.generate_layout("11.4", p, m, user=u, password=pw)
    @staticmethod
    def generate_layout_11_5(p, m, y, mo, u='SYSDBA', pw='masterkey'): return HealthDataIngestor.generate_layout("11.5", p, m, competencia=f"{y}{mo}", user=u, password=pw)
    @staticmethod
    def generate_layout_11_8(p, m, y, mo, u='SYSDBA', pw='masterkey'): return HealthDataIngestor.generate_layout("11.8", p, m, competencia=f"{y}{mo}", user=u, password=pw)
