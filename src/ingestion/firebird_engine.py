import subprocess
import json
import os
import sys
import pandas as pd

class HealthDataIngestor:
    """
    Class responsible for validating and managing connections to Health databases (CNES, FPO, SIH, SIA).
    Uses a 32-bit subprocess to ensure compatibility with legacy Firebird DLLs.
    """
    
    @staticmethod
    def check_connection(path: str, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, str]:
        """
        Attempts to connect to the Firebird database at the given path.
        Returns (True, "OK") if successful, (False, ErrorMessage) otherwise.
        """
        if not path:
             return False, "Caminho vazio."
             
        # Normalize path
        path = path.strip().strip('"')
        
        if not os.path.exists(path):
            return False, f"Arquivo não encontrado: {path}"
            
        # Defines the worker script path
        worker_script = os.path.join(os.path.dirname(__file__), 'validate_conn.py')
        
        # Command to run in 32-bit environment
        cmd = [
            "py", "-3.11-32", worker_script,
            "--path", path,
            "--user", user,
            "--password", password
        ]
        
        try:
            # Capture stdout to parse JSON result
            proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
            output = proc.stdout.strip()
            
            # Try to find JSON in the output (ignoring potential print noise)
            # We look for the last line that looks like JSON
            lines = output.strip().split('\n')
            last_line = lines[-1] if lines else ""
            
            try:
                data = json.loads(last_line)
                if data.get("success"):
                    return True, "Conectado com sucesso."
                else:
                    return False, data.get("error", "Erro desconhecido.")
            except json.JSONDecodeError:
                # If we catch a glimpse of the error in stdout/stderr
                return False, f"Erro ao decodificar resposta do worker: {output} | {proc.stderr}"
                
        except subprocess.CalledProcessError as e:
            return False, f"Falha na execução do processo 32-bit: {e.stderr}"
        except Exception as e:
            return False, f"Erro inesperado: {str(e)}"

    @staticmethod
    def get_schema(path: str, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, dict]:
        """
        Retrieves the schema (Tables and Columns) from the database via 32-bit worker.
        Returns (True, SchemaDict) or (False, ErrorMsg).
        """
        if not path or not os.path.exists(path):
            return False, "Caminho inválido."
            
        worker_script = os.path.join(os.path.dirname(__file__), 'worker_schema.py')
        
        cmd = [
            "py", "-3.11-32", worker_script,
            "--dsn", path.strip().strip('"'),
            "--user", user,
            "--password", password
        ]
        
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
            output = proc.stdout.strip()
            
            # Robust parsing (last line)
            lines = output.strip().split('\n')
            last_line = lines[-1] if lines else ""
            
            data = json.loads(last_line)
            if data.get("success"):
                return True, data.get("schema")
            else:
                return False, data.get("error", "Erro desconhecido ao ler schema.")
                
        except Exception as e:
            return False, f"Erro ao buscar schema: {e}"

    @staticmethod
    def generate_layout_11_1(path: str, mapping: dict, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, str]:
        """
        Executes the ETL for Layout 11.1 using the 32-bit worker with DYNAMIC MAPPING.
        Mapping format: {"table": "NAME", "columns": {"Target": "Source"}}
        """
        if not path or not os.path.exists(path):
            return False, "Caminho do CNES inválido."
            
        worker_script = os.path.join(os.path.dirname(__file__), 'worker_11_1.py')
        output_db = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/sqlite/layout_11_1.db'))
        
        # Ensure output dir exists
        os.makedirs(os.path.dirname(output_db), exist_ok=True)
        
        # Serialize mapping to JSON string to pass as arg
        mapping_json = json.dumps(mapping)
        
        cmd = [
            "py", "-3.11-32", worker_script,
            "--dsn", path.strip().strip('"'),
            "--user", user,
            "--password", password,
            "--output", output_db,
            "--mapping", mapping_json
        ]
        
        try:
            # We check=True, so it raises on 1 exit code
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            return True, output_db
        except subprocess.CalledProcessError as e:
            return False, f"Erro no ETL: {e.stderr}"
        except Exception as e:
            return False, f"Erro: {str(e)}"

    @staticmethod
    def get_table_preview(path: str, table_name: str, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, object]:
        """
        Retrieves top 50 rows of a table via 32-bit worker.
        Returns (True, pd.DataFrame) or (False, ErrorMsg).
        """
        if not path or not os.path.exists(path):
            return False, "Caminho inválido."
            
        worker_script = os.path.join(os.path.dirname(__file__), 'worker_query.py')
        
        cmd = [
            "py", "-3.11-32", worker_script,
            "--dsn", path.strip().strip('"'),
            "--user", user,
            "--password", password,
            "--table", table_name
        ]
            
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
            output = proc.stdout.strip()
            
            # Robust parsing (last line)
            lines = output.strip().split('\n')
            last_line = lines[-1] if lines else ""
            
            data = json.loads(last_line)
            if data.get("success"):
                df = pd.DataFrame(data.get("data"))
                return True, df
            else:
                return False, data.get("error", "Erro desconhecido.")
                
        except Exception as e:
            return False, f"Erro ao buscar dados: {e}"

    @staticmethod
    def generate_layout_11_2(path: str, mapping: dict, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, str]:
        """
        Runs the ETL process for Layout 11.2 (VinculoProfissional from LFCES021/018/004) in a separate 32-bit process.
        """
        if not path or not os.path.exists(path):
            return False, "Caminho do CNES inválido."
            
        worker_script = os.path.join(os.path.dirname(__file__), 'worker_11_2.py')
        output_db = os.path.join(os.path.dirname(path), 'siap_data.db')
        
        os.makedirs(os.path.dirname(output_db), exist_ok=True)
        
        # Serialize mapping mapping dict to JSON for CLI arg
        mapping_json = json.dumps(mapping)
        
        # Call the worker script using the same Python interpreter (presumably 32-bit)
        # or relying on system 'py -3.11-32' if configured.
        # Ideally, we use sys.executable if we are already in the 32-bit env, 
        # but since the Orchestrator might be 64-bit, we force the 32-bit launcher usually.
        # But here we assume the same environment pattern as check_connection for consistency.
        
        python_exe = "py -3.11-32" # Transformation requires fdb (32bit)
        
        cmd = [
            'py', '-3.11-32', worker_script,
            '--dsn', path.strip().strip('"'),
            '--user', user,
            '--password', password,
            '--output', output_db,
            '--mapping', mapping_json
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print(result.stdout)
            return True, output_db
        except subprocess.CalledProcessError as e:
            return False, f"Erro no Worker 11.2: {e.stderr}"
        except Exception as e:
            return False, f"Erro: {str(e)}"

    @staticmethod
    def generate_layout_11_3(path: str, mapping: dict, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, str]:
        """
        Runs the ETL process for Layout 11.3 (EstabelecimentoLeito from LFCES002/004) in a separate 32-bit process.
        """
        worker_script = os.path.join(os.path.dirname(__file__), 'worker_11_3.py')
        output_db = os.path.join(os.path.dirname(path), 'siap_data.db')
        mapping_json = json.dumps(mapping)
        
        cmd = [
            'py', '-3.11-32', worker_script,
            '--dsn', path.strip().strip('"'),
            '--user', user,
            '--password', password,
            '--output', output_db,
            '--mapping', mapping_json
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print(result.stdout)
            return True, output_db
        except subprocess.CalledProcessError as e:
            return False, f"Erro no Worker 11.3: {e.stderr}"
        except Exception as e:
            return False, f"Erro: {str(e)}"
    @staticmethod
    def generate_layout_11_4(path: str, mapping: dict, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, str]:
        """
        Runs the ETL process for Layout 11.4 (Equipamentos from LFCES020/004) in a separate 32-bit process.
        """
        worker_script = os.path.join(os.path.dirname(__file__), 'worker_11_4.py')
        output_db = os.path.join(os.path.dirname(path), 'siap_data.db')
        mapping_json = json.dumps(mapping)
        
        cmd = [
            'py', '-3.11-32', worker_script,
            '--dsn', path.strip().strip('"'),
            '--user', user,
            '--password', password,
            '--output', output_db,
            '--mapping', mapping_json
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print(result.stdout)
            return True, output_db
        except subprocess.CalledProcessError as e:
            return False, f"Erro no Worker 11.4: {e.stderr}"
        except Exception as e:
            return False, f"Erro: {str(e)}"
    @staticmethod
    def generate_layout_11_5(path: str, mapping: dict, year: str, month: str, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, str]:
        """
        Runs the ETL process for Layout 11.5 (FPO - FichaProgramacaoOrcamentaria)
        Filters by Competence (AAAAMM)
        """
        worker_script = os.path.join(os.path.dirname(__file__), 'worker_11_5.py')
        output_db = os.path.join(os.path.dirname(path), 'siap_data.db')
        mapping_json = json.dumps(mapping)
        
        competencia = f"{year}{month}"
        
        cmd = [
            'py', '-3.11-32', worker_script,
            '--dsn', path.strip().strip('"'),
            '--user', user,
            '--password', password,
            '--output', output_db,
            '--mapping', mapping_json,
            '--competencia', competencia
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print(result.stdout)
            return True, output_db
        except subprocess.CalledProcessError as e:
            return False, f"Erro no Worker 11.5: {e.stderr}"
        except Exception as e:
            return False, f"Erro: {str(e)}"

    @staticmethod
    def generate_layout_11_8(path: str, mapping: dict, year: str, month: str, user: str = 'SYSDBA', password: str = 'masterkey') -> tuple[bool, str]:
        """
        Runs the ETL process for Layout 11.8 (SIH - AutorizacaoInternacaoHospitalar)
        Filters by Competence (AAAAMM)
        """
        worker_script = os.path.join(os.path.dirname(__file__), 'worker_11_8.py')
        output_db = os.path.join(os.path.dirname(path), 'siap_data.db')
        mapping_json = json.dumps(mapping)
        
        competencia = f"{year}{month}"
        
        cmd = [
            'py', '-3.11-32', worker_script,
            '--dsn', path.strip().strip('"'),
            '--user', user,
            '--password', password,
            '--output', output_db,
            '--mapping', mapping_json,
            '--competencia', competencia
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print(result.stdout)
            return True, output_db
        except subprocess.CalledProcessError as e:
            return False, f"Erro no Worker 11.8: {e.stderr}"
        except Exception as e:
            return False, f"Erro: {str(e)}"
