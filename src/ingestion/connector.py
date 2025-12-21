import fdb
import os

class FirebirdConnector:
    def __init__(self, dsn, user, password, role=None, charset='WIN1252'):
        self.dsn = dsn
        self.user = user
        self.password = password
        self.role = role
        self.charset = charset
        self.connection = None

    def connect(self):
        # Determine strict library path
        drivers_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../drivers'))
        fb_lib = os.path.join(drivers_path, 'fbclient.dll')
        
        # FEATURE: Support Embedded Mode if user provided fbembed.dll
        # This is great for standalone file access without a server
        if os.path.exists(os.path.join(drivers_path, 'fbembed.dll')):
            fb_lib = os.path.join(drivers_path, 'fbembed.dll')
            print(f"Modo Embedded Detectado: Usando {fb_lib}")

        # Base arguments
        connect_args = {
            'user': self.user,
            'password': self.password,
            'role': self.role,
            'charset': self.charset,
            'fb_library_name': fb_lib
        }
        
        try:
            last_error = None
            # Strategy 1: Try TCP (Force localhost) - Best for permissions if Server is running
            try:
                print("Tentando conexão via TCP (localhost)...")
                tcp_args = connect_args.copy()
                if ':' not in self.dsn:
                     tcp_args['dsn'] = 'localhost:' + self.dsn
                else:
                     tcp_args['dsn'] = self.dsn
                
                self.connection = fdb.connect(**tcp_args)
                print("Conectado via TCP.")
                return
            except Exception as e_tcp:
                print(f"TCP falhou: {e_tcp}")
                last_error = e_tcp

            # Strategy 2: Try Local/Embedded (Direct Path)
            try:
                print("Tentando conexão Local/Embedded...")
                local_args = connect_args.copy()
                # Remove localhost prefix if it exists in input (unlikely per current logic but safe)
                raw_dsn = self.dsn.replace('localhost:', '')
                
                # Check for remote IP (e.g., 192.168.1.1:C:/db) BUT allow Windows Drive letters (C:\db)
                # If there's a colon, ensure it's the 2nd char (index 1) which implies a Drive Letter
                is_windows_drive = len(raw_dsn) > 1 and raw_dsn[1] == ':'
                
                if ':' in raw_dsn and not is_windows_drive:
                    raise Exception("DSN contém IP remoto, modo local ignorado.")
                
                local_args['database'] = raw_dsn 
                # Remove 'dsn' key if present from strict copy, fdb uses 'database' param for local
                if 'dsn' in local_args: del local_args['dsn'] 

                self.connection = fdb.connect(**local_args)
                print("Conectado via Modo Local.")
                return
            except Exception as e_local:
                print(f"Modo Local falhou: {e_local}")
                # Analyze -904 specifically
                if (last_error and "unavailable database" in str(last_error)) or "unavailable database" in str(e_local):
                   print("\nDIAGNÓSTICO ARQUIVO LOCAL:")
                   if os.path.exists(self.dsn):
                       print(f"- Arquivo '{self.dsn}' ENCONTRADO no disco.")
                       print("- Erro -904 geralmente indica PERMISSÃO ou USO EXCLUSIVO.")
                       print("  1. Se tiver um Firebird Server rodando, verifique se ele tem permissão na pasta.")
                       print("  2. Se deseja modo EMBEDDED (sem server), verifique se pegou a DLL 'fbembed.dll' renomeada.")
                   else:
                       print(f"- Arquivo '{self.dsn}' NÃO ENCONTRADO.")
                
                raise e_local
        except Exception as e:
            if "fb_interpret" in str(e):
                print("ERRO DE COMPATIBILIDADE: A 'fbclient.dll' fornecida (v1.5) é muito antiga para este driver.")
                print("SOLUÇÃO: Substitua o arquivo em '/drivers' pela 'fbclient.dll' do Firebird 2.5 (32-bit).")
                print("         Drivers mais novos conseguem conectar em bancos antigos (v1.5) sem problemas.")
                raise Exception("DLL do Firebird incompatível (Falta 'fb_interpret'). Use a DLL do FB 2.5.") from e
            
            print(f"Erro ao conectar ao Firebird: {e}")
            raise

    def execute_query(self, query):
        if not self.connection:
            raise Exception("Não conectado ao banco de dados.")
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            # Retrieve column names from cursor description
            columns = [desc[0] for desc in cursor.description]
            return cursor.fetchall(), columns
        finally:
            cursor.close()

    def close(self):
        if self.connection:
            self.connection.close()
            print("Conexão fechada.")
