import streamlit as st
import sys
import os
import subprocess
import pandas as pd

# Adicionar root ao path para imports funcionarem
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.ingestion.connector import FirebirdConnector
from src.transformation.processor import DataProcessor
from src.serializer.xml_generator import XMLSerializer

def main():
    st.set_page_config(layout="wide")
    st.title("SIAP - Gerador de XML")
    
    st.sidebar.header("Configuração de Conexão")
    dsn = st.sidebar.text_input("Caminho do Banco (DSN)", value="localhost:C:/path/to/db.fdb")
    # Auto-clean input: remove quotes and whitespace common in Windows paths
    if dsn:
        dsn = dsn.strip().strip('"')
    user = st.sidebar.text_input("Usuário", value="SYSDBA")
    password = st.sidebar.text_input("Senha", value="masterkey", type="password")
    
    if st.button("Conectar e Listar Tabelas"):
        st.info("Iniciando processo Híbrido (UI 64-bit -> Ingestão 32-bit)...")
        
        # Caminho do worker 32-bit
        ingestion_worker = "src/ingestion/run_ingestion.py"
        output_file = os.path.abspath(os.path.join("data/sqlite", "ingestion_result.json"))
        
        # Comando para executar o script no ambiente 32-bit
        # Assumindo que 'py -3.11-32' está disponível
        cmd = [
            "py", "-3.11-32", ingestion_worker,
            "--dsn", dsn,
            "--user", user,
            "--password", password,
            "--output", output_file
        ]
        
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            st.success("Conexão (32-bit) OK!")
            # st.text(result.stdout)
            
            # Carregar resultado
            if os.path.exists(output_file):
                df_tables = pd.read_json(output_file)
                st.session_state['tables'] = df_tables['TableName'].tolist()
                st.session_state['dsn'] = dsn
                st.session_state['user'] = user
                st.session_state['password'] = password
                st.session_state['connected'] = True
            else:
                st.error("Arquivo de saída não encontrado.")
                
        except subprocess.CalledProcessError as e:
            st.error("Falha na conexão 32-bit:")
            st.code(e.stderr)
        except Exception as e:
            st.error(f"Erro inesperado: {e}")

    # Section for Interactive Exploration (Connection Persisted in Session)
    if st.session_state.get('connected'):
        st.divider()
        st.subheader("Explorador de Dados")
        
        table_list = st.session_state.get('tables', [])
        selected_table = st.selectbox("Selecione uma tabela para visualizar:", [""] + table_list)
        
        if selected_table:
            col1, col2 = st.columns([1, 1])
            
            with col1:
                if st.button(f"Carregar Preview (50 linhas)"):
                    with st.spinner(f"Lendo tabela {selected_table}..."):
                        # Call worker again specifically for this table
                        ingestion_worker = "src/ingestion/run_ingestion.py"
                        output_file_table = os.path.abspath(os.path.join("data/sqlite", "table_data.json"))
                        
                        cmd_table = [
                            "py", "-3.11-32", ingestion_worker,
                            "--dsn", st.session_state['dsn'],
                            "--user", st.session_state['user'],
                            "--password", st.session_state['password'],
                            "--output", output_file_table,
                            "--table", selected_table
                        ]
                        
                        try:
                            subprocess.run(cmd_table, check=True, capture_output=True, text=True)
                            if os.path.exists(output_file_table):
                                df_data = pd.read_json(output_file_table)
                                st.write(f"**Preview de {selected_table}:**")
                                st.dataframe(df_data, use_container_width=True)
                            else:
                                st.warning("Nenhum dado retornado.")
                        except subprocess.CalledProcessError as e:
                            st.error(f"Erro ao ler tabela {selected_table}:")
                            st.code(e.stderr)

            with col2:
                if st.button("Exportar CSV Completo"):
                    with st.spinner(f"Exportando TODOS os dados de {selected_table}... (Isso pode demorar)"):
                         ingestion_worker = "src/ingestion/run_ingestion.py"
                         output_file_full = os.path.abspath(os.path.join("data/sqlite", "full_table_export.json"))
                         
                         cmd_full = [
                            "py", "-3.11-32", ingestion_worker,
                            "--dsn", st.session_state['dsn'],
                            "--user", st.session_state['user'],
                            "--password", st.session_state['password'],
                            "--output", output_file_full,
                            "--table", selected_table,
                            "--full" # Flag for no limit
                        ]
                         
                         try:
                            subprocess.run(cmd_full, check=True, capture_output=True, text=True)
                            if os.path.exists(output_file_full):
                                df_full = pd.read_json(output_file_full)
                                csv = df_full.to_csv(index=False).encode('utf-8')
                                
                                st.download_button(
                                    label="📥 Baixar CSV Agora",
                                    data=csv,
                                    file_name=f"{selected_table}.csv",
                                    mime="text/csv",
                                )
                            else:
                                st.error("Falha na exportação.")
                         except subprocess.CalledProcessError as e:
                            st.error(f"Erro na exportação de {selected_table}:")
                            st.code(e.stderr)

if __name__ == "__main__":
    main()
