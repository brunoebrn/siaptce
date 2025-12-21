import streamlit as st
import pandas as pd
import sys
import os

# Ensure imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.ingestion.firebird_engine import HealthDataIngestor

def render_db_explorer():
    st.header("🔍 Explorador de Banco de Dados")
    st.markdown("Visualize as tabelas e dados brutos dos bancos conectados.")
    
    st.sidebar.divider()
    
    # 1. Database Selection
    available_dbs = [name for name, path in st.session_state.db_paths.items() if path]
    if not available_dbs:
        st.error("Nenhum banco de dados conectado. Configure em 'Setup'.")
        return

    selected_db = st.sidebar.selectbox("Selecione o Banco de Dados", available_dbs)
    db_path = st.session_state.db_paths[selected_db]

    # 2. Schema Loading (Cached per DB)
    cache_key = f'schema_{selected_db}'
    if cache_key not in st.session_state:
        with st.spinner(f"Lendo Schema do {selected_db}..."):
            success, result = HealthDataIngestor.get_schema(db_path)
            if success:
                st.session_state[cache_key] = result
            else:
                st.error(f"Erro ao ler schema do {selected_db}: {result}")
                return

    schema = st.session_state[cache_key]
    tables = list(schema.keys())
    
    # Filter
    filter_text = st.sidebar.text_input("Filtrar Tabelas", "").upper()
    filtered_tables = [t for t in tables if filter_text in t]
    
    selected_table = st.sidebar.radio("Tabelas Disponíveis", filtered_tables)
    
    if selected_table:
        st.subheader(f"Tabela: {selected_table} ({selected_db})")
        
        # Columns Info
        cols = schema[selected_table]
        st.caption(f"Colunas ({len(cols)}): {', '.join(cols)}")
        
        # Preview Data
        if st.button("👁️ Visualizar Dados (Top 50)", key=f"btn_prev_{selected_db}_{selected_table}"):
            with st.spinner("Buscando dados..."):
                success, df = HealthDataIngestor.get_table_preview(db_path, selected_table)
                
                if success:
                    st.dataframe(df, use_container_width=True)
                else:
                    st.error(f"Erro ao buscar dados: {df}")
