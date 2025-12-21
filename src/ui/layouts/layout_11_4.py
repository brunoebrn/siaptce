import streamlit as st
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
from xml.dom import minidom
from src.ingestion.firebird_engine import HealthDataIngestor
import os

def render_layout_11_4():
    st.title("Layout 11.4 - Equipamentos")
    st.markdown("Cadastro de equipamentos existentes na unidade (em uso ou não).")
    
    cnes_path = st.session_state.db_paths.get('CNES')
    if not cnes_path:
        st.error("Configure o banco CNES na aba Setup.")
        return

    # 1. Load Schema
    if 'cnes_schema' not in st.session_state:
        with st.spinner("Lendo Schema do Banco..."):
            success, result = HealthDataIngestor.get_schema(cnes_path)
            if success:
                st.session_state['cnes_schema'] = result
            else:
                st.error(f"Erro ao ler schema: {result}")
                return

    schema = st.session_state['cnes_schema']
    
    # 2. Table Selection (Hidden/Fixed)
    if 'mapping_11_4' not in st.session_state:
        st.session_state['mapping_11_4'] = {}
        
    selected_table = "LFCES020" 
    st.session_state['mapping_11_4']['table_main'] = selected_table
    
    st.info("ℹ️ **Integração Automática Ativa**: Dados de **LFCES020** (Equipamentos) + **LFCES004** (Estabelecimentos).")
    
    # Schema Merging
    if selected_table not in schema:
        available_columns = [""] 
        st.error(f"Tabela base {selected_table} não encontrada no banco.")
    else:
        available_columns = [""] + schema.get(selected_table, [])
    
    extra_tables = ["LFCES004"]
    for t in extra_tables:
        if t in schema:
             available_columns.extend([c for c in schema[t] if c not in available_columns])
    
    available_columns = sorted(list(set(available_columns)))
    
    st.divider()
    
    # 3. Column Mapping (Read-Only)
    from src.ui.components import render_readonly_mapping, render_data_preview, render_xml_button
    
    # Defaults are used as the mapping since we are "display only"
    mapping_data = {
         'defaults': {
            "CNES": "CNES",        
            "CodigoEquipamento": "COD_EQUIP",   
            "TipoEquipamentoSaude": "CODTPEQUIP", 
            "Quantidade": "QTDE_EXIST",
            "QuantidadeUso": "QTDE_USO", 
            "DisponibilidadeSUS": "IND_SUS"
         }
    }
    
    # Update session state to match expected worker input format
    # The worker expects 'columns_main' key in the mapping dict
    st.session_state['mapping_11_4']['columns_main'] = mapping_data['defaults']
    
    render_readonly_mapping(st.session_state['mapping_11_4'])
    
    st.divider()
    
    col_action, col_status = st.columns([1, 2])
    
    with col_action:
        if st.button("🔄 Validar e Converter", type="primary", use_container_width=True, key='btn_11_4_run'):
             with st.spinner("Realizando ETL 11.4 (Integração)..."):
                 success, result = HealthDataIngestor.generate_layout_11_4(cnes_path, st.session_state['mapping_11_4'])
                 if success:
                     st.success("Sucesso!")
                     st.session_state['layout_11_4_db'] = result
                     st.rerun()
                 else:
                     st.error(f"Erro: {result}")

    # 4. Preview & XML
    if 'layout_11_4_db' in st.session_state:
        db_path = st.session_state['layout_11_4_db']
        df = render_data_preview(db_path, "layout_11_4")
        render_xml_button(df, "11.4")
