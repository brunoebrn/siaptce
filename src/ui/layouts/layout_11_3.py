import streamlit as st
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
from xml.dom import minidom
from src.ingestion.firebird_engine import HealthDataIngestor
import os
from src.ui.components import render_readonly_mapping, render_data_preview, render_xml_button

def render_layout_11_3():
    st.title("Layout 11.3 - EstabelecimentoLeito")
    st.markdown("Quantidade e tipos de leitos de cada Estabelecimento de Saúde.")
    
    cnes_path = st.session_state.db_paths.get('CNES')
    if not cnes_path:
        st.error("Configure o banco CNES na aba Setup.")
        return

    # 1. Load Schema (Cached)
    if 'cnes_schema' not in st.session_state:
        with st.spinner("Lendo Schema do Banco..."):
            success, result = HealthDataIngestor.get_schema(cnes_path)
            if success:
                st.session_state['cnes_schema'] = result
            else:
                st.error(f"Erro ao ler schema: {result}")
                return

    schema = st.session_state['cnes_schema']
    
    if 'mapping_11_3' not in st.session_state:
        st.session_state['mapping_11_3'] = {}
        
    st.session_state['mapping_11_3']['table_main'] = "LFCES002"
    
    st.info("ℹ️ **Integração Automática Ativa**: Este layout utiliza dados combinados das tabelas **LFCES002** (Leitos) e **LFCES004** (Estabelecimentos).")
    
    st.divider()
    
    # 3. Column Mapping (Read-Only)
    mapping_data = {
         'defaults': {
            "CNES": "CNES",        
            "CodigoLeito": "COD_LEITO",   
            "TipoLeito": "CODTPLEITO", 
            "Quantidade": "QTDE_EXIST",
            "QuantidadeSUS": "QTDE_SUS"
         }
    }
    
    if 'columns_main' not in st.session_state['mapping_11_3']:
         st.session_state['mapping_11_3']['columns_main'] = {}
         
    st.session_state['mapping_11_3']['columns_main'] = mapping_data['defaults']
    render_readonly_mapping(st.session_state['mapping_11_3'])
    
    st.divider()
    
    col_action, col_status = st.columns([1, 2])
    
    with col_action:
        if st.button("🔄 Validar e Converter", type="primary", use_container_width=True, key='btn_11_3_run'):
             with st.spinner("Realizando ETL 11.3 (Integração)..."):
                 success, result = HealthDataIngestor.generate_layout_11_3(cnes_path, st.session_state['mapping_11_3'])
                 if success:
                     st.success("Sucesso!")
                     st.session_state['layout_11_3_db'] = result
                     st.rerun()
                 else:
                     st.error(f"Erro: {result}")

    # 4. Preview & XML
    if 'layout_11_3_db' in st.session_state:
        db_path = st.session_state['layout_11_3_db']
        df = render_data_preview(db_path, "layout_11_3")
        render_xml_button(df, "11.3")
