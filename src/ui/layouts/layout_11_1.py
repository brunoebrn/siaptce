import streamlit as st
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
from xml.dom import minidom
from src.ingestion.firebird_engine import HealthDataIngestor
import os
from src.ui.components import render_readonly_mapping, render_data_preview, render_xml_button

def render_layout_11_1():
    st.title("Layout 11.1 - Estabelecimentos de Saúde")
    st.markdown("Mapeamento Dinâmico: Estabelecimentos de Saúde.")
    
    cnes_path = st.session_state.db_paths.get('CNES')
    if not cnes_path:
        st.error("Caminho do Banco CNES não configurado.")
        return

    # 1. Load Schema (Cached)
    if 'cnes_schema' not in st.session_state:
        with st.spinner("Lendo Schema..."):
            success, result = HealthDataIngestor.get_schema(cnes_path)
            if success:
                st.session_state['cnes_schema'] = result
            else:
                st.error(f"Erro: {result}")
                return

    schema = st.session_state['cnes_schema']
    
    if 'mapping_11_1' not in st.session_state:
        st.session_state['mapping_11_1'] = {}
    
    # Default Table Preference
    st.session_state['mapping_11_1']['table_main'] = "LFCES004"
    
    st.divider()
    
    # 3. Column Mapping (Read-Only)
    mapping_data = {
         'defaults': {
            "CNES": "CNES",
            "CNPJ": "CNPJ_MANT",
            "NomeFantasia": "NOME_FANTA",
            "RazaoSocial": "R_SOCIAL",
            "Endereco": "LOGRADOURO",
            "CEP": "COD_CEP",
            "CPFDiretor": "CPFDIRETORCLINICO",
            "TipoEstabelecimentoSaude": "TP_UNID_ID",
            "AtividadePrincipal": "CO_ATIVIDADE_PRINCIPAL",
            "SistemaSUS": "" # Dummy or mapped (Force 1 in ETL)
         }
    }
    
    if 'columns_main' not in st.session_state['mapping_11_1']:
         st.session_state['mapping_11_1']['columns_main'] = {}
         
    st.session_state['mapping_11_1']['columns_main'] = mapping_data['defaults']
    render_readonly_mapping(st.session_state['mapping_11_1'])
    
    st.divider()
    
    col_action, col_status = st.columns([1, 2])
    
    with col_action:
        if st.button("🔄 Validar e Converter", type="primary", use_container_width=True, key='btn_11_1_run'):
             with st.spinner("Realizando ETL 11.1..."):
                 success, result = HealthDataIngestor.generate_layout_11_1(cnes_path, st.session_state['mapping_11_1'])
                 if success:
                     st.success("Sucesso!")
                     st.session_state['layout_11_1_db'] = result
                     st.rerun()
                 else:
                     st.error(f"Erro: {result}")

    # 4. Preview & XML
    if 'layout_11_1_db' in st.session_state:
        db_path = st.session_state['layout_11_1_db']
        df = render_data_preview(db_path, "layout_11_1")
        render_xml_button(df, "11.1")
