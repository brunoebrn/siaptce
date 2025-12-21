import streamlit as st
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
from xml.dom import minidom
from src.ingestion.firebird_engine import HealthDataIngestor
import os
from src.ui.components import render_readonly_mapping, render_data_preview, render_xml_button

def render_layout_11_5():
    st.title("Layout 11.5 - Ficha Programação Orçamentária")
    st.markdown("Programação Orçamentária dos Estabelecimentos (Fonte: FPO).")
    
    fpo_path = st.session_state.db_paths.get('FPO')
    if not fpo_path:
        st.error("Caminho do Banco FPO não configurado. Verifique na aba Setup.")
        return

    # 1. Load Schema (Cached)
    # Using 'fpo_schema' key
    if 'fpo_schema' not in st.session_state:
        with st.spinner("Lendo Schema do FPO..."):
            success, result = HealthDataIngestor.get_schema(fpo_path)
            if success:
                st.session_state['fpo_schema'] = result
            else:
                st.error(f"Erro ao ler schema: {result}")
                return

    schema = st.session_state['fpo_schema']
    
    if 'mapping_11_5' not in st.session_state:
        st.session_state['mapping_11_5'] = {}
        
    st.session_state['mapping_11_5']['table_main'] = "S_IPU"
    
    st.info("ℹ️ **Origem**: Tabela **S_IPU** do banco FPO.")
    
    st.divider()
    
    # 3. Column Mapping (Read-Only)
    mapping_data = {
         'defaults': {
            "CNES": "IPU_UID",        
            "Procedimento": "IPU_PA",   
            "Financiamento": "IPU_TPFIN", 
            "Quantidade": "IPU_QT_O",
            "ValorUnitario": "IPU_VU_O",
            "ValorTotal": "IPU_VL_O"
         }
    }
    
    if 'columns_main' not in st.session_state['mapping_11_5']:
         st.session_state['mapping_11_5']['columns_main'] = {}
         
    st.session_state['mapping_11_5']['columns_main'] = mapping_data['defaults']
    render_readonly_mapping(st.session_state['mapping_11_5'])
    
    st.divider()
    
    col_action, col_status = st.columns([1, 2])
    
    with col_action:
        if st.button("🔄 Validar e Converter", type="primary", use_container_width=True, key='btn_11_5_run'):
             
             exercicio = st.session_state.get('global_exercicio', '2024')
             mes = st.session_state.get('global_mes', '01')
             
             with st.spinner(f"Realizando ETL 11.5 (FPO) para Competência {exercicio}{mes}..."):
                 success, result = HealthDataIngestor.generate_layout_11_5(
                     fpo_path, 
                     st.session_state['mapping_11_5'],
                     exercicio,
                     mes
                 )
                 if success:
                     st.success("Sucesso!")
                     st.session_state['layout_11_5_db'] = result
                     st.rerun()
                 else:
                     st.error(f"Erro: {result}")

    # 4. Preview & XML
    if 'layout_11_5_db' in st.session_state:
        db_path = st.session_state['layout_11_5_db']
        df = render_data_preview(db_path, "layout_11_5")
        render_xml_button(df, "11.5")
