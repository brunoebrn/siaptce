import streamlit as st
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
from xml.dom import minidom
from src.ingestion.firebird_engine import HealthDataIngestor
import os
from src.ui.components import render_readonly_mapping, render_data_preview, render_xml_button

def render_layout_11_8():
    st.title("Layout 11.8 - Autorização Internação Hospitalar")
    st.markdown("Autorizações de Internação Hospitalar (Fonte: SIH).")
    
    sih_path = st.session_state.db_paths.get('SIH')
    if not sih_path:
        st.error("Caminho do Banco SIH não configurado. Verifique na aba Setup.")
        return

    # 1. Load Schema (Cached)
    if 'sih_schema' not in st.session_state:
        with st.spinner("Lendo Schema do SIH..."):
            success, result = HealthDataIngestor.get_schema(sih_path)
            if success:
                st.session_state['sih_schema'] = result
            else:
                st.error(f"Erro ao ler schema: {result}")
                return

    schema = st.session_state['sih_schema']
    
    if 'mapping_11_8' not in st.session_state:
        st.session_state['mapping_11_8'] = {}
        
    st.session_state['mapping_11_8']['table_main'] = "TB_HAIH"
    
    st.info("ℹ️ **Origem**: Tabela **TB_HAIH** do banco SIH.")
    st.caption("Filtro Automático: Competência (AH_CMPT) igual ao Ano/Mês selecionados.")
    
    st.divider()
    
    # 3. Column Mapping (Read-Only)
    mapping_data = {
         'defaults': {
            "CNES": "AH_CNES",
            "NumeroAIH": "AH_NUM_AIH",
            "Identificacao": "AH_IDENT",
            "EspecialidadeLeito": "AH_ESPECIALIDADE",
            "ModalidadeInternacao": "AH_MODALIDADE_INTERNACAO",
            "AIHAnterior": "AH_NUM_AIH_ANT",
            "DataEmissao": "AH_DT_EMISSAO",
            "DataInternacao": "AH_DT_INTERNACAO",
            "DataSaida": "AH_DT_SAIDA",
            "ProcedimentoSolicitado": "AH_PROC_SOLICITADO",
            "CaraterInternacao": "AH_CAR_INTERNACAO",
            "MotivoSaida": "AH_MOT_SAIDA",
            "CNSSolicitante": "AH_MED_SOL_DOC",
            "CNSResponsavel": "AH_MED_RESP_DOC",
            "CNSAutorizador": "AH_AUTORIZADOR_DOC",
            "DiagnosticoPrincipal": "AH_DIAG_PRI",
            "CNSPaciente": "AH_PACIENTE_NUMERO_CNS"
         }
    }
    
    if 'columns_main' not in st.session_state['mapping_11_8']:
         st.session_state['mapping_11_8']['columns_main'] = {}
         
    st.session_state['mapping_11_8']['columns_main'] = mapping_data['defaults']
    render_readonly_mapping(st.session_state['mapping_11_8'])
    
    st.divider()
    
    col_action, col_status = st.columns([1, 2])
    
    with col_action:
        if st.button("🔄 Validar e Converter", type="primary", use_container_width=True, key='btn_11_8_run'):
             
             exercicio = st.session_state.get('global_exercicio', '2024')
             mes = st.session_state.get('global_mes', '01')
             
             with st.spinner(f"Realizando ETL 11.8 (SIH) para Competência {exercicio}{mes}..."):
                 success, result = HealthDataIngestor.generate_layout_11_8(
                     sih_path, 
                     st.session_state['mapping_11_8'],
                     exercicio,
                     mes
                 )
                 if success:
                     st.success("Sucesso!")
                     st.session_state['layout_11_8_db'] = result
                     st.rerun()
                 else:
                     st.error(f"Erro: {result}")

    # 4. Preview & XML
    if 'layout_11_8_db' in st.session_state:
        db_path = st.session_state['layout_11_8_db']
        df = render_data_preview(db_path, "layout_11_8")
        render_xml_button(df, "11.8")
