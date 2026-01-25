import streamlit as st
import pandas as pd
import os
from src.ui.components import XML_COL_MAPS, generate_xml_string

def render_xlsx_converter():
    st.title("Conversor XLSX para XML")
    st.markdown("Converta arquivos XLSX editados diretamente para o formato XML do Tribunal.")
    st.info("Certifique-se de que os arquivos XLSX mantenham a estrutura de colunas original exportada pelo sistema.")
    
    st.divider()

    # Layout Inputs configuration
    layouts = [
        {'id': '11.1', 'label': 'Layout 11.1 - Estabelecimentos'},
        {'id': '11.2', 'label': 'Layout 11.2 - Vínculo Profissional'},
        {'id': '11.3', 'label': 'Layout 11.3 - Leitos'},
        {'id': '11.4', 'label': 'Layout 11.4 - Equipamentos'},
        {'id': '11.5', 'label': 'Layout 11.5 - Orçamento (FPO)'},
        {'id': '11.6', 'label': 'Layout 11.6 - Produção Ambulatorial', 'future': True},
        {'id': '11.7', 'label': 'Layout 11.7 - Apuração Ambulatorial', 'future': True},
        {'id': '11.8', 'label': 'Layout 11.8 - Internação (SIH)'}
    ]

    # Check which layouts are actually supported in XML_COL_MAPS
    supported_layouts = [l for l in layouts if l['id'] in XML_COL_MAPS]

    # Initialize State
    if 'xlsx_paths' not in st.session_state:
        st.session_state.xlsx_paths = {l['id']: '' for l in supported_layouts}
    
    if 'xlsx_status' not in st.session_state:
        st.session_state.xlsx_status = {l['id']: None for l in supported_layouts} # True/False/None
    
    if 'xlsx_dfs' not in st.session_state:
        st.session_state.xlsx_dfs = {} # Cache valid DFs

    if 'xlsx_errors' not in st.session_state:
        st.session_state.xlsx_errors = {}

    # Render Inputs
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Caminho dos Arquivos XLSX")
        for layout in supported_layouts:
            key = layout['id']
            st.session_state.xlsx_paths[key] = st.text_input(
                layout['label'], 
                value=st.session_state.xlsx_paths[key],
                placeholder=f"C:\\caminho\\para\\{layout['label'].split(' - ')[1]}.xlsx",
                key=f"input_xlsx_{key}"
            )

    with col2:
        st.subheader("Status")
        st.markdown("")
        st.markdown("")
        for layout in supported_layouts:
             key = layout['id']
             status = st.session_state.xlsx_status[key]
             if status is True:
                 st.success(f"{key}: Válido ✅")
             elif status is False:
                 st.error(f"{key}: Falha ❌")
                 if key in st.session_state.xlsx_errors:
                     st.caption(f"📝 {st.session_state.xlsx_errors[key]}")
             else:
                 st.info(f"{key}: Aguardando")

    st.divider()

    # Validar Button
    if st.button("Validar Arquivos", type="primary", use_container_width=True):
        validate_files(supported_layouts)
        st.rerun()

    # Export Area
    st.subheader("Exportação XML")
    
    at_least_one_valid = any(st.session_state.xlsx_status.values())
    
    if not at_least_one_valid:
        st.caption("Valide os arquivos para habilitar a exportação.")
    else:
        # Map IDs to Descriptive Names for UI Label
        name_map = {
            '11.1': 'EstabelecimentoSaude',
            '11.2': 'VinculoProfissionalSaude',
            '11.3': 'EstabelecimentoLeito',
            '11.4': 'EstabelecimentoEquipamento',
            '11.5': 'FichaProgramacaoOrcamentaria',
            '11.8': 'AutorizacaoInternacaoHospitalar'
        }

        # Render Export Buttons for Valid Files
        cols = st.columns(4)
        idx = 0
        for layout in supported_layouts:
            key = layout['id']
            if st.session_state.xlsx_status[key]:
                with cols[idx % 4]:
                     file_label = name_map.get(key, key)
                     st.info(f"XML: {key} - {file_label}")
                     df = st.session_state.xlsx_dfs.get(key)
                     render_xlsx_xml_export(df, key)
                     idx += 1


def validate_files(layouts):
    """
    Validates existence and structure of files.
    """
    # Reset errors
    st.session_state.xlsx_errors = {}
    
    for layout in layouts:
        key = layout['id']
        path_raw = st.session_state.xlsx_paths[key]
        
        if not path_raw:
            st.session_state.xlsx_status[key] = None
            continue
            
        path = path_raw.strip().strip('"').strip("'")
        
        # 1. Check Existence
        if not os.path.exists(path):
            st.session_state.xlsx_status[key] = False
            st.session_state.xlsx_errors[key] = f"Arquivo não encontrado: {path}"
            # st.error removed to avoid flash before rerun
            continue
            
        try:
            # 2. Read Excel
            # Try 'Dados' sheet first (standard export), else first sheet
            # FORCE ALL COLUMNS TO STRING to preserve leading zeros
            try:
                df = pd.read_excel(path, sheet_name='Dados', dtype=str)
            except:
                df = pd.read_excel(path, dtype=str)
             
            # Remove any '.0' or 'nan' artifacts if they persist from previous saves?
            # With dtype=str, "123" stays "123". "123.0" stays "123.0".
            # If the User's Excel ACTUALLY has "123.0" stored as text, we might need cleanup.
            # But usually it helps.
            
            # Clear previous DF to ensures updates
            if key in st.session_state.xlsx_dfs:
                del st.session_state.xlsx_dfs[key]
            
            # 3. Validate Columns
            expected_map = XML_COL_MAPS.get(key, {})
            
            # Use VALUES (Target XML Tags) for validation
            # The Excel file is expected to have the Final MixedCase names.
            required_cols = set(expected_map.values())
            
            # Check what we have (Case Insensitive? No, User says "mesmo formato")
            # But let's be lenient and check if we can map them back.
            # Actually, the XML generator is case-insensitive on input ( uppercases everything ).
            # BUT we want to ensure the headers match the "Target" names for clarity.
            # If we check Values, we check strict MixedCase from the map.
             
            df_cols = set(df.columns)
            
            missing = required_cols - df_cols
            
            # If we have case mismatch, we might want to warn or handle. 
            # But the user claimed "as colunas constam", so let's stick to strict or loose?
            # User said: "Sendo a primeira chamada 'SistemasSUS' e a segunda 'TipoEstabelecimentoSaude'"
            # These match the Values exactly.
            
            if missing:
                st.session_state.xlsx_status[key] = False
                st.session_state.xlsx_errors[key] = f"Colunas faltando: {', '.join(missing)}"
            else:
                st.session_state.xlsx_status[key] = True
                st.session_state.xlsx_dfs[key] = df
                
        except Exception as e:
             st.session_state.xlsx_status[key] = False
             st.session_state.xlsx_errors[key] = f"Erro ao ler arquivo: {e}"

def render_xlsx_xml_export(df, layout_type):
    # Reuses reuse logic, but we need the Global Context (TCE, Year, Month)
    # create mini context inputs if not present? 
    # Use globals from session state
    
    codigo_tce = st.session_state.get('global_codigo_tce', '000')
    exercicio = st.session_state.get('global_exercicio', '2024')
    mes = st.session_state.get('global_mes', '01')
    
    try:
        xml_str = generate_xml_string(df, layout_type, codigo_tce, exercicio, mes)
        
        # Map layout to specific filename
        filename_map = {
            '11.1': 'EstabelecimentoSaude',
            '11.2': 'VinculoProfissionalSaude',
            '11.3': 'EstabelecimentoLeito',
            '11.4': 'EstabelecimentoEquipamento',
            '11.5': 'FichaProgramacaoOrcamentaria',
            '11.8': 'AutorizacaoInternacaoHospitalar'
        }
        base_name = filename_map.get(layout_type, f"layout_{layout_type}")
        
        st.download_button(
            label="💾 Baixar XML",
            data=xml_str,
            file_name=f"{base_name}.xml",
            mime="application/xml",
            key=f"btn_xlsx_xml_{layout_type}"
        )
    except Exception as e:
        st.error(f"Erro XML: {e}")
