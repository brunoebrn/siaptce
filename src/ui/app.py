import streamlit as st
import sys
import os
import time

# Adicionar root ao path para imports funcionarem
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

import src.ingestion.firebird_engine
import src.ui.layouts.layout_11_1
import src.ui.layouts.layout_11_2
import src.ui.db_explorer
from src.utils.logger import setup_logger

# Initialize Logger
logger = setup_logger("UI")

import importlib
importlib.reload(src.ingestion.firebird_engine)
importlib.reload(src.ui.layouts.layout_11_1)
importlib.reload(src.ui.layouts.layout_11_2)
importlib.reload(src.ui.db_explorer)
from src.ingestion.firebird_engine import HealthDataIngestor
import src.ui.layouts.layout_11_1 as layout_11_1
import src.ui.layouts.layout_11_2 as layout_11_2
import src.ui.layouts.layout_11_3 as layout_11_3
import src.ui.layouts.layout_11_4 as layout_11_4
import src.ui.layouts.layout_11_5 as layout_11_5
import src.ui.layouts.layout_11_8 as layout_11_8
import src.ui.db_explorer as db_explorer
import src.ui.xlsx_converter as xlsx_converter

def main():
    st.set_page_config(
        page_title="SIAP - Auditoria e Ingestão",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    logger.info("Application Started")

    # Initialize Session State
    if 'step' not in st.session_state:
        st.session_state.step = 'setup'
    if 'db_paths' not in st.session_state:
        st.session_state.db_paths = {
            'CNES': '',
            'FPO': '',
            'SIH': '',
            'SIA': ''
        }
    if 'db_status' not in st.session_state:
        st.session_state.db_status = {
            'CNES': None,
            'FPO': None,
            'SIH': None,
            'SIA': None
        }
    if 'db_errors' not in st.session_state:
        st.session_state.db_errors = {}

    # --- Global Filters ---
    st.sidebar.header("Configurações Gerais")
    st.session_state['global_codigo_tce'] = st.sidebar.text_input("Código TCE", "000")
    st.session_state['global_exercicio'] = st.sidebar.text_input("Exercício", "2024")
    st.session_state['global_mes'] = st.sidebar.selectbox("Mês", [f"{i:02d}" for i in range(1, 13)])
    st.sidebar.divider()

    # --- Sidebar Navigation ---
    render_sidebar_navigation()

    # --- Main Content Routing ---
    if st.session_state.step == 'setup':
        render_setup_screen()
    elif st.session_state.step == 'dashboard':
        render_dashboard_screen()
    elif st.session_state.step == 'db_explorer':
        db_explorer.render_db_explorer()
    elif st.session_state.step == 'xlsx_converter':
        xlsx_converter.render_xlsx_converter()
    elif st.session_state.step == 'layout_11_1':
        layout_11_1.render_layout_11_1()
    elif st.session_state.step == 'layout_11_2':
        layout_11_2.render_layout_11_2()
    elif st.session_state.step == 'layout_11_3':
        layout_11_3.render_layout_11_3()
    elif st.session_state.step == 'layout_11_4':
        layout_11_4.render_layout_11_4()
    elif st.session_state.step == 'layout_11_5':
        layout_11_5.render_layout_11_5()
    elif st.session_state.step == 'layout_11_8':
        layout_11_8.render_layout_11_8()

def render_sidebar_navigation():
    """Renders buttons in the sidebar based on the current context."""
    
    # Don't show nav if in Setup
    if st.session_state.step == 'setup':
        return

    st.sidebar.subheader("Navegação")
    
    # "Voltar para Configuração" - Only on Dashboard
    if st.session_state.step == 'dashboard':
        if st.sidebar.button("⬅️ Voltar para Configuração", use_container_width=True):
            st.session_state.step = 'setup'
            st.rerun()
        st.sidebar.divider()

    # "Voltar ao Painel" - On all pages EXCEPT Dashboard and Setup
    if st.session_state.step != 'dashboard' and st.session_state.step != 'setup':
        if st.sidebar.button("🏠 Voltar ao Painel", use_container_width=True):
             st.session_state.step = 'dashboard'
             st.rerun()
        st.sidebar.divider()
        
    # List of Layouts (Direct Access)
    st.sidebar.markdown("**Acesso Rápido**")
    
    # Use distinct keys to avoid conflict if multiple buttons exist
    if st.sidebar.button("🏥 11.1 - Estabelecimentos", use_container_width=True):
        st.session_state.step = 'layout_11_1'
        st.rerun()
        
    if st.sidebar.button("👨‍⚕️ 11.2 - Vínculos", use_container_width=True):
        st.session_state.step = 'layout_11_2'
        st.rerun()

    if st.sidebar.button("🛏️ 11.3 - Leitos", use_container_width=True):
        st.session_state.step = 'layout_11_3'
        st.rerun()

    if st.sidebar.button("🔬 11.4 - Equipamentos", use_container_width=True):
        st.session_state.step = 'layout_11_4'
        st.rerun()
        
    if st.sidebar.button("💰 11.5 - Orçamento", use_container_width=True):
        st.session_state.step = 'layout_11_5'
        st.rerun()

    if st.sidebar.button("🚑 11.8 - Internação", use_container_width=True):
        st.session_state.step = 'layout_11_8'
        st.rerun()
        
    if st.sidebar.button("📂 DB Explorer", use_container_width=True):
        st.session_state.step = 'db_explorer'
        st.rerun()
        
    if st.sidebar.button("🔄 Conversor XLSX", use_container_width=True):
        st.session_state.step = 'xlsx_converter'
        st.rerun()

    st.sidebar.divider()
    if st.sidebar.button("❌ Encerrar Sistema", type="primary", use_container_width=True):
        logger.info("Shutdown requested by user.")
        st.warning("Encerrando sistema...")
        time.sleep(1)
        os._exit(0)


def render_setup_screen():
    st.title("🔧 Configuração de Fontes de Dados")
    st.markdown("Informe o caminho local dos arquivos de banco de dados (.gdb ou .fdb) para iniciar a sessão.")
    st.divider()

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Caminhos dos Arquivos")
        
        # Inputs for each DB
        st.session_state.db_paths['CNES'] = st.text_input("Banco de Dados CNES", value=st.session_state.db_paths['CNES'], placeholder="C:\\dados\\cnes.gdb")
        st.session_state.db_paths['FPO'] = st.text_input("Banco de Dados FPO", value=st.session_state.db_paths['FPO'], placeholder="C:\\dados\\fpo.gdb")
        st.session_state.db_paths['SIH'] = st.text_input("Banco de Dados SIH", value=st.session_state.db_paths['SIH'], placeholder="C:\\dados\\sih.gdb")
        st.session_state.db_paths['SIA'] = st.text_input("Banco de Dados SIA", value=st.session_state.db_paths['SIA'], placeholder="C:\\dados\\sia.gdb")

    with col2:
        st.subheader("Status da Validação")
        st.markdown("") 
        st.markdown("")
        
        # Display Status Icons
        for db, status in st.session_state.db_status.items():
            if status is True:
                st.success(f"**{db}**: Conectado ✅")
            elif status is False:
                st.error(f"**{db}**: Falha ❌")
                err_msg = st.session_state.db_errors.get(db)
                if err_msg:
                    st.caption(f"📝 {err_msg}")
            else:
                st.info(f"**{db}**: Aguardando...")

    st.divider()

    # Action Buttons
    c_btn1, c_btn2 = st.columns([1, 4])
    
    with c_btn1:
        if st.button("Validar Bases de Dados", type="primary", use_container_width=True):
            with st.spinner("Testando conexões..."):
                all_valid = True
                for db in ['CNES', 'FPO', 'SIH', 'SIA']:
                    path = st.session_state.db_paths.get(db)
                    
                    if db in st.session_state.db_errors:
                        del st.session_state.db_errors[db]
                        
                    if path:
                        path = path.strip().strip('"')
                        st.session_state.db_paths[db] = path 
                        
                        success, message = HealthDataIngestor.check_connection(path)
                        st.session_state.db_status[db] = success
                        
                        if not success:
                            st.session_state.db_errors[db] = message
                            all_valid = False
                    else:
                        st.session_state.db_status[db] = False
                        st.session_state.db_errors[db] = "Caminho não informado."
                        all_valid = False
                
                if all_valid:
                    st.toast("Todas as conexões foram bem-sucedidas!", icon="✅")
                else:
                    st.toast("Algumas conexões falharam. Verifique os caminhos.", icon="❌")
                
                time.sleep(0.5) 
                st.rerun()
    
    at_least_one_valid = any(st.session_state.db_status.values())
    
    with c_btn2:
        if at_least_one_valid:
            if st.button("Avançar para Layouts ➡️", type="secondary"):
                st.session_state.step = 'dashboard'
                st.rerun()
        else:
            st.button("Avançar para Layouts ➡️", disabled=True, help="Valide pelo menos uma base de dados para continuar.")

def render_dashboard_screen():
    st.title("Painel de Controle")
    st.markdown("Acesso aos Layouts do SIAP.")
    
    # Explorer Button and XLSX Converter
    c_exp, c_conv = st.columns(2)
    with c_exp:
        if st.button("🔍 Explorador de Banco de Dados", type="secondary", use_container_width=True):
            st.session_state.step = 'db_explorer'
            st.rerun()
    with c_conv:
        if st.button("🔄 Conversor XLSX para XML", type="secondary", use_container_width=True):
            st.session_state.step = 'xlsx_converter'
            st.rerun()

    st.divider()

    # Layouts Cards
    
    # Grupo CNES
    st.subheader("Grupo CNES")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.info("Layout 11.1")
        st.caption("Estabelecimentos")
        if st.button("Acessar 11.1", key="btn_dash_11_1", use_container_width=True):
            st.session_state.step = 'layout_11_1'
            st.rerun()
    with c2:
        st.info("Layout 11.2")
        st.caption("Vínculo Profissional")
        if st.button("Acessar 11.2", key="btn_dash_11_2", use_container_width=True):
             st.session_state.step = 'layout_11_2'
             st.rerun()
    with c3:
        st.info("Layout 11.3")
        st.caption("Leitos")
        if st.button("Acessar 11.3", key="btn_dash_11_3", use_container_width=True):
            st.session_state.step = 'layout_11_3'
            st.rerun()
    with c4:
        st.info("Layout 11.4")
        st.caption("Equipamentos")
        if st.button("Acessar 11.4", key="btn_dash_11_4", use_container_width=True):
            st.session_state.step = 'layout_11_4'
            st.rerun()

    st.markdown("---")

    # Grupo FPO e SIH
    col_fpo, col_sih = st.columns(2)
    
    with col_fpo:
        st.subheader("Grupo FPO")
        st.warning("Layout 11.5")
        st.caption("Ficha de Programação Orçamentária")
        if st.button("Acessar 11.5", key="btn_dash_11_5", use_container_width=True):
             st.session_state.step = 'layout_11_5'
             st.rerun()
        
    with col_sih:
        st.subheader("Grupo SIH")
        st.error("Layout 11.8")
        st.caption("Sistema de Informação Hospitalar")
        if st.button("Acessar 11.8", key="btn_dash_11_8", use_container_width=True):
             st.session_state.step = 'layout_11_8'
             st.rerun()

    st.markdown("---")

    # Grupo SIA
    st.subheader("Grupo SIA")
    c_sia1, c_sia2 = st.columns(2)
    with c_sia1:
        st.success("Layout 11.6")
        st.caption("Produção Ambulatorial")
        st.button("Acessar 11.6", key="btn_dash_11_6", use_container_width=True)
    with c_sia2:
        st.success("Layout 11.7")
        st.caption("Apuração Ambulatorial")
        st.button("Acessar 11.7", key="btn_dash_11_7", use_container_width=True)

if __name__ == "__main__":
    main()
