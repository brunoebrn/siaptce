import streamlit as st
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
from xml.dom import minidom
import os

def render_readonly_mapping(mapping: dict):
    """
    Renders a read-only table of the current column mapping.
    """
    st.subheader("Mapeamento de Colunas (Fixo)")
    
    # Convert dict to DataFrame for display
    data = []
    defaults = mapping.get('defaults', {})
    
    # If mapping has 'columns_main', use that, otherwise use defaults directly if structure differs
    # But usually the UI builds 'columns_main' from defaults. 
    # For read-only, we might just display the expected keys and their hardcoded values.
    
    # Actually, the user requirement says "transformar apenas em exibição".
    # We will assume the mapping passed is the final one used for ETL.
    
    if 'columns_main' in mapping:
        source_map = mapping['columns_main']
    else:
        source_map = defaults # Fallback
        
    for target, source in source_map.items():
        data.append({"Campo XML": target, "Coluna Origem (CNES)": source})
        
    df = pd.DataFrame(data)
    st.table(df)

import io

def render_data_preview(db_path: str, table_name: str):
    """
    Renders a preview of the data in the SQLite DB.
    """
    if not db_path or not os.path.exists(db_path):
        return

    st.divider()
    st.subheader("Pré-visualização dos Dados")
    
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()
        
        st.dataframe(df, use_container_width=True)
        st.caption(f"Total de Registros: {len(df)}")

        # Excel Export
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Dados')
        
        st.download_button(
            label="📥 Baixar em Excel (.xlsx)",
            data=buffer.getvalue(),
            file_name=f"{table_name}_export.xlsx",
            mime="application/vnd.ms-excel"
        )

        return df
    except Exception as e:
        st.error(f"Erro ao ler banco de dados gerado: {e}")
        return None

def render_xml_button(df: pd.DataFrame, layout_type: str):
    """
    Renders the Download XML button using global filters from session_state.
    
    layout_type: '11.1', '11.2', '11.3', '11.4'
    """
    st.divider()
    st.subheader(f"Gerar XML ({layout_type})")
    
    # Retrieve globals
    codigo_tce = st.session_state.get('global_codigo_tce', '000')
    exercicio = st.session_state.get('global_exercicio', '2024')
    mes = st.session_state.get('global_mes', '01')
    
    # Display read-only context
    c1, c2, c3 = st.columns(3)
    c1.text_input("Código TCE", value=codigo_tce, disabled=True, key=f"disp_tce_{layout_type}")
    c2.text_input("Exercício", value=exercicio, disabled=True, key=f"disp_exe_{layout_type}")
    c3.text_input("Mês", value=mes, disabled=True, key=f"disp_mes_{layout_type}")
    
    if st.button("💾 Baixar XML", key=f'btn_xml_{layout_type}'):
        if df is None or df.empty:
            st.warning("Nenhum dado para exportar.")
            return

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
                label="Download XML",
                data=xml_str,
                file_name=f"{base_name}.xml",
                mime="application/xml"
            )
        except Exception as e:
            st.error(f"Erro ao gerar XML: {e}")

def generate_xml_string(df, layout_type, codigo, exercicio, mes):
    root = ET.Element("SIAP")
    ET.SubElement(root, "Codigo").text = codigo
    ET.SubElement(root, "Exercicio").text = exercicio
    ET.SubElement(root, "Mes").text = mes
    
    # Define row tag and mapping based on layout
    # SQLite columns are expected to MATCH XML tag names now (as per refactor plan)
    
    row_tag_map = {
        '11.1': 'EstabelecimentoSaude',
        '11.2': 'VinculoProfissionalSaude',
        '11.3': 'EstabelecimentoLeito',
        '11.4': 'EstabelecimentoEquipamento',
        '11.5': 'FichaProgramacaoOrcamentaria',
        '11.8': 'AutorizacaoInternacaoHospitalar'
    }
    
    row_tag = row_tag_map.get(layout_type, "Registro")
    
    # Strict Column Case Enforcement for XML
    # Maps UPPERCASE (or internal) column names to Manual MixedCase
    xml_col_maps = {
        '11.1': {
            'CNES': 'CNES', 'CNPJ': 'CNPJ', 'NOMEFANTASIA': 'NomeFantasia',
            'RAZAOSOCIAL': 'RazaoSocial', 'ENDERECO': 'Endereco', 'CEP': 'CEP',
            'CPFDIRETOR': 'CPFDiretor', 'TIPO': 'Tipo', 'TIPOESTABELECIMENTOSAUDE': 'TipoEstabelecimentoSaude',
            'ATIVIDADEPRINCIPAL': 'AtividadePrincipal', 'SISTEMASSUS': 'SistemasSUS',
            'SISTEMASUS': 'SistemasSUS' 
        },
        '11.2': {
            'CNS': 'CNS', 'CPF': 'CPF', 'CNES': 'CNES', 'MATRICULA': 'Matricula',
            'VINCULO': 'Vinculo', 'OCUPACAO': 'Ocupacao', 
            'CARGAHORARIAAMBULATORIO': 'CargaHorariaAmbulatorio',
            'CARGAHORARIAHOSPITAL': 'CargaHorariaHospital',
            'CARGAHORARIATOTAL': 'CargaHorariaTotal'
        },
        '11.3': {
            'CNES': 'CNES', 'CODIGOLEITO': 'CodigoLeito', 'TIPOLEITO': 'TipoLeito',
            'QUANTIDADE': 'Quantidade', 'QUANTIDADESUS': 'QuantidadeSUS'
        },
        '11.4': {
            'CNES': 'CNES', 'CODIGO': 'CodigoEquipamento', 'CODIGOEQUIPAMENTO': 'CodigoEquipamento',
            'TIPO': 'TipoEquipamentoSaude', 'TIPOEQUIPAMENTOSAUDE': 'TipoEquipamentoSaude',
            'QUANTIDADE': 'Quantidade', 'QUANTIDADEUSO': 'QuantidadeUso',
            'DISPONIBILIDADE': 'DisponibilidadeSUS', 'DISPONIBILIDADESUS': 'DisponibilidadeSUS'
        },
        '11.5': {
            'CNES': 'CNES', 'PROCEDIMENTO': 'Procedimento',
            'FINANCIAMENTO': 'Financiamento', 'QUANTIDADE': 'Quantidade',
            'VALORUNITARIO': 'ValorUnitario', 'VALORTOTAL': 'ValorTotal'
        },
        '11.8': {
            'CNES': 'CNES', 'NUMEROAIH': 'NumeroAIH', 'IDENTIFICACAO': 'Identificacao',
            'ESPECIALIDADELEITO': 'EspecialidadeLeito', 'MODALIDADEINTERNACAO': 'ModalidadeInternacao',
            'AIHANTERIOR': 'AIHAnterior', 'DATAEMISSAO': 'DataEmissao',
            'DATAINTERNACAO': 'DataInternacao', 'DATASAIDA': 'DataSaida',
            'PROCEDIMENTOSOLICITADO': 'ProcedimentoSolicitado',
            'CARATERINTERNACAO': 'CaraterInternacao', 'MOTIVOSAIDA': 'MotivoSaida',
            'CNSSOLICITANTE': 'CNSSolicitante', 'CNSRESPONSAVEL': 'CNSResponsavel',
            'CNSAUTORIZADOR': 'CNSAutorizador', 'DIAGNOSTICOPRINCIPAL': 'DiagnosticoPrincipal',
            'CNSPACIENTE': 'CNSPaciente'
        }
    }
    
    target_map = xml_col_maps.get(layout_type, {})
    
    # Create a new df with renamed columns for XML generation only
    # We upper() the current columns to match keys easily
    xml_df = df.copy()
    xml_df.columns = [c.upper() for c in xml_df.columns]
    xml_df = xml_df.rename(columns=target_map)
    
    for _, row in xml_df.iterrows():
        reg = ET.SubElement(root, row_tag)
        for col in xml_df.columns:
            # Skip internal or unmapped columns if strictly enforcing? 
            # For now, we write everything, assuming the DF is clean.
            # But the rename map helps enforce the casing.
            
            val = row[col]
            
            # Strict formatting for Monetary/Decimal fields
            # Checks against standardized MixedCase names
            if col in ['ValorUnitario', 'ValorTotal', 'VALORUNITARIO', 'VALORTOTAL']:
                try:
                    text_val = "{:.2f}".format(float(val))
                except:
                    text_val = str(val) if pd.notna(val) else ""
            else:
                text_val = str(val) if pd.notna(val) else ""

            ET.SubElement(reg, col).text = text_val
            
    return minidom.parseString(ET.tostring(root)).toprettyxml(indent="  ")
