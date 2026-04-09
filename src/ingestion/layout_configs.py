import pandas as pd
import re
from datetime import datetime

def clean_numeric(val, length=0):
    if pd.isna(val) or val == '': return ''
    s = str(val).replace('.0', '')
    s = "".join(filter(str.isdigit, s))
    if length > 0:
        s = s.zfill(length)
    return s

def format_date(val):
    if not val or pd.isna(val): return ""
    try:
        if isinstance(val, (pd.Timestamp, datetime)):
            return val.strftime('%Y-%m-%d')
        return str(val).split()[0]
    except:
        return str(val)

# --- Transformers ---

def transform_11_1(df):
    """Layout 11.1 - Estabelecimentos"""
    df.columns = [c.upper() for c in df.columns]
    if 'CNES' in df.columns: df['CNES'] = df['CNES'].apply(lambda x: clean_numeric(x, 7))
    if 'CNPJ' in df.columns: df['CNPJ'] = df['CNPJ'].apply(lambda x: clean_numeric(x, 14))
    if 'CEP' in df.columns: df['CEP'] = df['CEP'].apply(lambda x: clean_numeric(x, 8))
    if 'CPFDIRETOR' in df.columns: df['CPFDIRETOR'] = df['CPFDIRETOR'].apply(lambda x: clean_numeric(x, 11))
    if 'ATIVIDADEPRINCIPAL' in df.columns:
        df['ATIVIDADEPRINCIPAL'] = df['ATIVIDADEPRINCIPAL'].apply(lambda x: clean_numeric(x, 2) if x else '00')
    df['SISTEMASUS'] = 1
    return df.rename(columns={
        'CNES': 'CNES', 'CNPJ': 'CNPJ', 'NOMEFANTASIA': 'NomeFantasia', 'RAZAOSOCIAL': 'RazaoSocial',
        'ENDERECO': 'Endereco', 'CEP': 'CEP', 'CPFDIRETOR': 'CPFDiretor', 
        'TIPOESTABELECIMENTOSAUDE': 'TipoEstabelecimentoSaude', 'ATIVIDADEPRINCIPAL': 'AtividadePrincipal', 'SISTEMASUS': 'SistemaSUS'
    })

def transform_11_2(df):
    """Layout 11.2 - Vínculos"""
    df.columns = [c.upper() for c in df.columns]
    if 'CNS' in df.columns: df['CNS'] = df['CNS'].apply(lambda x: clean_numeric(x, 15))
    if 'CPF' in df.columns: df['CPF'] = df['CPF'].apply(lambda x: clean_numeric(x, 11))
    if 'CNES' in df.columns: df['CNES'] = df['CNES'].apply(lambda x: clean_numeric(x, 7))
    if 'VINCULO' in df.columns: df['VINCULO'] = df['VINCULO'].apply(lambda x: clean_numeric(x, 6))
    if 'OCUPACAO' in df.columns: df['OCUPACAO'] = df['OCUPACAO'].apply(lambda x: clean_numeric(x, 7))
    
    for col in ['CARGAHORARIAAMBULATORIO', 'CARGAHORARIAHOSPITAL', 'CARGAHORARIAOUTROS']:
        df[col] = pd.to_numeric(df.get(col, 0), errors='coerce').fillna(0).astype(int)
    df['CARGAHORARIATOTAL'] = df['CARGAHORARIAAMBULATORIO'] + df['CARGAHORARIAHOSPITAL'] + df['CARGAHORARIAOUTROS']
    for ch in ['CARGAHORARIAAMBULATORIO', 'CARGAHORARIAHOSPITAL', 'CARGAHORARIATOTAL']:
        df[ch] = df[ch].astype(str).str.zfill(2)

    return df.rename(columns={
        'CNS': 'CNS', 'CPF': 'CPF', 'CNES': 'CNES', 'MATRICULA': 'Matricula',
        'VINCULO': 'Vinculo', 'OCUPACAO': 'Ocupacao', 'CARGAHORARIAAMBULATORIO': 'CargaHorariaAmbulatorio',
        'CARGAHORARIAHOSPITAL': 'CargaHorariaHospital', 'CARGAHORARIATOTAL': 'CargaHorariaTotal'
    })

def transform_11_3(df):
    """Layout 11.3 - Leitos"""
    df.columns = [c.upper() for c in df.columns]
    if 'CNES' in df.columns: df['CNES'] = df['CNES'].apply(lambda x: clean_numeric(x, 7))
    if 'CODIGOLEITO' in df.columns: df['CODIGOLEITO'] = df['CODIGOLEITO'].apply(lambda x: clean_numeric(x, 2))
    if 'TIPOLEITO' in df.columns: df['TIPOLEITO'] = df['TIPOLEITO'].apply(lambda x: clean_numeric(x, 1))
    if 'QUANTIDADE' in df.columns: df['QUANTIDADE'] = df['QUANTIDADE'].apply(lambda x: clean_numeric(x, 6))
    if 'QUANTIDADESUS' in df.columns: df['QUANTIDADESUS'] = df['QUANTIDADESUS'].apply(lambda x: clean_numeric(x, 6))
    return df.rename(columns={
        'CNES': 'CNES', 'CODIGOLEITO': 'CodigoLeito', 'TIPOLEITO': 'TipoLeito', 
        'QUANTIDADE': 'Quantidade', 'QUANTIDADESUS': 'QuantidadeSUS'
    })

def transform_11_4(df):
    """Layout 11.4 - Equipamentos"""
    df.columns = [c.upper() for c in df.columns]
    if 'CNES' in df.columns: df['CNES'] = df['CNES'].apply(lambda x: clean_numeric(x, 7))
    if 'CODIGO' in df.columns: df['CODIGO'] = df['CODIGO'].apply(lambda x: clean_numeric(x, 6))
    if 'TIPO' in df.columns: df['TIPO'] = df['TIPO'].apply(lambda x: clean_numeric(x, 2))
    if 'QUANTIDADE' in df.columns: df['QUANTIDADE'] = df['QUANTIDADE'].apply(lambda x: clean_numeric(x, 3))
    if 'QUANTIDADEUSO' in df.columns: df['QUANTIDADEUSO'] = df['QUANTIDADEUSO'].apply(lambda x: clean_numeric(x, 3))
    if 'DISPONIBILIDADE' in df.columns: df['DISPONIBILIDADE'] = df['DISPONIBILIDADE'].apply(lambda x: clean_numeric(x, 1))
    return df.rename(columns={
        'CNES': 'CNES', 'CODIGO': 'CodigoEquipamento', 'TIPO': 'TipoEquipamentoSaude',
        'QUANTIDADE': 'Quantidade', 'QUANTIDADEUSO': 'QuantidadeUso', 'DISPONIBILIDADE': 'DisponibilidadeSUS'
    })

def transform_11_5(df):
    """Layout 11.5 - FPO"""
    df.columns = [c.upper() for c in df.columns]
    if 'CNES' in df.columns: df['CNES'] = df['CNES'].apply(lambda x: clean_numeric(x, 7))
    if 'PROCEDIMENTO' in df.columns: df['PROCEDIMENTO'] = df['PROCEDIMENTO'].apply(lambda x: clean_numeric(x, 10))
    if 'FINANCIAMENTO' in df.columns:
        df['FINANCIAMENTO'] = df['FINANCIAMENTO'].apply(lambda x: {'1':'PAB', '2':'MAC', '3':'FAEC'}.get(str(x).strip(), str(x).strip()))
    if 'QUANTIDADE' in df.columns: df['QUANTIDADE'] = pd.to_numeric(df['QUANTIDADE'], errors='coerce').fillna(0).astype(int)
    if 'VALORUNITARIO' in df.columns: df['VALORUNITARIO'] = pd.to_numeric(df['VALORUNITARIO'], errors='coerce').fillna(0).round(2)
    if 'VALORTOTAL' in df.columns: df['VALORTOTAL'] = pd.to_numeric(df['VALORTOTAL'], errors='coerce').fillna(0).round(2)
    return df.rename(columns={
        'CNES': 'CNES', 'PROCEDIMENTO': 'Procedimento', 'FINANCIAMENTO': 'Financiamento',
        'QUANTIDADE': 'Quantidade', 'VALORUNITARIO': 'ValorUnitario', 'VALORTOTAL': 'ValorTotal'
    })

def transform_11_8(df):
    """Layout 11.8 - SIH"""
    df.columns = [c.upper() for c in df.columns]
    for date_col in ['DATAEMISSAO', 'DATAINTERNACAO', 'DATASAIDA']:
        if date_col in df.columns: df[date_col] = df[date_col].apply(format_date)
    if 'CNES' in df.columns: df['CNES'] = df['CNES'].apply(lambda x: clean_numeric(x, 7))
    if 'NUMEROAIH' in df.columns: df['NUMEROAIH'] = df['NUMEROAIH'].apply(lambda x: clean_numeric(x, 13))
    if 'AIHANTERIOR' in df.columns: df['AIHANTERIOR'] = df['AIHANTERIOR'].apply(lambda x: clean_numeric(x, 13) if x else "0000000000000")
    if 'IDENTIFICACAO' in df.columns: df['IDENTIFICACAO'] = df['IDENTIFICACAO'].apply(lambda x: clean_numeric(x, 2))
    for cns_col in ['CNSSOLICITANTE', 'CNSRESPONSAVEL', 'CNSAUTORIZADOR', 'CNSPACIENTE']:
         if cns_col in df.columns: df[cns_col] = df[cns_col].apply(lambda x: clean_numeric(x, 15))
    return df.rename(columns={
        'CNES': 'CNES', 'NUMEROAIH': 'NumeroAIH', 'IDENTIFICACAO': 'Identificacao', 'ESPECIALIDADELEITO': 'EspecialidadeLeito',
        'MODALIDADEINTERNACAO': 'ModalidadeInternacao', 'AIHANTERIOR': 'AIHAnterior', 'DATAEMISSAO': 'DataEmissao',
        'DATAINTERNACAO': 'DataInternacao', 'DATASAIDA': 'DataSaida', 'PROCEDIMENTOSOLICITADO': 'ProcedimentoSolicitado',
        'CARATERINTERNACAO': 'CaraterInternacao', 'MOTIVOSAIDA': 'MotivoSaida', 'CNSSOLICITANTE': 'CNSSolicitante',
        'CNSRESPONSAVEL': 'CNSResponsavel', 'CNSAUTORIZADOR': 'CNSAutorizador', 'DIAGNOSTICOPRINCIPAL': 'DiagnosticoPrincipal', 'CNSPACIENTE': 'CNSPaciente'
    })

# --- Configs ---

LAYOUT_CONFIGS = {
    '11.1': {
        'query_builder': lambda table, cols, comp: f"SELECT {cols} FROM {table} WHERE (CD_MOTIVO_DESAB IS NULL OR CD_MOTIVO_DESAB = '')" if table.upper() == 'LFCES004' else f"SELECT {cols} FROM {table}",
        'transformer': transform_11_1
    },
    '11.2': {
        'query_builder': lambda table, cols, comp: f"SELECT {cols} FROM {table} T21 LEFT JOIN LFCES018 T18 ON T21.PROF_ID = T18.PROF_ID LEFT JOIN LFCES004 T04 ON T21.UNIDADE_ID = T04.UNIDADE_ID",
        'transformer': transform_11_2
    },
    '11.3': {
        'query_builder': lambda table, cols, comp: f"SELECT {cols} FROM LFCES002 T02 LEFT JOIN LFCES004 T04 ON T02.UNIDADE_ID = T04.UNIDADE_ID",
        'transformer': transform_11_3
    },
    '11.4': {
        'query_builder': lambda table, cols, comp: f"SELECT {cols} FROM LFCES020 T20 LEFT JOIN LFCES004 T04 ON T20.UNIDADE_ID = T04.UNIDADE_ID",
        'transformer': transform_11_4
    },
    '11.5': {
        'query_builder': lambda table, cols, comp: f"SELECT {cols} FROM S_IPU" + (f" WHERE IPU_CMP = '{comp}'" if comp else ""),
        'transformer': transform_11_5
    },
    '11.8': {
        'query_builder': lambda table, cols, comp: f"SELECT {cols} FROM TB_HAIH" + (f" WHERE AH_CMPT = '{comp}'" if comp else ""),
        'transformer': transform_11_8
    }
}
