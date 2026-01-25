import sys
import os
import sqlite3
import pandas as pd
import fdb

# Add root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.ingestion.connector import FirebirdConnector
from src.utils.logger import setup_logger

# Disable file logging for workers
logger = setup_logger("Worker_11_1", log_to_file=False)

import json

def transform_and_load(dsn, user, password, output_db, mapping_json):
    try:
        mapping = json.loads(mapping_json)
        
        main_table = mapping.get('table_main')
        cols_map = mapping.get('columns_main', {})
        
        conn_fb = FirebirdConnector(dsn, user, password)
        conn_fb.connect()
        
        logger.info(f"Extraindo dados de {main_table}...")
        
        select_parts = []
        for target, source in cols_map.items():
            if source:
                select_parts.append(f"{source} as {target}")
            else:
                select_parts.append(f"NULL as {target}")
                
        query_cols = ", ".join(select_parts)
        if main_table.upper() == 'LFCES004':
            logger.info("Aplicando filtro: Apenas estabelecimentos ativos (CD_MOTIVO_DESAB IS NULL OR '')")
            query_main = f"SELECT {query_cols} FROM {main_table} WHERE (CD_MOTIVO_DESAB IS NULL OR CD_MOTIVO_DESAB = '')"
        else:
            query_main = f"SELECT {query_cols} FROM {main_table}"
        
        data_main, cols_main = conn_fb.execute_query(query_main)
        df_result = pd.DataFrame(data_main, columns=cols_main)
        
        conn_fb.close()
        
        # --- Transformations ---
        logger.info("Aplicando transformações básicas...")
        
        df_result.columns = [c.upper() for c in df_result.columns]
        
        if 'CNES' in df_result.columns:
            df_result['CNES'] = df_result['CNES'].fillna('').astype(str).str.zfill(7)
            
        if 'CNPJ' in df_result.columns:
            df_result['CNPJ'] = df_result['CNPJ'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(14)
            
        if 'CEP' in df_result.columns:
            df_result['CEP'] = df_result['CEP'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
            
        if 'CPFDIRETOR' in df_result.columns:
            df_result['CPFDIRETOR'] = df_result['CPFDIRETOR'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(11)

        if 'ATIVIDADEPRINCIPAL' in df_result.columns:
             def clean_activity(x):
                 if pd.isna(x) or x == '': return '00'
                 try:
                     clean = ''.join(filter(str.isdigit, str(x)))
                     return str(int(clean)).zfill(2)
                 except:
                     return '00'
                     
             df_result['ATIVIDADEPRINCIPAL'] = df_result['ATIVIDADEPRINCIPAL'].apply(clean_activity)

        df_result['SISTEMASSUS'] = 1
        
        rename_map = {
            'CNES': 'CNES',
            'CNPJ': 'CNPJ',
            'NOMEFANTASIA': 'NomeFantasia',
            'RAZAOSOCIAL': 'RazaoSocial',
            'ENDERECO': 'Endereco',
            'CEP': 'CEP',
            'CPFDIRETOR': 'CPFDiretor',
            'TIPOESTABELECIMENTOSAUDE': 'TipoEstabelecimentoSaude',
            'ATIVIDADEPRINCIPAL': 'AtividadePrincipal',
            'SISTEMASSUS': 'SistemasSUS'
        }
        df_result = df_result.rename(columns=rename_map)

        logger.info(f"Salvando {len(df_result)} registros em {output_db}...")
        
        os.makedirs(os.path.dirname(output_db), exist_ok=True)
        
        conn_sqlite = sqlite3.connect(output_db)
        df_result.to_sql('layout_11_1', conn_sqlite, if_exists='replace', index=False)
        conn_sqlite.close()
        
        logger.info("ETL concluído com sucesso.")
        return True

    except Exception as e:
        logger.error(f"Erro no ETL: {e}")
        return False

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dsn', required=True)
    parser.add_argument('--user', default='SYSDBA')
    parser.add_argument('--password', default='masterkey')
    parser.add_argument('--output', required=True)
    parser.add_argument('--mapping', required=True) # New arg
    
    args = parser.parse_args()
    
    success = transform_and_load(args.dsn, args.user, args.password, args.output, args.mapping)
    if not success:
        sys.exit(1)
