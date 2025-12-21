import sys
import os
import sqlite3
import pandas as pd
import fdb

# Add root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.ingestion.connector import FirebirdConnector

import json

def transform_and_load(dsn, user, password, output_db, mapping_json):
    try:
        mapping = json.loads(mapping_json)
        
        main_table = mapping.get('table_main')
        cols_map = mapping.get('columns_main', {})
        
        # secondary mapping is handled if user provides it, for now focusing on main mapping
        # As per dynamic strategy, secondary table might be advanced. 
        # Requirement: "Selecionar a correspondência desejada entre colunas e tabelas"
        # For simplicity in this iteration, we focus on the MAIN table dynamic mapping.
        # If the user selects a main table, we just map everything from there for now.
        
        conn_fb = FirebirdConnector(dsn, user, password)
        conn_fb.connect()
        
        print(f"Extraindo dados de {main_table}...")
        
        # Build SELECT query dynamically
        # TARGET_FIELD -> SOURCE_COLUMN
        # "CNES": "CO_UNIDADE"
        
        select_parts = []
        for target, source in cols_map.items():
            if source:
                select_parts.append(f"{source} as {target}")
            else:
                select_parts.append(f"NULL as {target}")
                
        query_cols = ", ".join(select_parts)
        query_main = f"SELECT {query_cols} FROM {main_table}"
        
        data_main, cols_main = conn_fb.execute_query(query_main)
        df_result = pd.DataFrame(data_main, columns=cols_main)
        
        conn_fb.close()
        
        # --- Transformations ---
        print("Aplicando transformações básicas...")
        
        # 1. Normalize Column Names to match Logic keys
        # The query alias might have quotes or case issues depending on driver
        # Pandas columns will be exactly what was aliased if driver supports it, 
        # typically firebird returns UPPER CASE aliases.
        df_result.columns = [c.upper() for c in df_result.columns]
        
        # Apply standard formattings if columns exist
        
        if 'CNES' in df_result.columns:
            df_result['CNES'] = df_result['CNES'].fillna('').astype(str).str.zfill(7)
            
        if 'CNPJ' in df_result.columns:
            df_result['CNPJ'] = df_result['CNPJ'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(14)
            
        if 'CEP' in df_result.columns:
            df_result['CEP'] = df_result['CEP'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
            
        if 'CPFDIRETOR' in df_result.columns:
            df_result['CPFDIRETOR'] = df_result['CPFDIRETOR'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(11)

        if 'ATIVIDADEPRINCIPAL' in df_result.columns:
             # User request: Ensure 2 digits, remove leading zero if needed.
             # Logic: Convert to int to remove any zeros, then zfill(2).
             # Example: "001" -> 1 -> "01". "05" -> 5 -> "05".
             def clean_activity(x):
                 if pd.isna(x) or x == '': return '00'
                 try:
                     # Remove non-digits first just in case
                     clean = ''.join(filter(str.isdigit, str(x)))
                     return str(int(clean)).zfill(2)
                 except:
                     return '00'
                     
             df_result['ATIVIDADEPRINCIPAL'] = df_result['ATIVIDADEPRINCIPAL'].apply(clean_activity)

        # Force SISTEMASSUS = 1 (User Request)
        # All State establishments belong to SUS.
        df_result['SISTEMASSUS'] = 1
        
        # Final Rename for XML Standard
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

        # --- Loading to SQLite ---
        print(f"Salvando {len(df_result)} registros em {output_db}...")
        
        os.makedirs(os.path.dirname(output_db), exist_ok=True)
        
        conn_sqlite = sqlite3.connect(output_db)
        df_result.to_sql('layout_11_1', conn_sqlite, if_exists='replace', index=False)
        conn_sqlite.close()
        
        print("ETL concluído com sucesso.")
        return True

    except Exception as e:
        print(f"Erro no ETL: {e}", file=sys.stderr)
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
