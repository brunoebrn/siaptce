import fdb
import pandas as pd
import sqlite3
import os
import sys
import json
from connector import FirebirdConnector

def transform_and_load(dsn, user, password, output_db, mapping_json):
    try:
        mapping = json.loads(mapping_json)
        
        # Hardcoded logic for Layout 11.3 2-table join
        # Tables: LFCES002 (Base - Leitos), LFCES004 (Unidades)
        # Joins: T02.UNIDADE_ID = T04.UNIDADE_ID
        
        main_table = "LFCES002"
        cols_map = mapping.get('columns_main', {})
        
        conn_fb = FirebirdConnector(dsn, user, password)
        conn_fb.connect()
        
        print(f"Executando Query Complexa (Joins) em {main_table}...")
        
        select_parts = []
        for target, source in cols_map.items():
            if source:
                select_parts.append(f"{source} as {target}")
                
        # Only add valid SQL parts
        if not select_parts:
             query_cols = "*" # Fallback
        else:
             query_cols = ", ".join(select_parts)
        
        # The Query
        # T02 = LFCES002, T04 = LFCES004
        
        sql = f"""
        SELECT {query_cols}
        FROM {main_table} T02
        LEFT JOIN LFCES004 T04 ON T02.UNIDADE_ID = T04.UNIDADE_ID
        """
        
        print(f"SQL Gerado: {sql}")
        
        data_main, cols_main = conn_fb.execute_query(sql)
        df_result = pd.DataFrame(data_main, columns=cols_main)
        
        conn_fb.close()
        
        # --- Transformations 11.3 ---
        print("Transformando dados (Layout 11.3)...")
        # Standardize keys to upper
        df_result.columns = [c.upper() for c in df_result.columns]

        # CNES -> 7 digits
        if 'CNES' in df_result.columns:
             df_result['CNES'] = df_result['CNES'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(7)
             
        # CodigoLeito -> 2 digits
        if 'CODIGOLEITO' in df_result.columns:
             df_result['CODIGOLEITO'] = df_result['CODIGOLEITO'].fillna(0).astype(int).astype(str).str.zfill(2)
             
        # TipoLeito -> 1 digit
        if 'TIPOLEITO' in df_result.columns:
             df_result['TIPOLEITO'] = df_result['TIPOLEITO'].fillna(0).astype(int).astype(str).str.zfill(1) # Tabela 41 usually 1 digit? Layout says 1.

        # Quantidade -> 6 digits
        if 'QUANTIDADE' in df_result.columns:
             df_result['QUANTIDADE'] = df_result['QUANTIDADE'].fillna(0).astype(int).astype(str).str.zfill(6)
             
        # QuantidadeSUS -> 6 digits
        if 'QUANTIDADESUS' in df_result.columns:
             df_result['QUANTIDADESUS'] = df_result['QUANTIDADESUS'].fillna(0).astype(int).astype(str).str.zfill(6)


        # Final Rename for XML Standard
        rename_map = {
            'CNES': 'CNES',
            'CODIGOLEITO': 'CodigoLeito',
            'TIPOLEITO': 'TipoLeito',
            'QUANTIDADE': 'Quantidade',
            'QUANTIDADESUS': 'QuantidadeSUS'
        }
        df_result = df_result.rename(columns=rename_map)

        # --- Loading to SQLite ---
        print(f"Salvando {len(df_result)} registros em {output_db}...")
        
        os.makedirs(os.path.dirname(output_db), exist_ok=True)
        
        conn_sqlite = sqlite3.connect(output_db)
        df_result.to_sql('layout_11_3', conn_sqlite, if_exists='replace', index=False)
        conn_sqlite.close()
        
        print("ETL (11.3) concluído com sucesso.")
        return True

    except Exception as e:
        print(f"Erro no ETL: {e}", file=sys.stderr)
        return False

# Standalone execution for subprocess
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Worker Process for Layout 11.3')
    parser.add_argument('--dsn', required=True)
    parser.add_argument('--user', default='SYSDBA')
    parser.add_argument('--password', default='masterkey')
    parser.add_argument('--output', required=True)
    parser.add_argument('--mapping', required=True) # JSON string
    
    args = parser.parse_args()
    
    success = transform_and_load(args.dsn, args.user, args.password, args.output, args.mapping)
    if not success:
        sys.exit(1)
