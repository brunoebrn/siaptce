import sys
import os
import sqlite3
import pandas as pd
import json
import fdb

# Add root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.ingestion.connector import FirebirdConnector

def transform_and_load(dsn, user, password, output_db, mapping_json):
    try:
        mapping = json.loads(mapping_json)
        
        main_table = mapping.get('table_main', 'LFCES021')
        cols_map = mapping.get('columns_main', {})
        
        # Hardcoded logic for Layout 11.2 3-table join as requested
        # Tables: LFCES021 (Base), LFCES018 (Prof), LFCES004 (Unit)
        # Joins: T21.PROF_ID = T18.PROF_ID, T21.UNIDADE_ID = T04.UNIDADE_ID
        
        conn_fb = FirebirdConnector(dsn, user, password)
        conn_fb.connect()
        
        print(f"Executando Query Complexa (Joins) em {main_table}...")
        
        # Identify columns for CH calculation (Need to fetch them even if not mapped strictly to output yet, or usually they are mapped to temp vars)
        # In this specific worker, we will fetch the columns strictly needed.
        # But we need raw values for CH AMB, HOSPITAL, OUTROS to sum them.
        # The user maps 'CargaHorariaAmbulatorio', 'CargaHorariaHospital', 'CargaHorariaOutros' (new input).
        
        # We need to construct a robust SELECT list. 
        # Since we are joining, we should ideally qualify columns, but we don't know the schema ownership easily here.
        # Strategy: Ask Firebird for the columns blindly. If ambiguous, it might fail, but usually these tables have distinct prefixes or we hope so.
        # Actually, simpler: Select the columns defined in mapping.
        
        select_parts = []
        for target, source in cols_map.items():
            if source:
                select_parts.append(f"{source} as {target}")
                
        # Only add valid SQL parts
        query_cols = ", ".join(select_parts)
        
        # The Query
        # Using aliases for clarity, but if the user provided raw column names, we must ensure they match.
        # T21 = LFCES021, T18 = LFCES018, T04 = LFCES004
        
        sql = f"""
        SELECT {query_cols}
        FROM {main_table} T21
        LEFT JOIN LFCES018 T18 ON T21.PROF_ID = T18.PROF_ID
        LEFT JOIN LFCES004 T04 ON T21.UNIDADE_ID = T04.UNIDADE_ID
        """
        
        print(f"SQL Gerado: {sql}")
        
        data_main, cols_main = conn_fb.execute_query(sql)
        df_result = pd.DataFrame(data_main, columns=cols_main)
        
        conn_fb.close()
        
        # --- Transformations 11.2 ---
        print("Transformando dados (Layout 11.2)...")
        # Standardize keys to upper
        df_result.columns = [c.upper() for c in df_result.columns]

        # CNS -> 15 digits
        if 'CNS' in df_result.columns:
            df_result['CNS'] = df_result['CNS'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(15)

        # CPF -> 11 digits
        if 'CPF' in df_result.columns:
            df_result['CPF'] = df_result['CPF'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(11)
            
        # CNES -> 7 digits
        if 'CNES' in df_result.columns:
             df_result['CNES'] = df_result['CNES'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(7)
             
        # Vinculo -> 6 digits
        if 'VINCULO' in df_result.columns:
             df_result['VINCULO'] = df_result['VINCULO'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(6)
             
        # Ocupacao -> 7 digits
        if 'OCUPACAO' in df_result.columns:
             df_result['OCUPACAO'] = df_result['OCUPACAO'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(7)
             
        # Carga Horaria Calculation
        # Inputs expected: CARGAHORARIAAMBULATORIO, CARGAHORARIAHOSPITAL, CARGAHORARIAOUTROS
        # Output: CARGAHORARIATOTAL
        
        # Ensure numeric
        for col in ['CARGAHORARIAAMBULATORIO', 'CARGAHORARIAHOSPITAL', 'CARGAHORARIAOUTROS']:
            if col not in df_result.columns:
                df_result[col] = 0
            else:
                df_result[col] = pd.to_numeric(df_result[col], errors='coerce').fillna(0)
                
        df_result['CARGAHORARIATOTAL'] = (
            df_result['CARGAHORARIAAMBULATORIO'] + 
            df_result['CARGAHORARIAHOSPITAL'] + 
            df_result['CARGAHORARIAOUTROS']
        ).astype(int)
        
        # Format final CH columns to 2 digits
        for ch in ['CARGAHORARIAAMBULATORIO', 'CARGAHORARIAHOSPITAL', 'CARGAHORARIATOTAL']:
            if ch in df_result.columns:
                df_result[ch] = df_result[ch].astype(int).astype(str).str.zfill(2)

        # Final Rename for XML Standard
        rename_map = {
            'CNS': 'CNS',
            'CPF': 'CPF',
            'CNES': 'CNES',
            'MATRICULA': 'Matricula',
            'VINCULO': 'Vinculo',
            'OCUPACAO': 'Ocupacao',
            'CARGAHORARIAAMBULATORIO': 'CargaHorariaAmbulatorio',
            'CARGAHORARIAHOSPITAL': 'CargaHorariaHospital',
            'CARGAHORARIATOTAL': 'CargaHorariaTotal'
        }
        df_result = df_result.rename(columns=rename_map)

        # --- Loading to SQLite ---
        print(f"Salvando {len(df_result)} registros em {output_db}...")
        
        os.makedirs(os.path.dirname(output_db), exist_ok=True)
        
        conn_sqlite = sqlite3.connect(output_db)
        df_result.to_sql('layout_11_2', conn_sqlite, if_exists='replace', index=False)
        conn_sqlite.close()
        
        print("ETL (11.2) concluído com sucesso.")
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
    parser.add_argument('--mapping', required=True)
    
    args = parser.parse_args()
    
    success = transform_and_load(args.dsn, args.user, args.password, args.output, args.mapping)
    if not success:
        sys.exit(1)
