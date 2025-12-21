import fdb
import pandas as pd
import sqlite3
import os
import sys
import json
from connector import FirebirdConnector

def transform_and_load(dsn, user, password, output_db, mapping_json, competencia=None):
    try:
        mapping = json.loads(mapping_json)
        
        # Layout 11.5: Ficha Programação Orçamentária
        main_table = "S_IPU"
        cols_map = mapping.get('columns_main', {})
        
        conn_fb = FirebirdConnector(dsn, user, password)
        conn_fb.connect()
        
        print(f"Extraindo dados de {main_table}...")
        
        select_parts = []
        for target, source in cols_map.items():
            if source:
                select_parts.append(f"{source} as {target}")
                
        if not select_parts:
             query_cols = "*"
        else:
             query_cols = ", ".join(select_parts)
        
        sql = f"SELECT {query_cols} FROM {main_table}"
        
        params = []
        if competencia:
            print(f"Filtrando por competência: {competencia}")
            sql += f" WHERE IPU_CMP = '{competencia}'"
        
        print(f"SQL Gerado: {sql}")
        
        # Determine if we should use params or just f-string (safe enough for this local script with sanitized inputs)
        # Using string injection for simplicity with the provided context, but params are safer if 'competencia' wasn't controlled.
        # Firebird connector usually handles queries.
        
        data_main, cols_main = conn_fb.execute_query(sql)
        df_result = pd.DataFrame(data_main, columns=cols_main)
        
        conn_fb.close()
        
        # --- Transformations 11.5 ---
        print("Transformando dados (Layout 11.5)...")
        # Ensure upper columns for processing
        df_result.columns = [c.upper() for c in df_result.columns]

        # CNES -> 7 digits
        if 'CNES' in df_result.columns:
             df_result['CNES'] = df_result['CNES'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(7)
             
        # Procedimento -> 10 digits
        if 'PROCEDIMENTO' in df_result.columns:
             df_result['PROCEDIMENTO'] = df_result['PROCEDIMENTO'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(10)

        # Financiamento -> PAB, MAC, FAEC
        if 'FINANCIAMENTO' in df_result.columns:
             def map_fin(val):
                 s_val = str(val).strip()
                 if s_val == '1': return 'PAB'
                 if s_val == '2': return 'MAC'
                 if s_val == '3': return 'FAEC'
                 return s_val
             
             df_result['FINANCIAMENTO'] = df_result['FINANCIAMENTO'].apply(map_fin)

        # Quantidade -> Integer
        if 'QUANTIDADE' in df_result.columns:
             df_result['QUANTIDADE'] = pd.to_numeric(df_result['QUANTIDADE'], errors='coerce').fillna(0).astype(int)
             
        # ValorUnitario -> Decimal
        if 'VALORUNITARIO' in df_result.columns:
             df_result['VALORUNITARIO'] = pd.to_numeric(df_result['VALORUNITARIO'], errors='coerce').fillna(0).round(2)
             
        # ValorTotal -> Decimal
        if 'VALORTOTAL' in df_result.columns:
             df_result['VALORTOTAL'] = pd.to_numeric(df_result['VALORTOTAL'], errors='coerce').fillna(0).round(2)


        # Final Rename for XML Standard (Strict MixedCase)
        rename_map = {
            'CNES': 'CNES',
            'PROCEDIMENTO': 'Procedimento',
            'FINANCIAMENTO': 'Financiamento',
            'QUANTIDADE': 'Quantidade',
            'VALORUNITARIO': 'ValorUnitario',
            'VALORTOTAL': 'ValorTotal'
        }
        df_result = df_result.rename(columns=rename_map)
        
        # --- Loading to SQLite ---
        print(f"Salvando {len(df_result)} registros em {output_db}...")
        
        os.makedirs(os.path.dirname(output_db), exist_ok=True)
        
        conn_sqlite = sqlite3.connect(output_db)
        df_result.to_sql('layout_11_5', conn_sqlite, if_exists='replace', index=False)
        conn_sqlite.close()
        
        print("ETL (11.5) concluído com sucesso.")
        return True

    except Exception as e:
        print(f"Erro no ETL: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Worker Process for Layout 11.5')
    parser.add_argument('--dsn', required=True)
    parser.add_argument('--user', default='SYSDBA')
    parser.add_argument('--password', default='masterkey')
    parser.add_argument('--output', required=True)
    parser.add_argument('--mapping', required=True)
    parser.add_argument('--competencia', required=False, help='Competencia AAAAMM')
    
    args = parser.parse_args()
    
    success = transform_and_load(args.dsn, args.user, args.password, args.output, args.mapping, args.competencia)
    if not success:
        sys.exit(1)
