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
        
        # Layout 11.4: Equipamentos
        # Tables: LFCES020 (Base - Equipamentos), LFCES004 (Unidades)
        # Joins: T20.UNIDADE_ID = T04.UNIDADE_ID
        
        main_table = "LFCES020"
        cols_map = mapping.get('columns_main', {})
        
        conn_fb = FirebirdConnector(dsn, user, password)
        conn_fb.connect()
        
        print(f"Executando Query Complexa (Joins) em {main_table}...")
        
        select_parts = []
        for target, source in cols_map.items():
            if source:
                select_parts.append(f"{source} as {target}")
                
        if not select_parts:
             query_cols = "*"
        else:
             query_cols = ", ".join(select_parts)
        
        # T20 = LFCES020, T04 = LFCES004
        sql = f"""
        SELECT {query_cols}
        FROM {main_table} T20
        LEFT JOIN LFCES004 T04 ON T20.UNIDADE_ID = T04.UNIDADE_ID
        """
        
        print(f"SQL Gerado: {sql}")
        
        data_main, cols_main = conn_fb.execute_query(sql)
        df_result = pd.DataFrame(data_main, columns=cols_main)
        
        conn_fb.close()
        
        # --- Transformations 11.4 ---
        print("Transformando dados (Layout 11.4)...")
        df_result.columns = [c.upper() for c in df_result.columns]

        # CNES -> 7 digits
        if 'CNES' in df_result.columns:
             df_result['CNES'] = df_result['CNES'].fillna('').astype(str).str.replace(r'\D', '', regex=True).str.zfill(7)
             
        # Codigo -> 6 digits
        if 'CODIGO' in df_result.columns:
             df_result['CODIGO'] = df_result['CODIGO'].fillna(0).astype(int).astype(str).str.zfill(6)
             
        # Tipo -> 2 digits
        if 'TIPO' in df_result.columns:
             df_result['TIPO'] = df_result['TIPO'].fillna(0).astype(int).astype(str).str.zfill(2)

        # Quantidade -> 3 digits
        if 'QUANTIDADE' in df_result.columns:
             df_result['QUANTIDADE'] = df_result['QUANTIDADE'].fillna(0).astype(int).astype(str).str.zfill(3)
             
        # QuantidadeUso -> 3 digits
        if 'QUANTIDADEUSO' in df_result.columns:
             df_result['QUANTIDADEUSO'] = df_result['QUANTIDADEUSO'].fillna(0).astype(int).astype(str).str.zfill(3)

        # Disponibilidade -> 1 digit
        if 'DISPONIBILIDADE' in df_result.columns:
             df_result['DISPONIBILIDADE'] = df_result['DISPONIBILIDADE'].fillna(0).astype(int).astype(str).str.zfill(1)


        # Final Rename for XML Standard
        rename_map = {
            'CNES': 'CNES',
            'CODIGO': 'CodigoEquipamento',
            'TIPO': 'TipoEquipamentoSaude',
            'QUANTIDADE': 'Quantidade',
            'QUANTIDADEUSO': 'QuantidadeUso',
            'DISPONIBILIDADE': 'DisponibilidadeSUS'
        }
        df_result = df_result.rename(columns=rename_map)

        # --- Loading to SQLite ---
        print(f"Salvando {len(df_result)} registros em {output_db}...")
        
        os.makedirs(os.path.dirname(output_db), exist_ok=True)
        
        conn_sqlite = sqlite3.connect(output_db)
        df_result.to_sql('layout_11_4', conn_sqlite, if_exists='replace', index=False)
        conn_sqlite.close()
        
        print("ETL (11.4) concluído com sucesso.")
        return True

    except Exception as e:
        print(f"Erro no ETL: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Worker Process for Layout 11.4')
    parser.add_argument('--dsn', required=True)
    parser.add_argument('--user', default='SYSDBA')
    parser.add_argument('--password', default='masterkey')
    parser.add_argument('--output', required=True)
    parser.add_argument('--mapping', required=True) 
    
    args = parser.parse_args()
    
    success = transform_and_load(args.dsn, args.user, args.password, args.output, args.mapping)
    if not success:
        sys.exit(1)
