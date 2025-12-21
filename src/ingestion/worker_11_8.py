import fdb
import pandas as pd
import sqlite3
import os
import sys
import json
from connector import FirebirdConnector
from datetime import datetime

def transform_and_load(dsn, user, password, output_db, mapping_json, competencia=None):
    try:
        mapping = json.loads(mapping_json)
        
        # Layout 11.8: Autorizacao Internacao Hospitalar
        # Table: TB_HAIH (SIH Database)
        
        main_table = "TB_HAIH"
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
        
        # Filter by Competence (AH_CMPT)
        if competencia:
            print(f"Filtrando por competência (AH_CMPT): {competencia}")
            sql += f" WHERE AH_CMPT = '{competencia}'"
        
        print(f"SQL Gerado: {sql}")
        
        data_main, cols_main = conn_fb.execute_query(sql)
        df_result = pd.DataFrame(data_main, columns=cols_main)
        
        conn_fb.close()
        
        # --- Transformations 11.8 ---
        print("Transformando dados (Layout 11.8)...")
        # Ensure upper columns for processing
        df_result.columns = [c.upper() for c in df_result.columns]

        # Formatting Helpers
        def format_date(val):
            # Firebird dates might be datetime objects or strings like '2024-05-30'
            if not val or pd.isna(val): return ""
            try:
                if isinstance(val, (pd.Timestamp, datetime)):
                    return val.strftime('%Y-%m-%d')
                
                # If string, try parsing common formats
                val_str = str(val).split()[0] # Remove time if present
                # Assuming standard ISO or YMD. 
                # If already YYYY-MM-DD, just return.
                return val_str
            except:
                return str(val)

        def clean_number(val, length=0):
            if pd.isna(val): return ""
            s = str(val).replace('.0', '') # Remove float suffix if integer
            s = "".join(filter(str.isdigit, s))
            if length > 0:
                s = s.zfill(length)
            return s


        # Apply Transformations
        
        # Dates
        for date_col in ['DATAEMISSAO', 'DATAINTERNACAO', 'DATASAIDA']:
            if date_col in df_result.columns:
                df_result[date_col] = df_result[date_col].apply(format_date)

        # IDs / Numeric Strings
        # CNES (7)
        if 'CNES' in df_result.columns:
            df_result['CNES'] = df_result['CNES'].apply(lambda x: clean_number(x, 7))

        # NumeroAIH (13)
        if 'NUMEROAIH' in df_result.columns:
             df_result['NUMEROAIH'] = df_result['NUMEROAIH'].apply(lambda x: clean_number(x, 13))

        # AIHAnterior (13) - Optional? Manual says NO mandatory, but format is 13.
        if 'AIHANTERIOR' in df_result.columns:
             df_result['AIHANTERIOR'] = df_result['AIHANTERIOR'].apply(lambda x: clean_number(x, 13) if x else "0000000000000") # Default zero if empty?

        # Identificacao (2)
        if 'IDENTIFICACAO' in df_result.columns:
             df_result['IDENTIFICACAO'] = df_result['IDENTIFICACAO'].apply(lambda x: clean_number(x, 2))

        # CNS Fields (15)
        for cns_col in ['CNSSOLICITANTE', 'CNSRESPONSAVEL', 'CNSAUTORIZADOR', 'CNSPACIENTE']:
             if cns_col in df_result.columns:
                  df_result[cns_col] = df_result[cns_col].apply(lambda x: clean_number(x, 15))


        # Final Rename for XML Standard (Strict MixedCase)
        rename_map = {
            'CNES': 'CNES',
            'NUMEROAIH': 'NumeroAIH',
            'IDENTIFICACAO': 'Identificacao',
            'ESPECIALIDADELEITO': 'EspecialidadeLeito',
            'MODALIDADEINTERNACAO': 'ModalidadeInternacao',
            'AIHANTERIOR': 'AIHAnterior',
            'DATAEMISSAO': 'DataEmissao',
            'DATAINTERNACAO': 'DataInternacao',
            'DATASAIDA': 'DataSaida',
            'PROCEDIMENTOSOLICITADO': 'ProcedimentoSolicitado',
            'CARATERINTERNACAO': 'CaraterInternacao',
            'MOTIVOSAIDA': 'MotivoSaida',
            'CNSSOLICITANTE': 'CNSSolicitante',
            'CNSRESPONSAVEL': 'CNSResponsavel',
            'CNSAUTORIZADOR': 'CNSAutorizador',
            'DIAGNOSTICOPRINCIPAL': 'DiagnosticoPrincipal',
            'CNSPACIENTE': 'CNSPaciente'
        }
        df_result = df_result.rename(columns=rename_map)
        
        # --- Loading to SQLite ---
        print(f"Salvando {len(df_result)} registros em {output_db}...")
        
        os.makedirs(os.path.dirname(output_db), exist_ok=True)
        
        conn_sqlite = sqlite3.connect(output_db)
        df_result.to_sql('layout_11_8', conn_sqlite, if_exists='replace', index=False)
        conn_sqlite.close()
        
        print("ETL (11.8) concluído com sucesso.")
        return True

    except Exception as e:
        print(f"Erro no ETL: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Worker Process for Layout 11.8')
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
