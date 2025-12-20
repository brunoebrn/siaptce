import argparse
import sys
import os
import json
import pandas as pd

# Adicionar root ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.ingestion.connector import FirebirdConnector

def run_ingestion(dsn, user, password, output_file, table=None, full_export=False):
    if table:
        mode = "FULL" if full_export else "PREVIEW (Limit 50)"
        print(f"Iniciando consulta (32-bit) na tabela: {table} [{mode}] ...")
    else:
        print(f"Iniciando listagem de tabelas (32-bit)...")

    try:
        connector = FirebirdConnector(dsn, user, password)
        connector.connect()
        
        if table:
            # Query specific table data 
            limit_clause = "" if full_export else "FIRST 50"
            query = f"SELECT {limit_clause} * FROM {table}"
        else:
            # Query schema for table list
            query = "SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG=0"
        
        # Now expects (data, columns) tuple
        results, columns = connector.execute_query(query)
        
        connector.close()
        
        # Convert to DataFrame
        if not table:
             df = pd.DataFrame(results, columns=['TableName'])
             # Clean whitespace often found in Firebird char fields
             df['TableName'] = df['TableName'].apply(lambda x: x.strip() if isinstance(x, str) else x)
             df = df.sort_values('TableName')
        else:
             # Use the columns retrieved from cursor
             df = pd.DataFrame(results, columns=columns)

        df.to_json(output_file, orient='records')
        print(f"Sucesso. {len(df)} registros salvos em {output_file}")

    except Exception as e:
        print(f"Erro na ingestão: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Worker de Ingestão Firebird (32-bit)')
    parser.add_argument('--dsn', required=True, help='Data Source Name')
    parser.add_argument('--user', required=True, help='Database User')
    parser.add_argument('--password', required=True, help='Database Password')
    parser.add_argument('--output', required=True, help='Output JSON file path')
    parser.add_argument('--table', help='Table to query (optional)')
    parser.add_argument('--full', action='store_true', help='Export full table (no limit)')
    
    args = parser.parse_args()
    
    run_ingestion(args.dsn, args.user, args.password, args.output, args.table, args.full)
