import sys
import os
import json
import pandas as pd

# Add root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.ingestion.connector import FirebirdConnector

def get_table_preview(dsn, user, password, table_name, limit=50):
    try:
        conn = FirebirdConnector(dsn, user, password)
        conn.connect()
        
        # Sanitize table name (basic check)
        if not table_name.isidentifier() and not all(c.isalnum() or c == '_' for c in table_name):
             return {"success": False, "error": "Nome de tabela inválido"}
             
        query = f"SELECT FIRST {limit} * FROM {table_name}"
        rows, cols = conn.execute_query(query)
        conn.close()
        
        # Serialize to dict list
        data = [dict(zip(cols, row)) for row in rows]
        
        return {"success": True, "columns": cols, "data": data}

    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dsn', required=True)
    parser.add_argument('--user', default='SYSDBA')
    parser.add_argument('--password', default='masterkey')
    parser.add_argument('--table', required=True)
    
    args = parser.parse_args()
    
    result = get_table_preview(args.dsn, args.user, args.password, args.table)
    # Print JSON to stdout
    print(json.dumps(result, default=str)) # default=str to handle dates etc
