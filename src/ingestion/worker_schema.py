import sys
import os
import json
import argparse

# Add root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.ingestion.connector import FirebirdConnector

def get_schema(dsn, user, password):
    try:
        # Padrão Context Manager (Ponto 4 do plano)
        with FirebirdConnector(dsn, user, password) as connector:
            # Query for all non-system tables
            query_tables = "SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG=0 AND RDB$VIEW_BLR IS NULL"
            tables_raw, _ = connector.execute_query(query_tables)
            tables = [t[0].strip() for t in tables_raw]
            
            schema = {}
            for table in tables:
                # Query for columns of each table
                query_cols = f"SELECT RDB$FIELD_NAME FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = '{table}' ORDER BY RDB$FIELD_POSITION"
                cols_raw, _ = connector.execute_query(query_cols)
                schema[table] = [c[0].strip() for c in cols_raw]
                
            return {"success": True, "schema": schema}
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dsn', required=True)
    parser.add_argument('--user', default='SYSDBA')
    parser.add_argument('--password', default='masterkey')
    args = parser.parse_args()
    
    result = get_schema(args.dsn, args.user, args.password)
    print(json.dumps(result))
