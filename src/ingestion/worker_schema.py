import sys
import os
import json
import collections

# Add root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.ingestion.connector import FirebirdConnector

def get_schema(dsn, user, password):
    try:
        conn = FirebirdConnector(dsn, user, password)
        conn.connect()
        
        # Query to get Tables and Columns
        # RDB$RELATION_FIELDS contains the columns
        # RDB$RELATIONS contains the tables
        query = """
            SELECT TRIM(R.RDB$RELATION_NAME) as TABLE_NAME, TRIM(RF.RDB$FIELD_NAME) as COLUMN_NAME
            FROM RDB$RELATION_FIELDS RF
            JOIN RDB$RELATIONS R ON RF.RDB$RELATION_NAME = R.RDB$RELATION_NAME
            WHERE R.RDB$SYSTEM_FLAG = 0
            ORDER BY 1, RF.RDB$FIELD_POSITION
        """
        
        rows, cols = conn.execute_query(query)
        conn.close()
        
        # Group by Table
        schema = collections.defaultdict(list)
        for row in rows:
            table = row[0]
            col = row[1]
            schema[table].append(col)
            
        return {"success": True, "schema": schema}

    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dsn', required=True)
    parser.add_argument('--user', default='SYSDBA')
    parser.add_argument('--password', default='masterkey')
    
    args = parser.parse_args()
    
    result = get_schema(args.dsn, args.user, args.password)
    print(json.dumps(result))
