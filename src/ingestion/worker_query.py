import sys
import os
import json
import argparse
import pandas as pd

# Add root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.ingestion.connector import FirebirdConnector

def get_preview(dsn, user, password, table_name):
    try:
        with FirebirdConnector(dsn, user, password) as connector:
            # Pega as primeiras 50 linhas para preview
            query = f"SELECT FIRST 50 * FROM {table_name}"
            data, columns = connector.execute_query(query)
            
            df = pd.DataFrame(data, columns=columns)
            
            # Limpeza de strings (CHAR fields)
            for col in df.select_dtypes([object]):
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
            
            # --- Correção de Serialização ---
            # Converte colunas de Data/Hora para string ISO
            for col in df.select_dtypes(include=['datetime', 'datetimetz']):
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Converte Decimais/Objetos genéricos (como date simples) para string
            # Isso evita TypeError: Object of type Decimal/date is not JSON serializable
            def robust_serialize(val):
                if isinstance(val, (pd.Timestamp, pd.Series)): return str(val)
                import decimal
                import datetime
                if isinstance(val, (decimal.Decimal, datetime.date, datetime.time)):
                    return str(val)
                return val

            df = df.applymap(robust_serialize)
            
            return {"success": True, "data": df.to_dict(orient='records')}
    except Exception as e:
        import traceback
        return {"success": False, "error": f"{str(e)}\n{traceback.format_exc()}"}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dsn', required=True)
    parser.add_argument('--user', default='SYSDBA')
    parser.add_argument('--password', default='masterkey')
    parser.add_argument('--table', required=True)
    args = parser.parse_args()
    
    result = get_preview(args.dsn, args.user, args.password, args.table)
    print(json.dumps(result))
