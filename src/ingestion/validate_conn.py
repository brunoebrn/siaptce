import argparse
import sys
import os
import json

# Ensure we can import from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.ingestion.connector import FirebirdConnector

def validate(path, user, password):
    result = {
        "success": False,
        "error": None
    }
    
    try:
        # Clean path just in case
        path = path.strip().strip('"')
        
        if not os.path.exists(path):
            result["error"] = f"Arquivo não encontrado: {path}"
            return result
            
        connector = FirebirdConnector(dsn=path, user=user, password=password)
        connector.connect()
        connector.close()
        
        result["success"] = True
        return result
        
    except Exception as e:
        result["error"] = str(e)
        return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    parser.add_argument("--user", default="SYSDBA")
    parser.add_argument("--password", default="masterkey")
    
    args = parser.parse_args()
    
    # Run validation
    res = validate(args.path, args.user, args.password)
    
    # Print JSON to stdout for the caller to parse
    print(json.dumps(res))
