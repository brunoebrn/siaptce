import sys
import os
import sqlite3
import pandas as pd
import json
import argparse

# Add root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.ingestion.connector import FirebirdConnector
from src.ingestion.layout_configs import LAYOUT_CONFIGS
from src.utils.logger import setup_logger

def extract_data(dsn, user, password, sql, logger):
    """Fase E (Extract): Extração pura do Firebird."""
    logger.info(f"Conectando ao banco Firebird para extração...")
    with FirebirdConnector(dsn, user, password) as conn_fb:
        data, columns = conn_fb.execute_query(sql)
        return pd.DataFrame(data, columns=columns)

def transform_data(df, layout_id, logger):
    """Fase T (Transform): Transformação pura dos dados."""
    if layout_id not in LAYOUT_CONFIGS:
        raise ValueError(f"Configuração de transformação não encontrada para {layout_id}")
    
    transformer = LAYOUT_CONFIGS[layout_id]['transformer']
    logger.info(f"Aplicando transformações para {layout_id}...")
    return transformer(df)

def load_data(df, output_db, layout_id, logger):
    """Fase L (Load): Carga pura no SQLite."""
    table_name = f"layout_{layout_id.replace('.', '_')}"
    logger.info(f"Salvando {len(df)} registros em {output_db} [Tabela: {table_name}]...")
    
    os.makedirs(os.path.dirname(output_db), exist_ok=True)
    with sqlite3.connect(output_db) as conn_sqlite:
        df.to_sql(table_name, conn_sqlite, if_exists='replace', index=False)

def run_etl(layout_id, dsn, user, password, output_db, mapping_json, competencia=None):
    logger = setup_logger(f"Worker_{layout_id.replace('.', '_')}", log_to_file=False)
    
    try:
        config = LAYOUT_CONFIGS.get(layout_id)
        if not config:
            logger.error(f"Configuração não encontrada para layout {layout_id}")
            return False

        mapping = json.loads(mapping_json)
        main_table = mapping.get('table_main')
        cols_map = mapping.get('columns_main', {})
        
        # 1. Preparar Query
        select_parts = []
        for target, source in cols_map.items():
            select_parts.append(f"{source} as {target}" if source else f"NULL as {target}")
        query_cols = ", ".join(select_parts) if select_parts else "*"
        sql = config['query_builder'](main_table, query_cols, competencia)

        # 2. Executar Pipeline E-T-L
        df_raw = extract_data(dsn, user, password, sql, logger)
        df_processed = transform_data(df_raw, layout_id, logger)
        load_data(df_processed, output_db, layout_id, logger)
            
        logger.info(f"Pipeline ETL {layout_id} concluído com sucesso.")
        return True

    except Exception as e:
        logger.error(f"Falha no Pipeline ETL {layout_id}: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='Caminho do arquivo JSON de configuração')
    
    # Suporte legado (fallback se alguém chamar manualmente)
    parser.add_argument('--layout', help='ID do Layout (ex: 11.1)')
    parser.add_argument('--dsn')
    parser.add_argument('--user', default='SYSDBA')
    parser.add_argument('--password', default='masterkey')
    parser.add_argument('--output')
    parser.add_argument('--mapping')
    parser.add_argument('--competencia', help='Competencia AAAAMM')
    
    args = parser.parse_args()
    
    if args.config:
        # Modo Seguro: Lê do arquivo
        with open(args.config, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        success = run_etl(
            layout_id=config['layout_id'],
            dsn=config['dsn'],
            user=config['user'],
            password=config['password'],
            output_db=config['output_db'],
            mapping_json=json.dumps(config['mapping']),
            competencia=config.get('competencia')
        )
    else:
        # Modo Legado: Lê da CLI
        if not args.layout:
            print("Erro: Deve fornecer --config ou --layout.")
            sys.exit(1)
            
        success = run_etl(args.layout, args.dsn, args.user, args.password, args.output, args.mapping, args.competencia)
    
    if not success:
        sys.exit(1)
