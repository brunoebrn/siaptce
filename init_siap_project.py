import os

def init_siap_project():
    # Estrutura modular para portabilidade
    folders = [
        'drivers', 'data/input', 'data/sqlite', 'output/xml',
        'src/ingestion', 'src/transformation', 'src/serializer', 'src/ui'
    ]
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
    
    # Dependências críticas para Firebird 1.5 e XML SIAP
    requirements = [
        "pandas", "fdb", "lxml", "sqlalchemy", "streamlit", "openpyxl"
    ]
    with open('requirements.txt', 'w') as f:
        f.write("\n".join(requirements))
    
    print("Ambiente configurado. Coloque a fbclient.dll (v1.5) na pasta /drivers.")

if __name__ == "__main__":
    init_siap_project()
