import sys

def check_imports():
    try:
        import pandas
        print("Pandas version:", pandas.__version__)
        import fdb
        print("fdb version:", fdb.__version__)
        import lxml
        print("lxml imported successfully")
        import sqlalchemy
        print("SQLAlchemy version:", sqlalchemy.__version__)
        import openpyxl
        print("openpyxl version:", openpyxl.__version__)
        
        # Check internal modules
        sys.path.append('c:/siaptce')
        from src.ingestion.connector import FirebirdConnector
        from src.transformation.processor import DataProcessor
        from src.serializer.xml_generator import XMLSerializer
        print("Internal modules imported successfully")
        
    except ImportError as e:
        print(f"Import Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    check_imports()
