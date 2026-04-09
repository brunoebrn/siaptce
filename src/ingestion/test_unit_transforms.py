import pandas as pd
import sys
import os

# Adicionar root ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.ingestion.layout_configs import transform_11_1, transform_11_2, clean_numeric

def test_numeric_cleaner():
    print("--- Testando Formatador Numérico (clean_numeric) ---")
    test_cases = [
        ("123.456.789-00", 11, "12345678900"), # CPF com máscara
        ("12.345.678/0001-99", 14, "12345678000199"), # CNPJ com máscara
        ("abc123def", 6, "000123"), # Texto com números e padding
        (None, 7, ""), # Nulo
        ("", 7, ""), # Vazio
        (12345, 7, "0012345"), # Inteiro com padding
        (123.0, 5, "00123"), # Float com .0
    ]
    
    for val, length, expected in test_cases:
        result = clean_numeric(val, length)
        status = "PASS" if result == expected else "FAIL"
        print(f"[{status}] Input: {val} | Expected: {expected} | Got: {result}")

def test_layout_11_1_logic():
    print("\n--- Testando Regras de Negócio Layout 11.1 ---")
    data = {
        'CNES': ['123', None, '78912345'],
        'CNPJ': ['12.345.678/0001-99', '', '999'],
        'ATIVIDADEPRINCIPAL': ['01-Hospital', '2', None]
    }
    df = pd.DataFrame(data)
    df_proc = transform_11_1(df)
    
    # Validações
    print(f"[OK] CNES Padding: {df_proc['CNES'].tolist()} (Esperado: 7 dígitos)")
    print(f"[OK] CNPJ Cleaning: {df_proc['CNPJ'].tolist()} (Esperado: 14 dígitos)")
    print(f"[OK] Atividade Cleaning: {df_proc['AtividadePrincipal'].tolist()} (Esperado: 2 dígitos)")
    print(f"[OK] Sistema SUS fixo: {df_proc['SistemaSUS'].unique()} (Esperado: [1])")

if __name__ == "__main__":
    test_numeric_cleaner()
    test_layout_11_1_logic()
