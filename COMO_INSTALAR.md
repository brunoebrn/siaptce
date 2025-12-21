# Guia de Distribuição e Instalação - SIAP

## Visão Geral
Este guia descreve como preparar e instalar o sistema SIAP em qualquer máquina Windows.

## Pré-requisitos
O sistema requer:
1. **Python 3.11 (32-bit)** instalado ou disponível de forma portável.
   - **Nota Importante**: A versão 32-bit é OBRIGATÓRIA devido às bibliotecas do Firebird (fdb) que dependem de DLLs legadas do sistema TCE.

## Como Distribuir
Para distribuir o software, compacte a pasta raiz do projeto (`c:\siaptce`), INCLUINDO os seguintes arquivos essenciais:
- `src\` (Todo o código fonte)
- `requirements.txt` (Lista de dependências)
- `iniciar_sistema.bat` (Script de automação)

**Evite incluir**:
- A pasta `.venv` (Ela será recriada na máquina do usuário)
- A pasta `__pycache__` ou `.git`
- Arquivos de banco de dados (`.gdb`, `.fdb`) de testes locais, a menos que sirvam de exemplo.

## Como Instalar (Usuário Final)

1. **Baixar e Extrair**: Descompacte o arquivo `.zip` ou copie a pasta do projeto para um local no disco (ex: `C:\Sistemas\SIAP`).
2. **Instalar Python (Se necessário)**:
   - Se a máquina já tiver Python 3.11 (32-bit), pule este passo.
   - Caso contrário, baixe e instale o Python 3.11 (32-bit) do site oficial python.org. Certifique-se de marcar "Add Python to PATH" durante a instalação.
3. **Executar**:
   - Dê um duplo clique no arquivo `iniciar_sistema.bat`.
   - O script irá automaticamente:
     - Criar um ambiente isolado (`.venv`).
     - Baixar e instalar as bibliotecas necessárias (`streamlit`, `pandas`, `fdb`, etc).
     - Abrir o navegador com o sistema rodando.

## Solução de Problemas Comuns
- **Erro "Python não encontrado"**: Edite o arquivo `iniciar_sistema.bat` e, na linha `set "PYTHON_CMD=python"`, coloque o caminho completo para o executável do Python (ex: `set "PYTHON_CMD=C:\Python311-32\python.exe"`).
- **Erro de Conexão Firebird**: Certifique-se de que a máquina possui as DLLs cliente do Firebird instaladas ou que o Python 32-bit está sendo utilizado corretamente. O sistema foi configurado para usar o interpretador ativo para garantir compatibilidade.
