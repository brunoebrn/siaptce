# Guia de Distribuição e Instalação - SIAP

## Visão Geral
Este guia descreve como preparar e instalar o sistema SIAP em qualquer máquina Windows.

## Pré-requisitos
O sistema requer:
1. **Python 3.11 (32-bit)** instalado ou disponível de forma portável.
   - **Nota Importante**: A versão 32-bit é OBRIGATÓRIA devido às bibliotecas do Firebird (fdb) que dependem de DLLs legadas do sistema TCE.

## Como Distribuir (Arquitetura Híbrida)
O sistema usa dois ambientes Python para funcionar: um moderno (64-bit) para a interface e um legado (32-bit) para o banco de dados.

1. **Baixe os Ambientes**:
   - Acesse [python.org/downloads/windows](https://www.python.org/downloads/windows/)
   - **Para a Interface**: Baixe "Windows embeddable package (64-bit)" da versão 3.11.x.
   - **Para o Banco**: Baixe "Windows embeddable package (32-bit)" da versão 3.11.x.

2. **Prepare as Pastas**:
   - Extraia o **64-bit** para uma pasta chamada `python_embed`.
   - Extraia o **32-bit** para uma pasta chamada `python_worker`.
   - **Importante**: Em AMBAS as pastas, abra o arquivo `._pth` e descomente `import site`.

3. **Preparar Dependências (Crucial)**:
   - Baixe o `get-pip.py` e coloque uma cópia em cada pasta (`python_embed` e `python_worker`).
   - **No ambiente 64-bit (Interface)**:
     - Abra o terminal em `python_embed` e rode:
       `python.exe -I get-pip.py`
       `python.exe -I -m pip install -r ..\requirements.txt`
   - **No ambiente 32-bit (Banco)**:
     - Abra o terminal em `python_worker` e rode:
       `python.exe -I get-pip.py`
       `python.exe -I -m pip install -r ..\requirements-ingestion.txt`
     *(Nota: Se der erro de certificado SSL, tente sem o -I, mas o ideal é usar para isolamento)*

4. **Compactar**:
   - Compacte a pasta raiz, incluindo `src`, `python_embed`, `python_worker`, `requirements*.txt` e o `.bat`.
   
   **Evite incluir**:
   - `.venv` (Será criada automaticamente)
   - `__pycache__` ou `.git`

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
## Solução de Problemas Comuns
- **Erro "Python não encontrado"**:
  - Se estiver usando o modo Portátil, verifique se a pasta se chama exatamente `python_embed` e contém o `python.exe`.
  - Se estiver usando o Python do sistema, verifique se ele é a versão 3.11 (32-bit) e se o comando `python` responde no terminal.
  - Você pode editar `iniciar_sistema.bat` para forçar um caminho específico se necessário.
- **Erro de Conexão Firebird**: Certifique-se de que a máquina possui as DLLs cliente do Firebird instaladas ou que o Python 32-bit está sendo utilizado corretamente. O sistema foi configurado para usar o interpretador ativo para garantir compatibilidade.
