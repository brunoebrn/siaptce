# Guia de Distribuição e Instalação - SIAP

## Visão Geral
Este guia descreve como preparar e instalar o sistema SIAP em qualquer máquina Windows.
O sistema agora possui um mecanismo de **auto-reparação** que instala dependências automaticamente se necessário.

## Pré-requisitos
O sistema requer:
1. **Python 3.11 (32-bit)** instalado ou disponível de forma portável.
2. **Conexão com Internet** (apenas para a primeira execução se estiver usando o modo Compacto).

## Modos de Distribuição

### Modo 1: Compacto (Recomendado para distribuição leve)
Neste modo, o instalador baixa as bibliotecas na primeira execução.
1. Baixe os pacotes Python Embeddable (32-bit e 64-bit) e extraia para `python_worker` e `python_embed`.
2. Inclua apenas o código fonte e o arquivo `iniciar_sistema.bat`.
3. **Tamanho**: ~25MB.
4. **Requisito**: A máquina de destino precisa de Internet na primeira vez.

### Modo 2: Offline Completo (Recomendado para servidores sem internet)
Neste modo, você pré-instala tudo.
1. Execute os passos de "Preparar Dependências" abaixo **antes** de compactar.
2. O arquivo final terá ~400MB+.
3. **Vantagem**: Funciona sem internet.

## Como Preparar (Desenvolvedor)
O sistema usa dois ambientes Python para funcionar: um moderno (64-bit) para a interface e um legado (32-bit) para o banco de dados.

1. **Baixe os Ambientes**:
   - Acesse [python.org/downloads/windows](https://www.python.org/downloads/windows/)
   - **Para a Interface**: Baixe "Windows embeddable package (64-bit)" da versão 3.11.x.
   - **Para o Banco**: Baixe "Windows embeddable package (32-bit)" da versão 3.11.x.

2. **Prepare as Pastas**:
   - Extraia o **64-bit** para uma pasta chamada `python_embed`.
   - Extraia o **32-bit** para uma pasta chamada `python_worker`.
   - *Nota: O script `iniciar_sistema.bat` ajustará o arquivo ._pth automaticamente se necessário.*

3. **(Opcional) Pré-instalar Dependências para Modo Offline**:
   - Se deseja entregar o sistema pronto para funcionar offline, você deve rodar o script `iniciar_sistema.bat` uma vez na sua máquina **antes** de criar o ZIP.
   - Isso preencherá as pastas com as bibliotecas.

4. **Compactar**:
   - Compacte a pasta raiz, incluindo `src`, `python_embed`, `python_worker`, `requirements*.txt` e o `.bat`.
   - Se for o Modo Compacto, pode excluir as pastas `Lib` dentro dos pythons para economizar espaço (mas garanta que os arquivos `python311.zip` e `.exe` permaneçam).

## Como Instalar (Usuário Final)

1. **Baixar e Extrair**: Descompacte o arquivo ou copie a pasta do projeto (ex: `C:\Sistemas\SIAP`).
2. **Executar**:
   - Dê um duplo clique no arquivo `iniciar_sistema.bat`.
   - O script irá automaticamente:
     - Verificar se o Python está presente.
     - Verificar se as bibliotecas (Streamlit, Pandas, FDB) estão instaladas.
     - **Se faltar algo**: Ele tentará baixar e instalar (pode demorar alguns minutos na primeira vez).
     - Abrir o navegador com o sistema rodando.

## Solução de Problemas Comuns
- **Erro "Falha ao instalar PIP"**: Verifique sua conexão com a internet. Se estiver em rede corporativa com proxy, configure as variáveis de ambiente HTTP_PROXY antes de rodar.
- **Erro de DLL ou VCRUNTIME**: Certifique-se de que o "Visual C++ Redistributable" está instalado na máquina (geralmente já vem com o Windows, mas versões antigas podem precisar de atualização).
- **Caminhos Muito Longos**: Extraia o sistema em `C:\SIAP` em vez de pastas profundas na Área de Trabalho para evitar limitações do Windows.
