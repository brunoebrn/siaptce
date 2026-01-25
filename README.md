# SIAP - Sistema Integrado de Auditoria Pública

Ferramenta automatizada para ingestão, análise e transformação de dados de saúde (CNES, FPO, SIA, SIH) para auditoria e geração de layouts do TCE (Tribunal de Contas do Estado).

## 🎯 Objetivo
Facilitar o trabalho de auditores e técnicos, permitindo a extração de dados de bancos **Firebird legado (32-bit)** (comuns em sistemas de saúde pública) e transformando-os em formatos modernos (SQLite, Excel) e layouts XML padronizados, tudo através de uma interface visual amigável.

## 🚀 Como Funciona (Arquitetura Híbrida)
Este projeto resolve um desafio técnico complexo: conectar interfaces modernas (que exigem 64-bit) com bancos de dados antigos (que exigem drivers 32-bit chumbados no sistema).

A solução é **100% Portátil e Híbrida**:
- **Interface (UI)**: Roda em um Python **64-bit** embutido (`python_embed`). Usa **Streamlit** para gráficos e interatividade rápida.
- **Motor (Worker)**: Roda em um Python **32-bit** isolado (`python_worker`). Conecta-se aos arquivos `.GDB/.FDB` usando drivers oficiais Firebird.

**Resultado**: Você roda em qualquer Windows moderno sem precisar instalar nada, nem configurar drivers complexos.

---

## 📦 Como Usar (Tutorial)

### 1. Instalação
Não há instalação! O sistema é "Portable".
1. **Baixe** o repositório completo (Arquivo ZIP).
2. **Extraia** para uma pasta (Ex: `C:\SIAP`).
   - *Nota: Evite caminhos muito longos ou com espaços.*

### 2. Execução
1. Abra a pasta do projeto.
2. Dê um **duplo clique** no arquivo:
   ▶️ `iniciar_sistema.bat`
3. Uma tela preta abrirá (é o servidor de logs) e, em seguida, o sistema abrirá automaticamente no seu **Navegador**.

### 3. Funcionalidades
A interface possui um passo-a-passo lateral:
1. **Setup**: Selecione onde estão os arquivos de banco de dados (`CNES.GDB`, `SIH.GDB`, etc.). O sistema valida a conexão na hora.
2. **Auditoria (Layouts)**: Escolha o layout desejado (Ex: Layout 11.1 - Estabelecimentos).
   - O sistema fará o mapeamento automático das colunas.
   - Clique em **Validar e Converter**.
3. **DB Explorer**: Uma ferramenta para "espiar" as tabelas do banco de dados bruto, útil para tirar dúvidas sem precisar de ferramentas de TI.

### 4. Encerrando
Para fechar, clique no botão **❌ Encerrar Sistema** na barra lateral. Isso garante que todas as conexões com o banco sejam fechadas com segurança.

---

## 🛠️ Solução de Problemas

- **A janela preta fecha sozinha**: Verifique se você extraiu todas as pastas (`python_embed`, `python_worker`). O sistema precisa delas para funcionar.
- **Erro de Conexão com Banco**: Verifique se o arquivo `.GDB` não está sendo usado por outro programa.
- **Logs**: Se algo der errado e não aparecer mensagem na tela, verifique o arquivo `logs/siaptce.log`. Ele contém o "diário de bordo" completo do sistema.

---
**Desenvolvido para automação e conformidade.**
*Não requer instalação de Python ou Drivers no computador do usuário.*
