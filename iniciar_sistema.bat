@echo off
setlocal

:: Define Project Root
set "PROJECT_ROOT=%~dp0"
cd /d "%PROJECT_ROOT%"

:: Config
set "VENV_DIR=.venv"
set "PYTHON_CMD=python"

:: Banner
echo ========================================================
echo       SIAP - Sistema Integrado de Auditoria Publica
echo ========================================================

:: 1. Check for Python
%PYTHON_CMD% --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERRO] Python nao encontrado!
    echo Por favor, instale o Python 3.11 (32-bit).
    echo Se voce baixou o pacote Portavel, certifique-se de configurar o PATH ou editar este script.
    pause
    exit /b 1
)

:: 2. Check/Create VENV
if not exist "%VENV_DIR%" (
    echo [SETUP] Criando ambiente virtual em '%VENV_DIR%'...
    %PYTHON_CMD% -m venv "%VENV_DIR%"
    if %errorlevel% neq 0 (
        echo [ERRO] Falha ao criar ambiente virtual.
        pause
        exit /b 1
    )
)

:: 3. Activate VENV
echo [SETUP] Ativando ambiente virtual...
call "%VENV_DIR%\Scripts\activate.bat"
if %errorlevel% neq 0 (
    echo [ERRO] Falha ao ativar ambiente virtual.
    pause
    exit /b 1
)

:: 4. Install Dependencies
if exist "requirements.txt" (
    echo [SETUP] Verificando dependencias...
    pip install -r requirements.txt
    if %errorlevel% neq 0 (
        echo [ERRO] Falha ao instalar dependencias.
        pause
        exit /b 1
    )
) else (
    echo [AVISO] Arquivo requirements.txt nao encontrado.
)

:: 5. Run App
echo.
echo [RUN] Iniciando Aplicacao...
echo.
streamlit run src/ui/app.py

pause
