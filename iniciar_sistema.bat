@echo off
setlocal

:: Define Project Root
set "PROJECT_ROOT=%~dp0"
cd /d "%PROJECT_ROOT%"

:: Config - Log File
set "LOG_FILE=%PROJECT_ROOT%startup_log.txt"

:: Helper to log and print
call :Log "========================================================"
call :Log "      SIAP - Sistema Integrado de Auditoria Publica"
call :Log "            Modo Portatil (Hibrido Check)"
call :Log "========================================================"
call :Log "Data/Hora: %DATE% %TIME%"

:: Define paths
set "PYTHON_EMBED=%PROJECT_ROOT%python_embed\python.exe"
set "PYTHON_WORKER=%PROJECT_ROOT%python_worker\python.exe"

:: 1. Validate Environment
if not exist "%PYTHON_EMBED%" (
    call :Log "[ERRO CRITICO] Ambiente de Interface nao encontrado!"
    call :Log "Esperado em: %PYTHON_EMBED%"
    pause
    exit /b 1
)

if not exist "%PYTHON_WORKER%" (
    call :Log "[ERRO CRITICO] Ambiente de Banco de Dados nao encontrado!"
    call :Log "Esperado em: %PYTHON_WORKER%"
    pause
    exit /b 1
)

:: 2. Verify UI Python
call :Log "[CHECK] Verificando ambiente de Interface..."
"%PYTHON_EMBED%" -I --version >> "%LOG_FILE%" 2>&1
if %errorlevel% neq 0 (
    call :Log "[ERRO] O Python de Interface (python_embed) nao esta respondendo."
    pause
    exit /b 1
) else (
    call :Log "[OK] Python Interface OK."
)

:: 3. Run App
call :Log "[RUN] Iniciando Aplicacao..."
"%PYTHON_EMBED%" -I -m streamlit run src/ui/app.py >> "%LOG_FILE%" 2>&1

if %errorlevel% neq 0 (
    call :Log "[ERRO] A aplicacao encerrou com erro (Codigo: %errorlevel%)."
    echo Verifique o arquivo startup_log.txt para detalhes.
    pause
) else (
    call :Log "[INFO] Aplicacao encerrada pelo usuario."
    pause
)
endlocal
exit /b

:Log
echo %~1
echo %~1 >> "%LOG_FILE%"
exit /b 0
