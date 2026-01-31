@echo off
setlocal EnableDelayedExpansion

:: Define Project Root
set "PROJECT_ROOT=%~dp0"
cd /d "%PROJECT_ROOT%"

:: Config - Log File
set "LOG_FILE=%PROJECT_ROOT%startup_log.txt"

:: Helper to log
call :Log "========================================================"
call :Log "      SIAP - Sistema Integrado de Auditoria Publica"
call :Log "            Modo Portatil (Auto-Configuravel)"
call :Log "========================================================"
call :Log "Data/Hora: %DATE% %TIME%"

:: Define paths
set "PYTHON_EMBED=%PROJECT_ROOT%python_embed"
set "PYTHON_WORKER=%PROJECT_ROOT%python_worker"

:: 1. Validate Existance of Python Folders
if not exist "%PYTHON_EMBED%\python.exe" (
    call :Log "[ERRO CRITICO] Pasta 'python_embed' nao encontrada!"
    pause
    exit /b 1
)
if not exist "%PYTHON_WORKER%\python.exe" (
    call :Log "[ERRO CRITICO] Pasta 'python_worker' nao encontrada!"
    pause
    exit /b 1
)

:: 2. Configure Environments (Ensure 'import site' is enabled)
call :Log "[SETUP] Verificando configuracao do ambiente Python..."
call :EnableSitePackages "%PYTHON_EMBED%"
call :EnableSitePackages "%PYTHON_WORKER%"

:: 3. Check and Install Dependencies for UI (Embed)
call :Log "[CHECK] Verificando dependencias da Interface (Streamlit)..."
call :CheckAndInstall "%PYTHON_EMBED%\python.exe" "streamlit" "%PROJECT_ROOT%requirements.txt"
if %errorlevel% neq 0 exit /b 1

:: 4. Check and Install Dependencies for Worker (DB)
call :Log "[CHECK] Verificando dependencias do Banco (FDB/Pandas)..."
call :CheckAndInstall "%PYTHON_WORKER%\python.exe" "fdb" "%PROJECT_ROOT%requirements-ingestion.txt"
if %errorlevel% neq 0 exit /b 1

:: 5. Run App
call :Log "[RUN] Iniciando Aplicacao..."
"%PYTHON_EMBED%\python.exe" -I -m streamlit run src/ui/app.py >> "%LOG_FILE%" 2>&1

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

:: ----------------------------------------------------------------------
:: Function: EnableSitePackages
:: Uncomment 'import site' in ._pth file to allow pip/site-packages
:: ----------------------------------------------------------------------
:EnableSitePackages
set "PY_DIR=%~1"
pushd "%PY_DIR%"
for %%f in (*._pth) do (
    set "PTH_FILE=%%f"
)
if not defined PTH_FILE (
    popd
    exit /b 0
)

:: Check if already Uncommented
findstr /C:"import site" "%PTH_FILE%" >nul
if %errorlevel% equ 0 (
    :: Already explicitly there, ensure it's not commented
    powershell -Command "(Get-Content '%PTH_FILE%') -replace '#import site', 'import site' | Set-Content '%PTH_FILE%'"
)
popd
exit /b 0

:: ----------------------------------------------------------------------
:: Function: CheckPackage
:: Returns 0 if package importable, 1 otherwise
:: ----------------------------------------------------------------------
:CheckPackage
"%~1" -c "import %~2" >nul 2>&1
exit /b %errorlevel%

:: ----------------------------------------------------------------------
:: Function: CheckAndInstall
:: Args: PathToPython, TestModule, RequirementsFile
:: ----------------------------------------------------------------------
:CheckAndInstall
set "PY_EXE=%~1"
set "TEST_MOD=%~2"
set "REQ_FILE=%~3"

"%PY_EXE%" -c "import %TEST_MOD%" >nul 2>&1
if %errorlevel% equ 0 (
    call :Log "[OK] Modulo '%TEST_MOD%' encontrado."
    exit /b 0
)

call :Log "[WARN] Modulo '%TEST_MOD%' nao encontrado. Tentando corrigir..."
call :Log "       (Isso requer conexao com a internet)"

:: Check PIP
"%PY_EXE%" -m pip --version >nul 2>&1
if %errorlevel% neq 0 (
    call :Log "[SETUP] PIP nao encontrado. Baixando e instalando PIP..."
    if not exist "%PROJECT_ROOT%get-pip.py" (
        powershell -Command "Invoke-WebRequest -Uri https://bootstrap.pypa.io/get-pip.py -OutFile '%PROJECT_ROOT%get-pip.py'"
    )
    "%PY_EXE%" "%PROJECT_ROOT%get-pip.py" --no-warn-script-location
    if !errorlevel! neq 0 (
        call :Log "[ERRO] Falha ao instalar PIP. Verifique sua internet."
        pause
        exit /b 1
    )
    del "%PROJECT_ROOT%get-pip.py"
)

:: Install Requirements
call :Log "[SETUP] Instalando bibliotecas de %REQ_FILE%..."
"%PY_EXE%" -m pip install -r "%REQ_FILE%" >> "%LOG_FILE%" 2>&1
if %errorlevel% neq 0 (
    call :Log "[ERRO] Falha ao instalar dependencias. Verifique o log."
    pause
    exit /b 1
)

:: Re-verify
"%PY_EXE%" -c "import %TEST_MOD%" >nul 2>&1
if %errorlevel% equ 0 (
    call :Log "[OK] Instalacao concluida com sucesso!"
    exit /b 0
) else (
    call :Log "[ERRO] Ainda nao foi possivel importar '%TEST_MOD%' apos instalacao."
    pause
    exit /b 1
)
exit /b 0

:: ----------------------------------------------------------------------
:: Function: Log
:: ----------------------------------------------------------------------
:Log
echo %~1
echo %~1 >> "%LOG_FILE%"
exit /b 0
