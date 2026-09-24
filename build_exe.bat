@echo off
chcp 65001 > nul
echo =======================================================
echo    GERADOR DE EXECUTAVEL - ETL RF BASE BRONZE
echo =======================================================
echo.

python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [ERRO] O Python nao foi encontrado no seu sistema.
    echo Por favor, instale o Python atraves da Microsoft Store ou pelo site python.org.
    echo Durante a instalacao, certifique-se de marcar a opcao "Add Python to PATH".
    echo.
    pause
    exit /b
)

echo [1/3] Instalando as dependencias do projeto...
python -m pip install -r requirements_desktop.txt

echo.
echo [2/3] Construindo o arquivo executavel...
python -m PyInstaller --name "ETL_RF_Bronze" ^
    --add-data "templates;templates" ^
    --add-data "static;static" ^
    --hidden-import "engineio.async_drivers.threading" ^
    --windowed ^
    --onefile ^
    --clean ^
    desktop_main.py

echo.
echo [3/3] Concluido!
echo O seu executavel foi gerado com sucesso e esta disponivel na pasta 'dist'.
echo.
pause
