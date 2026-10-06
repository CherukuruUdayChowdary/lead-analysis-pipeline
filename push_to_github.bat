@echo off
REM Creates a GitHub repo for this folder and pushes all scripts.
REM Double-click this file, or run it from this folder in a terminal.
cd /d "%~dp0"
set REPO=lead-analysis-pipeline

where git >nul 2>nul || (echo Git is not installed. Get it from https://git-scm.com/download/win & pause & exit /b 1)

if not exist ".git" (
    git init
    git branch -M main
)
git add .
git commit -m "Lead generation pipeline: GMB scraper, dedupe, website checker, URL cleaner, email scraper"

where gh >nul 2>nul
if %errorlevel%==0 (
    gh auth status >nul 2>nul || gh auth login
    gh repo create %REPO% --private --source . --remote origin --push
    echo.
    echo Done. Opening the repo...
    gh repo view %REPO% --web
) else (
    echo.
    echo GitHub CLI not found. Do this instead:
    echo   1. Create an EMPTY private repo named %REPO% at https://github.com/new
    echo   2. Then run:
    echo      git remote add origin https://github.com/YOUR-USERNAME/%REPO%.git
    echo      git push -u origin main
)
pause
