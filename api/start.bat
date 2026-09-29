@echo off
REM Start the GutBiomeDB API server
REM 启动微生物组图谱API服务器
cd /d "%~dp0"
E:\microbiomap_env\Scripts\python.exe -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload