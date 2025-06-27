@echo off

git checkout main

git pull origin main

".\venv\Scripts\python.exe" .\main.py

".\venv\Scripts\python.exe" .\write_tabulator_rows.py

".\venv\Scripts\python.exe" .\service_maintenance.py

".\venv\Scripts\python.exe" .\check_logs.py

git add .\docs\input_json\

REM Re-using commit with message "Pushing updated json for Tabulator JS"
REM Doing this so interactive rebase can be used to clean redundant commit history
REM Pre rebase commit: 800fd10b
REM After rebase, the "Pushing updated json for Tabulator JS" commit will have a new hash. Paste below to continue fixups.
git commit --fixup 5779876f

git push origin main

exit /b