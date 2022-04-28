rem I actually don't know yet how to automate the environment creation/upd
rem These command line notes/ snippets that are executed manually 
rem /****************************************************/
rem /***********activate conda **************************/
C:\ProgramData\Miniconda3\Scripts\activate.bat C:\ProgramData\Miniconda3

rem /***********create the environment ******************/
conda env create --prefix C:\Users\josep\OneDrive\Documentos\projects\UdacityDeng\UDataEng_L03_P02_S3toRedshiftDW\S3toRedshiftDW-env --file environment.yml

rem /***********activate the environment ******************/
conda activate C:\Users\josep\OneDrive\Documentos\projects\UdacityDeng\UDataEng_L03_P02_S3toRedshiftDW\S3toRedshiftDW-env

rem /***********DE - activate the environment ******************/
conda deactivate

rem /***********when requirements need updated environment **********/
conda env update --file environment.yml --prune

rem /*********** remove environment if needed  **********/
conda env remove --prefix C:\Users\josep\OneDrive\Documentos\projects\UdacityDeng\UDataEng_L03_P02_S3toRedshiftDW\S3toRedshiftDW-env

rem /*********** pynotepbook kernel to use  **********/
C:\Users\josep\OneDrive\Documentos\projects\UdacityDeng\UDataEng_L03_P02_S3toRedshiftDW\S3toRedshiftDW-env\python.exe
~\OneDrive\Documentos\projects\UdacityDeng\UDataEng_L03_P02_S3toRedshiftDW\S3toRedshiftDW-env\python.exe

rem /*********** git notess ******************/
git init 
git add .
git commit -m "Initial commit"
git branch -m master main
git status
git remote add origin git@github.com:joseph-higaki/UDataEng_L03_P02_S3toRedshiftDW.git
git push -u origin main

test
