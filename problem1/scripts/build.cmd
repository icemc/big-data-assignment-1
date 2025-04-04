@echo off
setlocal

:: ==================================
:: Configuration
:: ==================================
set "SRC_DIR=..\src\main\java"
set "OUTPUT_DIR=..\output"
set "JAR_NAME=HadoopWordCount.jar"
set "FINAL_JAR_PATH=%OUTPUT_DIR%\%JAR_NAME%"

:: ==================================
:: Pre-Checks
:: ==================================
echo [INFO] Starting build process...

:: 1. Check HADOOP_HOME
echo [INFO] Checking HADOOP_HOME...
if "%HADOOP_HOME%"=="" (
    echo [ERROR] HADOOP_HOME environment variable is not set.
    exit /b 1
)
if not exist "%HADOOP_HOME%\." (
    echo [ERROR] HADOOP_HOME directory does not exist: "%HADOOP_HOME%"
    exit /b 1
)
echo [INFO] Using HADOOP_HOME: %HADOOP_HOME%

:: 2. Check Source Directory
echo [INFO] Checking source directory: %SRC_DIR%
if not exist "%SRC_DIR%\." (
    echo [ERROR] Source directory not found: "%SRC_DIR%"
    exit /b 1
)

:: 3. Check for Java Source Files
echo [INFO] Checking for *.java files in %SRC_DIR%...
dir "%SRC_DIR%\*.java" > nul 2>&1
if errorlevel 1 (
    echo [WARN] No *.java files found in "%SRC_DIR%". Nothing to compile.
    exit /b 0
)

:: ==================================
:: Build Steps
:: ==================================

:: 1. Construct Classpath
echo [INFO] Constructing classpath...
:: Use quoted paths and wildcards understood by javac
set "HADOOP_COMMON_JARS=%HADOOP_HOME%\share\hadoop\common\*"
set "HADOOP_MAPREDUCE_JARS=%HADOOP_HOME%\share\hadoop\mapreduce\*"
set "CLASSPATH=%HADOOP_COMMON_JARS%;%HADOOP_MAPREDUCE_JARS%"
echo [INFO] Classpath pattern: %CLASSPATH%

:: 2. Create Output Directory
echo [INFO] Ensuring output directory exists: %OUTPUT_DIR%
mkdir "%OUTPUT_DIR%" > nul 2>&1
if not exist "%OUTPUT_DIR%\." (
    echo [ERROR] Failed to create or find output directory: "%OUTPUT_DIR%". Check permissions.
    exit /b 1
)

:: 3. Compile Java Files
echo [INFO] Compiling Java source files...
javac -classpath "%CLASSPATH%" %SRC_DIR%\*.java -d %OUTPUT_DIR%
if errorlevel 1 (
    echo [ERROR] Java compilation failed. Check messages above.
    exit /b %errorlevel%
)
echo [INFO] Compilation successful.

:: 4. Check for Compiled Class Files
echo [INFO] Checking for *.class files in %OUTPUT_DIR%...
dir "%OUTPUT_DIR%\*.class" > nul 2>&1
if errorlevel 1 (
    echo [ERROR] No *.class files found in "%OUTPUT_DIR%" after successful compilation. This indicates an unexpected issue.
    exit /b 1
)

:: 5. Create JAR File
echo [INFO] Creating JAR file: %FINAL_JAR_PATH%
:: Store current directory before changing
pushd "%OUTPUT_DIR%" || (
    echo [ERROR] Failed to change directory to "%OUTPUT_DIR%".
    exit /b 1
)
:: Create the JAR from all class files in the current (output) directory
jar -cvf "%JAR_NAME%" *.class
set "jar_errorlevel=%errorlevel%"
:: Go back to the previous directory
popd
:: Check JAR command result after returning
if %jar_errorlevel% neq 0 (
    echo [ERROR] Failed to create JAR file "%FINAL_JAR_PATH%".
    exit /b %jar_errorlevel%
)
echo [INFO] JAR creation successful.

:: ==================================
:: Completion
:: ==================================
echo [INFO] Build complete. Output JAR: %FINAL_JAR_PATH%
exit /b 0