@echo off
setlocal enabledelayedexpansion

:: Ensure SPARK_HOME is set
if "%SPARK_HOME%"=="" (
    echo Error: SPARK_HOME is not set.
    exit /b 1
)

:: Construct classpath from Spark JARs
set CLASSPATH=%SPARK_HOME%\jars\*

:: Create output directory
mkdir ..\output 2>nul

:: Compile Scala sources
scalac -classpath "%CLASSPATH%" ../src/main/scala/*.scala -d ../output
