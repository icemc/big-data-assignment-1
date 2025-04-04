@echo off
setlocal enabledelayedexpansion

:: Change directory to output and create JAR
cd /d ..\output || exit /b 1
jar -cvf SparkWordCount.jar *.class

echo Build complete: output\SparkWordCount.jar
exit /b 0
