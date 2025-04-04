@echo off
setlocal enabledelayedexpansion

:: CONFIGURATION
set "base_input_path=../../input/"
set "base_output_path=../../output/spark/"
set "raw_input_classes=SparkWordCount SparkWordPairs"
set "spark_jar_path=../output/SparkWordCount.jar"

:: Validate arguments
if "%~1"=="" (
    echo Error: Missing class name argument >&2
    goto show_usage
)
if "%~2"=="" (
    echo Error: Missing input specification argument >&2
    goto show_usage
)

:: Set class name and check type
set "spark_class=%~1"
set "is_raw_input=0"

for %%c in (%raw_input_classes%) do (
    if "!spark_class!"=="%%c" set "is_raw_input=1"
)

if !is_raw_input!==1 (
    :: RAW INPUT PROCESSING
    echo [INFO] Processing RAW INPUT mode for class '!spark_class!'

    set input_prefixes=
    set /a arg_count=0
    for %%a in (%*) do (
        set /a arg_count+=1
        if !arg_count! gtr 1 (
            if not defined first_prefix set "first_prefix=%%a"
            set "last_prefix=%%a"
            if defined input_prefixes (
                set "input_prefixes=!input_prefixes!,%%a"
            ) else (
                set "input_prefixes=%%a"
            )
        )
    )

    if "!input_prefixes!"=="" (
        echo Error: Raw input mode requires at least one prefix argument. >&2
        goto show_usage
    )

    set input_paths=
    for %%p in (!input_prefixes!) do (
        if defined input_paths (
            set "input_paths=!input_paths!,%base_input_path%%%p"
        ) else (
            set "input_paths=%base_input_path%%%p"
        )
    )

    set output_path=%base_output_path%!spark_class!/!first_prefix!-!last_prefix!
) else (
    :: PROCESSED INPUT PROCESSING
    echo [INFO] Processing PROCESSED INPUT mode for class '!spark_class!'

    set "input_spec=%~2"
    echo "!input_spec!" | find "/" > nul
    if errorlevel 1 (
        echo Error: Input spec '!input_spec!' must use JobName/XX-YY format. >&2
        goto show_usage
    )

    for /f "tokens=1,2 delims=/" %%i in ("!input_spec!") do (
        set "input_job=%%i"
        set "input_range=%%j"
    )

    if "!input_range!"=="" (
        echo Error: Invalid format '!input_spec!' - requires non-empty JobName and Range. >&2
        goto show_usage
    )

    set "input_paths=%base_output_path%!input_spec!"
    set "output_path=%base_output_path%!spark_class!/!input_range!"
)

:: Execute Spark command
echo [INFO] Local Input Paths:  !input_paths!
echo [INFO] Local Output Path: !output_path!

:: Set Spark memory configuration (equivalent to HADOOP_CLIENT_OPTS=-Xmx16g)
set "SPARK_DRIVER_MEMORY=16g"
set "SPARK_EXECUTOR_MEMORY=16g"
echo [INFO] Using SPARK_DRIVER_MEMORY: !SPARK_DRIVER_MEMORY!

echo [INFO] Executing: spark-submit --class !spark_class! !spark_jar_path!  "!input_paths!" "!output_path!"

spark-submit --class !spark_class! !spark_jar_path! "!input_paths!" "!output_path!"

echo [INFO] Spark command completed successfully.
exit /b 0

:show_usage
echo Usage: >&2
echo   For raw input classes (%raw_input_classes%): >&2
echo     %~nx0 ^<class^> ^<prefix1^> [^<prefix2^> ...] >&2
echo     Example: %~nx0 SparkWordCount AA AB AC >&2
echo. >&2
echo   For processed input: >&2
echo     %~nx0 ^<class^> ^<input_job^>/^<input_range^> >&2
echo     Example: %~nx0 WordCountFilter SparkWordCount/AA-AB >&2
exit /b 1