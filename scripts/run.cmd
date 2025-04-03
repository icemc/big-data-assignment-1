@echo off
setlocal enabledelayedexpansion

:: CONFIGURATION
set "base_input_path=../input/"
set "base_output_path=../output/hadoop/"
set "raw_input_classes=HadoopWordCount HadoopWordPairs"
set "hadoop_jar_path=../output/HadoopWordCount.jar"

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
set "hadoop_class=%~1"
set "is_raw_input=0"

for %%c in (%raw_input_classes%) do (
    if "!hadoop_class!"=="%%c" set "is_raw_input=1"
)

if !is_raw_input!==1 (
    :: RAW INPUT PROCESSING
    echo [INFO] Processing RAW INPUT mode for class '!hadoop_class!'

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

    set output_path=%base_output_path%!hadoop_class!/!first_prefix!-!last_prefix!
) else (
    :: PROCESSED INPUT PROCESSING
    echo [INFO] Processing PROCESSED INPUT mode for class '!hadoop_class!'

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
    set "output_path=%base_output_path%!hadoop_class!/!input_range!"
)

:: Execute Hadoop command
echo [INFO] Local Input Paths:  !input_paths!
echo [INFO] Local Output Path: !output_path!

set "HADOOP_CLIENT_OPTS=-Xmx16g %HADOOP_CLIENT_OPTS%"
echo [INFO] Using HADOOP_CLIENT_OPTS: !HADOOP_CLIENT_OPTS!

echo [INFO] Executing: hadoop jar !hadoop_jar_path! !hadoop_class! "!input_paths!" "!output_path!"

hadoop jar !hadoop_jar_path! !hadoop_class! "!input_paths!" "!output_path!"
set exit_status=!errorlevel!

if !exit_status! neq 0 (
    echo [ERROR] Hadoop command failed with exit code !exit_status! >&2
    exit /b !exit_status!
)

echo [INFO] Hadoop command completed successfully.
exit /b 0

:show_usage
echo Usage: >&2
echo   For raw input classes (%raw_input_classes%): >&2
echo     %~nx0 ^<class^> ^<prefix1^> [^<prefix2^> ...] >&2
echo     Example: %~nx0 HadoopWordCount AA AB AC >&2
echo. >&2
echo   For processed input: >&2
echo     %~nx0 ^<class^> ^<input_job^>/^<input_range^> >&2
echo     Example: %~nx0 WordCountFilter HadoopWordCount/AA-AB >&2
exit /b 1