@echo off
setlocal enabledelayedexpansion

:: CONFIGURATION
set "base_input_path=/user/Ludovic/articles/articles/"
set "base_output_path=/user/Ludovic/output/"
set "raw_input_classes=HadoopWordCount HadoopWordPairs"

:: Validate arguments
if "%~1"=="" (
    echo Error: Missing class name argument
    goto show_usage
)
if "%~2"=="" (
    echo Error: Missing input specification argument
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
    set "input_spec=%~2"
    echo "!input_spec!" | find "/" > nul
    if errorlevel 1 (
        echo Error: Must use JobName/XX-YY format
        goto show_usage
    )

    for /f "tokens=1,2 delims=/" %%i in ("!input_spec!") do (
        set "input_job=%%i"
        set "input_range=%%j"
    )

    if "!input_range!"=="" (
        echo Error: Invalid format - missing range after /
        goto show_usage
    )

    set "input_paths=%base_output_path%!input_spec!"
    set "output_path=%base_output_path%!hadoop_class!/!input_range!"
)

:: Execute Hadoop command
set "HADOOP_CLIENT_OPTS=-Xmx16g %HADOOP_CLIENT_OPTS%"

hadoop jar ../HadoopWordCount.jar !hadoop_class! "!input_paths!" "!output_path!"
exit /b %errorlevel%

:show_usage
echo Usage:
echo   For raw input classes (%raw_input_classes%):
echo     %~nx0 ^<class^> ^<prefix1^> [^<prefix2^> ...]
echo     Example: %~nx0 HadoopWordCount AA AB AC
echo.
echo   For processed input:
echo     %~nx0 ^<class^> ^<input_job^>/^<input_range^>
echo     Example: %~nx0 WordCountFilter HadoopWordCount/AA-AB
exit /b 1