@echo off
setlocal enabledelayedexpansion

:: Configuration
set "SUBDIRS=AA AB AC AD AE AF AG AH AI AJ AK"
set "RAW_CLASSES=HadoopWordCount HadoopWordPairs"
set "WORDCOUNT_DEPS=WordCountFilter WordCountTop"
set "WORDPAIRS_DEPS=WordPairsFilter NumberWordPairsTop"

:: Create stats directory if it doesn't exist
if not exist "stats\" mkdir stats

:: Initialize master stats file
set "all_stats_file=stats\all-stats.csv"
set "all_stats_temp=stats\all-stats.tmp"
if exist "!all_stats_file!" del "!all_stats_file!"
if exist "!all_stats_temp!" del "!all_stats_temp!"

:: Write header to temp file
echo JobName,JobType,InputSpec,StartTime,EndTime,ElapsedTime > "!all_stats_temp!"

:: Initialize variables
set "input_dirs="

:: Process each subdirectory incrementally
for %%d in (%SUBDIRS%) do (
    :: Add the current directory to the input list
    if defined input_dirs (
        set "input_dirs=!input_dirs! %%d"
    ) else (
        set "input_dirs=%%d"
    )

    :: Run raw input jobs first
    for %%c in (%RAW_CLASSES%) do (
        echo.
        echo ==============================================
        echo Running %%c with input: !input_dirs!
        echo ==============================================

        :: Set stats file name for this job class in stats subdirectory
        set "stats_file=stats\%%c_stats.csv"
        set "job_type=RawInput"

        :: Create stats file header if it doesn't exist
        if not exist "!stats_file!" (
            echo InputDirs,StartTime,EndTime,ElapsedTime > "!stats_file!"
        )

        :: Get start time with milliseconds
        for /f "tokens=1-4 delims=:., " %%a in ("!time!") do (
            set "start_h=%%a"
            set "start_m=%%b"
            set "start_s=%%c"
            set "start_ms=%%d"
        )
        set "start_time=!time!"
        set "start_date=!date!"

        :: Run the job
        call run.cmd %%c !input_dirs!
        set "exit_code=!errorlevel!"

        :: Get end time with milliseconds
        for /f "tokens=1-4 delims=:., " %%a in ("!time!") do (
            set "end_h=%%a"
            set "end_m=%%b"
            set "end_s=%%c"
            set "end_ms=%%d"
        )
        set "end_time=!time!"

        :: Calculate elapsed time in seconds with 3 decimal places (milliseconds)
        set /a "start_total_ms=(1!start_h!-100)*3600000 + (1!start_m!-100)*60000 + (1!start_s!-100)*1000 + (1!start_ms!-100)"
        set /a "end_total_ms=(1!end_h!-100)*3600000 + (1!end_m!-100)*60000 + (1!end_s!-100)*1000 + (1!end_ms!-100)"

        :: Handle midnight crossing
        if !end_total_ms! lss !start_total_ms! set /a "end_total_ms+=86400000"
        set /a "elapsed_ms=end_total_ms - start_total_ms"

        :: Convert to seconds with 3 decimal places
        set /a "elapsed_sec=elapsed_ms / 1000"
        set /a "elapsed_ms_part=elapsed_ms %% 1000"
        set "elapsed_time=!elapsed_sec!.!elapsed_ms_part!"

        :: Pad milliseconds with leading zeros if needed
        if !elapsed_ms_part! lss 100 set "elapsed_time=!elapsed_sec!.0!elapsed_ms_part!"
        if !elapsed_ms_part! lss 10 set "elapsed_time=!elapsed_sec!.00!elapsed_ms_part!"

        :: Write to individual job stats file
        echo !input_dirs: =,!,!start_date! !start_time!,!end_time!,!elapsed_time! >> "!stats_file!"

        :: Append to temp stats file
        echo %%c,!job_type!,!input_dirs: =,!,!start_date! !start_time!,!end_time!,!elapsed_time! >> "!all_stats_temp!"

        :: Check for errors
        if !exit_code! neq 0 (
            echo ERROR: Job %%c with input !input_dirs! failed with exit code !exit_code!
            goto :eof
        )

        :: Now run dependent jobs if this was a parent job
        if "%%c"=="HadoopWordCount" (
            :: Run WordCount dependent jobs
            for %%d in (%WORDCOUNT_DEPS%) do (
                call :RunDependentJob "%%d" "%%c" "!input_dirs!"
                if !errorlevel! neq 0 goto :eof
            )
        ) else if "%%c"=="HadoopWordPairs" (
            :: Run WordPairs dependent jobs
            for %%d in (%WORDPAIRS_DEPS%) do (
                call :RunDependentJob "%%d" "%%c" "!input_dirs!"
                if !errorlevel! neq 0 goto :eof
            )
        )
    )
)

:: Now sort the all-stats file by JobName
echo Sorting all-stats.csv by JobName...
(
    :: Write header first
    type "!all_stats_temp!" | findstr /B "JobName"

    :: Then write sorted data (skip header line)
    type "!all_stats_temp!" | findstr /V /B "JobName" | sort
) > "!all_stats_file!"

del "!all_stats_temp!"

echo.
echo All jobs completed successfully. Statistics saved to:
echo stats\all-stats.csv (sorted by JobName)
for %%c in (%RAW_CLASSES% %WORDCOUNT_DEPS% %WORDPAIRS_DEPS%) do (
    echo stats\%%c_stats.csv
)
goto :eof

:RunDependentJob
setlocal
set "dep_class=%~1"
set "parent_class=%~2"
set "input_dirs=%~3"

:: Convert space-separated input dirs to first-last format
set "first_dir="
set "last_dir="
for %%i in (%input_dirs%) do (
    if not defined first_dir set "first_dir=%%i"
    set "last_dir=%%i"
)
set "input_spec=%parent_class%/%first_dir%-%last_dir%"

echo.
echo ==============================================
echo Running %dep_class% with input: %input_spec%
echo ==============================================

:: Set stats file name for this job class in stats subdirectory
set "stats_file=stats\%dep_class%_stats.csv"
set "job_type=Dependent"

:: Create stats file header if it doesn't exist
if not exist "%stats_file%" (
    echo InputSpec,StartTime,EndTime,ElapsedTime > "%stats_file%"
)

:: Get start time with milliseconds
for /f "tokens=1-4 delims=:., " %%a in ("%time%") do (
    set "start_h=%%a"
    set "start_m=%%b"
    set "start_s=%%c"
    set "start_ms=%%d"
)
set "start_time=%time%"
set "start_date=%date%"

:: Run the dependent job
call run.cmd %dep_class% %input_spec%
set "exit_code=%errorlevel%"

:: Get end time with milliseconds
for /f "tokens=1-4 delims=:., " %%a in ("%time%") do (
    set "end_h=%%a"
    set "end_m=%%b"
    set "end_s=%%c"
    set "end_ms=%%d"
)
set "end_time=%time%"

:: Calculate elapsed time in seconds with 3 decimal places (milliseconds)
set /a "start_total_ms=(1%start_h%-100)*3600000 + (1%start_m%-100)*60000 + (1%start_s%-100)*1000 + (1%start_ms%-100)"
set /a "end_total_ms=(1%end_h%-100)*3600000 + (1%end_m%-100)*60000 + (1%end_s%-100)*1000 + (1%end_ms%-100)"

:: Handle midnight crossing
if %end_total_ms% lss %start_total_ms% set /a "end_total_ms+=86400000"
set /a "elapsed_ms=end_total_ms - start_total_ms"

:: Convert to seconds with 3 decimal places
set /a "elapsed_sec=elapsed_ms / 1000"
set /a "elapsed_ms_part=elapsed_ms %% 1000"
set "elapsed_time=%elapsed_sec%.%elapsed_ms_part%"

:: Pad milliseconds with leading zeros if needed
if %elapsed_ms_part% lss 100 set "elapsed_time=%elapsed_sec%.0%elapsed_ms_part%"
if %elapsed_ms_part% lss 10 set "elapsed_time=%elapsed_sec%.00%elapsed_ms_part%"

:: Write to individual job stats file
echo %input_spec%,%start_date% %start_time%,%end_time%,%elapsed_time% >> "%stats_file%"

:: Append to temp stats file
echo %dep_class%,%job_type%,%input_spec%,%start_date% %start_time%,%end_time%,%elapsed_time% >> "!all_stats_temp!"

:: Check for errors
if %exit_code% neq 0 (
    echo ERROR: Job %dep_class% with input %input_spec% failed with exit code %exit_code%
    exit /b %exit_code%
)

endlocal
exit /b 0