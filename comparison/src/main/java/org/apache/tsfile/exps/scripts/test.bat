@echo off
setlocal enabledelayedexpansion

:: 定义参数"数组"
set ds[0]=TDrive
set ds[1]=GeoLife
set ds[2]=REDD
set ds[3]=TSBS
set ds[4]=ZY
set ds[5]=CCS

set qt[0]=SingleRaw
set qt[1]=AlignedRaw
set qt[2]=TimeFilter
set qt[3]=ValueFilter

:: 循环遍历参数并调用 jar
for /L %%i in (0,1,3) do (
@REM     echo using !ds[%%i]! for jar
    for /L %%j in (0,1,5) do (
        echo using !qt[%%i]! !ds[%%j]! fr jar
    )
@REM     java -jar your_jar_file.jar !params[%%i]!
)

endlocal