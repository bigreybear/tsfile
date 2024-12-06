@echo off
setlocal enabledelayedexpansion

cd ../../../../../target

dir

echo %date% %time%

set ds[0]=bw
set ds[1]=sw
set ds[2]=zy
set ds[3]=xyzc

set "mm[0]= -mt hash "
set "mm[1]= -mt hash -ms simple -merge "
set "mm[2]= -mt hash -ms partial -merge "
set "mm[3]= -mt hash -ms full -merge "
set "mm[4]= -mt fdm -ms full -merge "
set "mm[5]= -mt cdm -ms simple -merge "
set "mm[6]= -mt cdm -ms partial -merge "
set "mm[7]= -mt cdm -ms full -merge "


for /L %%i in (0,1,3) do (
    echo ============= SPACE for !ds[%%i]! ========= >> output.txt
    for /L %%j in (0,1,7) do (
        java -jar seart-1.0.1-SNAPSHOT.jar !mm[%%j]! -ds !ds[%%i]! -space >> output.txt 2>&1
        echo. >> output.txt
    )
    echo. >> output.txt
)


for /L %%i in (0,1,3) do (
    echo ============= LATENCY for !ds[%%i]! ========= >> output.txt
    for /L %%j in (0,1,7) do (
        java -jar seart-1.0.1-SNAPSHOT.jar !mm[%%j]! -ds !ds[%%i]! -latency >> output.txt 2>&1
        java -jar seart-1.0.1-SNAPSHOT.jar !mm[%%j]! -ds !ds[%%i]! -latency >> output.txt 2>&1
        java -jar seart-1.0.1-SNAPSHOT.jar !mm[%%j]! -ds !ds[%%i]! -latency >> output.txt 2>&1
        echo. >> output.txt
    )
    echo. >> output.txt
)

echo. >> output.txt