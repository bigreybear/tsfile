@echo off
setlocal enabledelayedexpansion

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

cd E:\IoTDB_Devs\tsfile\comparison\target

for /L %%i in (0,1,3) do (
    for /L %%j in (0,1,5) do (
        echo using !qt[%%i]! !ds[%%j]! fr jar
        java -Xmx32G -cp comparison-2024.2-jar-with-dependencies.jar org.apache.tsfile.exps.updated.BenchReader !ds[%%j]! !qt[%%i]!
        java -Xmx32G -cp comparison-2024.2-jar-with-dependencies.jar org.apache.tsfile.exps.updated.BenchReader !ds[%%j]! !qt[%%i]!
        java -Xmx32G -cp comparison-2024.2-jar-with-dependencies.jar org.apache.tsfile.exps.updated.BenchReader !ds[%%j]! !qt[%%i]!
    )
)

endlocal