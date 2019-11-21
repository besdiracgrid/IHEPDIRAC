#!/bin/bash

softwareVersion=$1
app=$2
outputPath=$3
outputPattern=$4
outputSE=$5
mode=$6

jobid=$7

evtmax=$8
seedStart=$9
extraArgs=${10}

seed=$((10#${jobid}+10#${seedStart}))

echo "Software version: $softwareVersion"
echo "Application: $app"
echo "Output path: $outputPath"
echo "Output pattern: $outputPattern"
echo "Output SE: $outputSE"
echo "Mode: $mode"
echo "Job ID: $jobid"
echo "EvtMax: $evtmax"
#echo "SeedStart: $seedStart"
#echo "Seed: $seed"
echo "ExtraArgs: $extraArgs"
echo ''

ls -lA

wget 'http://dirac-code.ihep.ac.cn/juno/ts/dirac-set-job-status.py'
chmod +x 'dirac-set-job-status.py'

./dirac-set-job-status.py "Start to run JUNO application: $app"

runScript="run-${app}-ts.sh"
wget "http://dirac-code.ihep.ac.cn/juno/ts/$runScript"
chmod +x "$runScript"
echo "./$runScript $softwareVersion $jobid $evtmax $extraArgs"
"./$runScript" "$softwareVersion" "$jobid" "$evtmax" "$extraArgs"
status=$?

if [ $status == 0 ]; then
    ./dirac-set-job-status.py 'JUNO application finished successfully'
else
    ./dirac-set-job-status.py 'JUNO application finished failed'
    exit $status
fi

ls -lA

./dirac-set-job-status.py 'Start to upload data'

wget http://dirac-code.ihep.ac.cn/juno/ts/dirac-add-files.py
chmod +x dirac-add-files.py
./dirac-add-files.py "$outputPath" "$outputPattern" "$outputSE" "$mode"
status=$?

if [ $status == 0 ]; then
    ./dirac-set-job-status.py 'Data uploaded successfully'
else
    ./dirac-set-job-status.py 'Data uploaded failed'
    exit $status
fi
