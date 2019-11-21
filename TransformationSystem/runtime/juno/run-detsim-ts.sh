#!/bin/bash

softwareVersion=$1
jobid=$2
evtmax=$3
extraArgs=$4

version=`more /etc/redhat-release|tr -cd "[0-9][.]"`
version1=${version%.*}
version2=${version1%.*}
export LD_LIBRARY_PATH=/usr/lib64/:$LD_LIBRARY_PATH
if [ $version2 -gt 6 ]; then
    echo "use sl7 juno software version"
    export CMTCONFIG=amd64_linux26
    source /cvmfs/juno.ihep.ac.cn/sl7_amd64_gcc48/Release/${softwareVersion}/setup.sh
else
    echo "use sl6 juno software version"
    export LD_LIBRARY_PATH=/cvmfs/juno.ihep.ac.cn/sl6_amd64_gcc44/common/lib:$LD_LIBRARY_PATH
    export CMTCONFIG=Linux-x86_64
    source /cvmfs/juno.ihep.ac.cn/sl6_amd64_gcc44/${softwareVersion}/setup.sh
fi

echo "Run command: python $TUTORIALROOT/share/tut_detsim.py --evtmax $evtmax --seed $jobid --output elecsim-${jobid}.root $extraArgs"
time python $TUTORIALROOT/share/tut_detsim.py --evtmax $evtmax --seed $jobid --output detsim-${jobid}.root $extraArgs
