#!/bin/bash

softwareVersion=$1
jobid=$2
evtmax=$3
seed=$4
extraArgs=$5
userOutput=$6
maxlogsize=$((32*1024*1024))

. ./env.sh "$softwareVersion"

echo "find root files in this directory" 
inputfile=$(ls *detsim*.root)
num=`echo $inputfile| tr -cd "[0-9]"`


if [ "${userOutput}" -ne 0 ]; then
   echo "Run command: python $TUTORIALROOT/share/tut_elec2rec.py --evtmax -1 --seed $num --input $inputfile --output elecsim_rec-$num.root --user-output elecsim_rec_user-$num.root $extraArgs"
   (time python $TUTORIALROOT/share/tut_elec2rec.py --evtmax -1 --seed $num --input $inputfile --output elecsim_rec-$num.root --user-output elecsim_rec_user-$num.root $extraArgs) 1> >(tail -c ${maxlogsize} > app.out) 2> >(tail -c ${maxlogsize} > app.err )
else
   echo "Run command: python $TUTORIALROOT/share/tut_elec2rec.py --evtmax -1 --seed $num --input $inputfile --output elecsim_rec-$num.root $extraArgs" 
  (time python $TUTORIALROOT/share/tut_elec2rec.py --evtmax -1 --seed $num --input $inputfile --output elecsim_rec-$num.root $extraArgs) 1> >(tail -c ${maxlogsize} > app.out) 2> >(tail -c ${maxlogsize} > app.err )
fi
