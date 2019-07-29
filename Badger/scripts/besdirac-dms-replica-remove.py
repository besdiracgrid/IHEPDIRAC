#!/usr/bin/env python 
#
"""remove the replicas of specified SE
   If files are only in this SE ,then do nothing
"""

__RCSID__= "$Id$"

from DIRAC.Core.Base import Script
switches = [
          ("e:","SEName=","SEName"),
          ("r:","DFCDir=","The logical dir in DFC."),
          ("f:","ForceRemoveAll=","RemoveAll? Default is false"),
                  ]
      
for switch in switches:
  Script.registerSwitch(*switch)
Script.setUsageMessage(__doc__)
Script.parseCommandLine()

#args = Script.getPositionalArgs()
args = Script.getUnprocessedSwitches()
#print args
if len(args) ==0:
  Script.showHelp()
  exit(-1)

removeAll = False
for switch in args:
   if switch[0].lower() == "e" or switch[0].lower() == "SEName":
     SEName = switch[1]
     print SEName
   elif switch[0].lower() == "r" or switch[0].lower() == "DFCDir":
     dfcDir = switch[1]
     print dfcDir
   elif switch[0].lower() == "f" or switch[0].lower() == "ForceRemoveAll":
     removeAll = switch[1]

from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
rm = ReplicaManager()

import os,sys
lfns = rm.getFilesFromDirectory(dfcDir)['Value']
print len(lfns)
if removeAll:
  print 'ALL'
  result = rm.removeFile(lfns)
else:
  print 'SE %s'%SEName
  result = rm.removeReplica(SEName,lfns)
