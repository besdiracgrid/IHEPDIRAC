# -*- coding: utf-8 -*-

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Download LFNs in a dataset 

Usage:
  %s <dataset name> 
""" % Script.scriptName)

Script.registerSwitch("", "save=", "The directory which save files.")

Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()
if (len(args) != 1):
  gLogger.error("Please support the dataset name")
  DIRAC.exit(-1)

dataset = args[0]
dir_save = args[0]

for k,v in Script.getUnprocessedSwitches():
  if k.lower() in ["save"]:
    dir_save = v

gLogger.info("Dataset Name: ", dataset)
gLogger.info("Save in: ", dir_save)

# Get the list of LFNs in one dataset
from DIRAC.Core.DISET.RPCClient import RPCClient
transferRequest = RPCClient("Transfer/Dataset")
res = transferRequest.list(dataset)

if not res["OK"]:
  gLogger.error(res)
  DIRAC.exit(-1)

file_list = [v[1] for v in res["Value"]]
gLogger.debug("File List", file_list)
# Begin to save file
# Refer to dirac-dms-get-file.py in DIRAC/Interfaces/scripts


from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()
res = dirac.getFile( file_list, destDir = dir_save, printOutput = True )

if not res["OK"]:
  gLogger.error(res)
  DIRAC.exit(-1)

DIRAC.exit(0)
