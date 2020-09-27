#!/usr/bin/env python

import os
import sys

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

usageMsg = '''Check if all files under specified DFC directory have a replica in specified SEs (splitted by commas).

{0} [option|cfgfile] DFCDir SEs'''.format(Script.scriptName)
Script.setUsageMessage(usageMsg)
Script.registerSwitch('f', 'fix', 'fix the discrepancy by replicating files from other SEs.')

Script.parseCommandLine(ignoreErrors=False)

args = Script.getPositionalArgs()
switches = Script.getUnprocessedSwitches()


dfcDir = args[0]
SEs = args[1]
SE_list = SEs.split(',')

fixDiscrepancy = False



for k,v in switches:
	if k == 'fix':
		fixDiscrepancy = True
	

#log
gLogger.notice('Checking with DFC folder: {0}\nFor SE(s): {1}'.format(dfcDir,SEs))
if fixDiscrepancy:
	gLogger.notice('Would fix the discrepancy by replicating files from other SEs.')
gLogger.notice("\n\n")

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fcc = FileCatalogClient('DataManagement/FileCatalog')

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
dm = DataManager()

#get all files in dfcDir
gf_result = dm.getFilesFromDirectory(dfcDir)
if not gf_result['OK']:
	gLogger.error('Failed to list directory %s: %s' %(dfcDir, gf_result['Message']))
	exit()
files = gf_result['Value']
fileNumber = len(files)
gLogger.notice('There are {0} files in dir {1}'.format(fileNumber,dfcDir))
	
#get replicas in the SE
gr_result = dm.getReplicasFromDirectory(dfcDir)
if not gr_result['OK']:
	gLogger.error('Failed to get replicas from directory %s: %s' %(dfcDir, gr_result['Message']))
	exit()

replicas = {SE:[] for SE in SE_list}
for LFN in gr_result['Value'].keys():
	repInfo = gr_result['Value'][LFN]
	for SE in SE_list:
		repInfo_in_SE = repInfo.get(SE)	#Status, PFN
		if repInfo_in_SE is not None:
			replicas[SE].append(LFN)

for SE in SE_list:
	replicaNumber = len(replicas[SE])
	gLogger.notice('There are {0} replicas in SE {1}'.format(replicaNumber,SE))

#compare
for SE in SE_list:
	files_not_in_SE = [f for f in files if f not in replicas[SE]]
	nFilesNotInSE = len(files_not_in_SE)

	nReplicate = 0

	gLogger.notice("\nThere are {0} files not in SE {1}.".format(nFilesNotInSE,SE))
	for f in files_not_in_SE:
		gLogger.notice('%s'%f)


	if fixDiscrepancy:
		gLogger.notice(' ')
		for f in files_not_in_SE:
			availableReplicas = gr_result['Value'][f]
			gLogger.notice('Start replicating LFN %s'%f)
			result = dm.replicateAndRegister(f, SE)
			if not result['OK']:
				gLogger.error('Failed to replicate LFN %s to target SE %s: %s' %(f, SE, result['Message']))
			else:
				gLogger.notice('Success')
				nReplicate += 1

		gLogger.notice('{0} files successfully replicated'.format(nReplicate))

