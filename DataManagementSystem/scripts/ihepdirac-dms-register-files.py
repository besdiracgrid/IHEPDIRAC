#!/usr/bin/env python

import os
import sys

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

Script.setUsageMessage('''Register SE files from a list of files to DFC. These list of files must be locally readable

{0} [option|cfgfile] DFCRoot LocalRoot Filelist SE

Example: {0} /juno/lustre/junofs/PmtCharacterization/scan_data/soft/root_macros /junofs/PmtCharacterization/scan_data/soft/root_macros  filelist.txt IHEP-STORM'''.format(Script.scriptName))
Script.registerSwitch( 'e', 'existCheck', 'Check if file exists')
Script.registerSwitch( 'q:', 'querySkip=', 'Skip files in the meta query')
Script.registerSwitch( 'b:', 'bufferSize=', 'Register buffer size, default to 100')
Script.parseCommandLine(ignoreErrors = False)

from DIRAC.Core.Utilities.Adler import fileAdler
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fcc = FileCatalogClient('DataManagement/FileCatalog')

args = Script.getPositionalArgs()

if len(args) != 4:
    Script.showHelp()
    exit(1)

dfcRoot = args[0]
if (not args[1]) or args[1].endswith(os.sep):
    localRoot = args[1]
else:
    localRoot = args[1] + os.sep
fileList = args[2]
toSE = args[3]

lfnQuery = []
existCheck = False
bufferSize = 100
switches = Script.getUnprocessedSwitches()
for switch in switches:
    if switch[0] == 'q' or switch[0] == 'querySkip':
        result = fcc.findFilesByMetadata({'juno_transfer': switch[1]}, '/')
        if result['OK']:
            lfnQuery += result['Value']
    if switch[0] == 'e' or switch[0] == 'existCheck':
        existCheck = True
    if switch[0] == 'b' or switch[0] == 'bufferSize':
        bufferSize = int(switch[1])

lfnQuery = set(lfnQuery)

counter = 0

dm = DataManager()
fileTupleBuffer = []
with open(fileList) as file_obj:
    for fullFn in file_obj:
        counter += 1
        print(fullFn)
        fullFn=fullFn.strip('\n')

        if not fullFn.startswith(localRoot):
            gLogger.error('%s does not start with %s' % (fullFn, localDir))
            continue
        lastPart = fullFn[len(localRoot):]

        #lastPart = os.path.basename(fullFn)
        lfn = os.path.join(dfcRoot, lastPart)
        print(lfn)

        if lfn in lfnQuery:
            if counter%1000 == 0:
                gLogger.notice('Skip file in query counter: %s' % counter)
            continue

        if existCheck:
            result = fcc.isFile(lfn)
            if result['OK'] and lfn in result['Value']['Successful'] and result['Value']['Successful'][lfn]:
                if counter%1000 == 0:
                    gLogger.notice('Skip file existed counter: %s' % counter)
                continue

        size = os.path.getsize(fullFn)
        adler32 = fileAdler(fullFn)
        guid = makeGuid()
        fileTuple = ( lfn, fullFn, size, toSE, guid, adler32 )
        #gLogger.notice('the parameter to registered %s %s %s %s %s %s' % (lfn,fullFn,size,toSE,guid,adler32))
        fileTupleBuffer.append(fileTuple)
        gLogger.debug('Register to lfn: %s' % lfn)
        gLogger.debug('fileTuple: %s' % (fileTuple,))

        if len(fileTupleBuffer) >= bufferSize:
            result = dm.registerFile( fileTupleBuffer )
            if not result['OK']:
               gLogger.error('Can not register %s' % fullFn)
               exit(1)
            del fileTupleBuffer[:]
            gLogger.notice('%s files registered' % counter)

if fileTupleBuffer:
    result = dm.registerFile( fileTupleBuffer )
    if not result['OK']:
        gLogger.error('Can not register %s' % fullFn)
        exit(1)
    del fileTupleBuffer[:]

gLogger.notice('Total %s files registered' % counter)
