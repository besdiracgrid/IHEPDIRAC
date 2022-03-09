#!/usr/bin/env python

import os
import sys

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script
from XRootD import client
from XRootD.client.flags import QueryCode

Script.setUsageMessage('''Register files in EOS SE Directory to DFC.

{0} [option|cfgfile] DFCDir EosDir SE 

Example : {0} /juno/raw/test_register root://junoeos01.ihep.ac.cn:1094//eos/juno/dirac/juno/raw/test_register IHEP-JUNOEOS'''.format(Script.scriptName))
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

if len(args) != 3:
    Script.showHelp()
    exit(1)
dfcRoot = args[0]
eosRoot = args[1] 
toSE = args[2]

group = eosRoot.split("//")
eosHead = group[0] + "//" + group[1] + "/" 
eosPath = "/" + group[2]

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
eosclient = client.FileSystem(eosRoot)

dm = DataManager()
fileTupleBuffer = []

cmd = 'xrdfs %s ls %s -R' % (eosHead, eosPath)
file_obj = os.popen(cmd).readlines()
for fullFn in file_obj:
    counter += 1
    fullFn=fullFn.strip('\n')

    if not fullFn.startswith(eosPath):
        gLogger.error('%s does not start with %s' % (fullFn, localDir))
        continue 
    lastPart = fullFn[len(eosPath):]
    pfn = eosHead + fullFn
       
    lfn = dfcRoot + lastPart

    if lfn in lfnQuery:
        gLogger.notice('File exists, skip: %s' % lfn)
        counter -= 1
        continue

    if existCheck:
        result = fcc.isFile(lfn)
        if result['OK'] and lfn in result['Value']['Successful'] and result['Value']['Successful'][lfn]:
            gLogger.notice('File exists, skip: %s' % lfn)
            counter -= 1
            continue

    status, response = eosclient.stat(fullFn)
    if status.ok:
        size = response.size
    else:
        gLogger.error('Error in getting size of %s!')% lfn
        continue
    status, response = eosclient.query(QueryCode.CHECKSUM, fullFn)
    if status.ok:
        gchecksum = response.split()
    else:
        gLogger.error('Error in getting checksum of %s!')% lfn
        continue
    if size != 0:
        adler32 = str(gchecksum[1].strip(b'\x00'.decode()))
        guid = makeGuid()
        fileTuple = ( lfn, pfn, size, toSE, guid, adler32 )
        gLogger.debug('To be registered: %s %s %s %s %s %s' % (lfn,pfn,size,toSE,guid,adler32))
        fileTupleBuffer.append(fileTuple)
        gLogger.debug('Register lfn: %s' % lfn)
    else:
        counter -=1

    if len(fileTupleBuffer) >= bufferSize:
        result = dm.registerFile( fileTupleBuffer )
        if not result['OK']:
            gLogger.error('Can not register %s' % pfn)
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
