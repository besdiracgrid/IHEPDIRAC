#!/usr/bin/env python

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Insert random trigger file into the File Catalog 

Usage:
   %s [option] ... JobID ...
""" % Script.scriptName )

fcType = 'FileCatalog'
Script.registerSwitch( "f:", "file-catalog=", "Catalog client type to use (default %s)" % fcType )
Script.registerSwitch( "l:", "lfn=",          "File lfn" )

Script.parseCommandLine( ignoreErrors = False )
options = Script.getUnprocessedSwitches()
args = Script.getPositionalArgs()


from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fccType = 'DataManagement/FileCatalog'
fcc = FileCatalogClient(fccType)

import os
import sys
import tarfile

def findFiles(jobIds):
    '''Find specified log files from the job ids'''
    lfns=[]

    for jobId in jobIds:
        result = fcc.findFilesByMetadata({'dataType': 'log', 'jobId': int(jobId)}, '/')

        if result['OK']:
            for f in result['Value']:
               lfns.append(f)

    return lfns

def getJobId(lfns):
    '''Find job ids for the output file lfn'''
    jobIds = []
    for lfn in lfns:
        result = fcc.getFileUserMetadata(lfn)

        if result['OK']:
            if result['Value'].has_key('jobId'):
               jobIds.append(result['Value']['jobId'])

    return jobIds

def getLog(loglfn):
    result = dirac.getFile(loglfn)
    if not result['OK']:
        print 'Download log error: %s' % loglfn
        return

    logname = os.path.basename(loglfn)
    tar = tarfile.open(logname, 'r:gz')
    for tarinfo in tar:
        tar.extract(tarinfo.name, '.')
        print 'Retrieving file: %s' % tarinfo.name

    os.remove(logname)

def main():
    lfns = []
    for option in options:
        (switch, val) = option
        if switch == 'l' or switch == 'lfn':
            lfns.append(val)

    jobIds = []

    if lfns:
        jobIds += getJobId(lfns)

    if args:
        jobIds += args

    loglfns = findFiles(jobIds)
    print '%s log(s) found' % len(loglfns)

    # retrieve all the logs and unpack
    for loglfn in loglfns:
        print 'Retrieving log: %s' % loglfn
        getLog(loglfn)

if __name__ == '__main__':
    main()
