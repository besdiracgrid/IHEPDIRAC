#!/usr/bin/env python

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Insert random trigger file into the File Catalog 

Usage:
   %s [option]
""" % Script.scriptName )

fcType = 'FileCatalog'
Script.registerSwitch( "f:", "file-catalog=", "Catalog client type to use (default %s)" % fcType )
Script.registerSwitch( "j:", "jopts=",  "jobOptions.txt" )
Script.registerSwitch( "r:", "runmin=", "Minimun run number" )
Script.registerSwitch( "R:", "runmax=", "Maximum run number" )
Script.registerSwitch( "e:", "se=",     "SE name" )

Script.parseCommandLine( ignoreErrors = False )
options = Script.getUnprocessedSwitches()


from DIRAC.DataManagementSystem.Client.DataManager       import DataManager
from DIRAC.Resources.Storage.StorageElement              import StorageElement
from DIRAC.Resources.Catalog.FileCatalogFactory          import FileCatalogFactory
from DIRAC.Core.Utilities.SiteSEMapping                  import getSEsForSite

import sys
import re
import socket
import time
import random

SeSiteMap = {
  'BES.JINR.ru'       : 'JINR-USER',
  'BES.IHEP-PBS.cn'   : 'IHEPD-USER',
  'BES.GUCAS.cn'      : 'IHEPD-USER',
  'BES.USTC.cn'       : 'USTC-USER',
  'BES.WHU.cn'        : 'WHU-USER',
}

SeDomainMap = {
  'jinrru'      : 'JINR-USER',
  'ihepaccn'    : 'IHEPD-USER',
  'ustceducn'   : 'USTC-USER',
  'whueducn'    : 'WHU-USER',
}



def determineSeFromSite():
    siteName = DIRAC.siteName()
    SEname = SeSiteMap.get(siteName, '')
    if not SEname:
        result = getSEsForSite(siteName)
        if result['OK'] and result['Value']:
            SEname = result['Value'][0]
    return SEname

def determineSeFromDomain():
    fqdn=socket.getfqdn()
    domain=''.join(fqdn.split('.')[-2:])
    if domain=='accn' or domain=='educn':
        domain=''.join(socket.getfqdn().split('.')[-3:])

    SEname = SeDomainMap.get(domain, '')
    return SEname

def determineSe():
    se = determineSeFromSite()
    if se:
        return se

    return determineSeFromDomain()

def getFile(lfn, se=''):
    dm = DataManager()

    download_ok = 0
    get_active_replicas_ok = False
    lfn_on_se = False
    error_msg = ''
    if se:
        for i in range(0, 5):
            result = dm.getActiveReplicas(lfn)
            if result['OK'] and result['Value']['Successful']:
                get_active_replicas_ok = True
                lfnReplicas = result['Value']['Successful']
                if se in lfnReplicas[lfn]:
                    lfn_on_se = True
                    break
            time.sleep(3)
            print '- Get replicas for %s failed, try again' % lfn

        if not get_active_replicas_ok:
            return S_ERROR('Get replicas error: %s' % lfn)

    if lfn_on_se:
        se = StorageElement(se)
        # try 5 times
        for j in range(0, 5):
            result = se.getFile(lfn)
            if result['OK'] and result['Value']['Successful'] and result['Value']['Successful'].has_key(lfn):
                break
            time.sleep(random.randint(180, 600))
            print '- %s getStorageFile(%s) failed, try again' % (lfn, se)
        if result['OK']:
            if result['Value']['Successful'] and result['Value']['Successful'].has_key(lfn):
                download_ok = 1
            else:
                error_msg = 'Downloading %s from SE %s error!' % (lfn, se)
        else:
            error_msg = result['Message']
    else:
        if se:
            print 'File %s not found on SE "%s" after %s tries, trying other SE' % (lfn, se, i+1)
        # try 5 times
        for j in range(0, 5):
            result = dm.getFile(lfn)
            if result['OK'] and result['Value']['Successful'] and result['Value']['Successful'].has_key(lfn):
                break
            time.sleep(random.randint(180, 600))
            print '- getFile(%s) failed, try again' % lfn
        if result['OK']:
            if result['Value']['Successful'] and result['Value']['Successful'].has_key(lfn):
                download_ok = 2
            else:
                error_msg = 'Downloading %s from random SE error!' % lfn
        else:
            error_msg = result['Message']

    if download_ok:
        return S_OK({lfn: {'DownloadOK': download_ok, 'Retry': j+1}})

    return S_ERROR(error_msg)

def parseOpt(filename):
    f = open(filename, 'r')
    fileContent = f.read()
    mat = re.findall('RealizationSvc\s*\.\s*RunIdList.*?;', fileContent, re.DOTALL)
    if not mat:
        return (0, 0)

    line = mat[-1]
    tmp = ''.join(line.split())
    line = tmp.replace('[', '{').replace(']', '}')

    vars = line.split('{')[1].split('}')[0].split(',')

    if len(vars) == 1:
        runmin = runmax = abs(int(vars[0]))
    elif len(vars) == 3 and int(vars[1]) == 0:
        runmin = abs(int(vars[0]))
        runmax = abs(int(vars[2]))
        if runmax < runmin:
            temp = runmax
            runmax = runmin
            runmin = temp
    else:
        runmin = runmax = 0

    return (runmin, runmax)

def findFiles(runnb):
    for i in range(0, 16):
        result = FileCatalogFactory().createCatalog(fcType)
        if result['OK']:
            break
        time.sleep(random.randint(30, 120))
        print '- Get FileCatalog failed, try again'
    if not result['OK']:
        print >>sys.stderr, 'Get FileCatalog error: %s. Retry %s' % (result['Message'], i+1)
        return result

    catalog = result['Value']

    (runmin,runmax) = runnb[0]

    for i in range(0, 16):
        result = catalog.findFilesByMetadata({'runL':{'>=':runmin},'runH':{'<=':runmax}}, '/bes/File/randomtrg')
        if result['OK']:
            break
        time.sleep(random.randint(30, 120))
        print '- Find files failed, try again'
    if not result['OK']:
        print >>sys.stderr, 'Find files error in run (%s - %s). Retry %s' % (runmin, runmax, i+1)
        print >>sys.stderr, result

    return result

def main():
    jfile = ''
    runmin = 0
    runmax = 0
    se = ''
    for option in options:
        (switch, val) = option
        if switch == 'j' or switch == 'jopts':
            jfile = val
        if switch == 'r' or switch == 'runmin':
            runmin = int(val)
        if switch == 'R' or switch == 'runmax':
            runmax = int(val)
        if switch == 'e' or switch == 'se':
            se = val

    if jfile != '':
        (runmin, runmax) = parseOpt(jfile)
    
    if (runmin, runmax) == (0, 0):
        print >>sys.stderr, 'No input run range. Check arguments or jobOptions.txt'
        sys.exit(68)

    if(runmax < runmin):
        temp = runmax
        runmax = runmin
        runmin = temp

    print "Run range:", runmin, runmax

    if not se:
        se = determineSe()
        print "Determine SE:", se

    result = findFiles([(runmin, runmax)])
    if not result['OK']:
        print >>sys.stderr, 'Finally find file error: (%s, %s)' % (runmin, runmax)
        print >>sys.stderr, result
        sys.exit(65)

    lfns = result['Value']
    print '%s files found in run %s - %s' % (len(lfns), runmin, runmax)
    for lfn in lfns:
        result = getFile(lfn, se)
        print result
        if not result['OK']:
            print >>sys.stderr, 'Finally download file %s from SE "%s" error:' % (lfn, se)
            print >>sys.stderr, result
            sys.exit(66)


if __name__ == '__main__':
    main()
