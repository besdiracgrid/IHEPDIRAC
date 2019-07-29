#!/usr/bin/env python

import DIRAC
from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Insert random trigger file into the File Catalog 

Usage:
   %s [option] lfn
""" % Script.scriptName )

fcType = 'FileCatalog'

Script.parseCommandLine( ignoreErrors = False )
options = Script.getUnprocessedSwitches()
args = Script.getPositionalArgs()

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fccType = 'DataManagement/FileCatalog'
fcc = FileCatalogClient(fccType)

def getMeta(lfn, metaname):
    '''Get metadata'''
    result = fcc.getDirectoryMetadata(lfn)

    if not result['OK']:
        print result['Message']
        return

    if result['Value'].has_key(metaname):
        return result['Value'][metaname]

def main():
    lfns = args

    for lfn in lfns:
        print '================================================================================'
        print 'JobOptions for: %s' % lfn
        print '--------------------------------------------------------------------------------'
        print getMeta(lfn, 'jobOptions')
#        print getMeta(lfn, 'decayCard')
        print '--------------------------------------------------------------------------------'

if __name__ == '__main__':
    main()
