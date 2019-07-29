#!/usr/bin/env python

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Insert random trigger file into the File Catalog 

Usage:
   %s [option]
""" % Script.scriptName )

#fcType = 'FileCatalog'
#Script.registerSwitch( "f:", "file-catalog=", "Catalog client type to use (default %s)" % fcType )
#Script.registerSwitch( "j:", "jopts=",  "jobOptions.txt" )
Script.registerSwitch( "r:", "runmin=",  "Minimun run number" )
Script.registerSwitch( "R:", "runmax=",  "Maximum run number" )
Script.registerSwitch( "l:", "list=",    "Random trigger file list for upload" )
Script.registerSwitch( "e:", "se=",      "SE name" )
Script.registerSwitch( "b:", "basedir=", "Base directory for random trigger file" )

Script.parseCommandLine( ignoreErrors = False )
options = Script.getUnprocessedSwitches()

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

import os
import sys

fccType = 'DataManagement/FileCatalog'
fcc = FileCatalogClient(fccType)

def uploadFile(src, lfn, se):
    print 'Uploading : ' + src + ' -> ' + lfn + ' on ' + se
    result = dirac.addFile(lfn, src, se)
    if result['OK']:
        if lfn in result['Value']['Successful'] and result['Value']['Successful'][lfn]:
            print 'Upload OK'
            return
    print >>sys.stderr, 'Uploading failed : ' + src + ' -> ' + lfn + ' on ' + se

def setMeta(run, lfn):
    result = fcc.setMetadata(lfn, {'runH': run, 'runL': run})
    return result

def validateDirectory(dfcdir, round):
    result = fcc.listDirectory(dfcdir)
    if not result['OK']:
        print 'DFC directory can not be accessed: %s' % (dfcdir, result['Message'])
        sys.exit(1)

    if result['Value']['Successful']:
        return

    result = fcc.createDirectory(dfcdir)
    if not result['OK']:
        print 'Failed to create directory %s: %s' % (dfcdir, result['Message'])
        sys.exit(1)

    result = fcc.setMetadata(dfcdir, {'dataType':'rantrg', 'round':round})
    if not result['OK']:
        print 'Failed to set metadata for %s: %s' % (dfcdir, result['Message'])
        sys.exit(1)

def uploadAndRegister(run, filepath, filename, basedir, se):
    fullpath = filepath + '/' + filename

    if not os.path.exists(fullpath):
        print >>sys.stderr, 'Original file does not exist: %s' % fullpath
        return

    round = 'roundxx'
    index = filepath.find('round')
    if index != -1:
        round = filepath[index:index+7]

    dfcdir = basedir + '/' + round
    validateDirectory(dfcdir, round)

    lfn = dfcdir + '/' + filename

    result = fcc.isFile(lfn)
    if result['OK']:
        if lfn in result['Value']['Successful'] and result['Value']['Successful'][lfn]:
            print >>sys.stderr, 'File already in DFC: %s' % lfn
            return

    uploadFile(fullpath, lfn, se)

    setMeta(run, lfn)

def putByDb(runmin, runmax, basedir, se):
    import MySQLdb

    sql = 'SELECT RunNo,FilePath,FileName FROM RanTrgData WHERE RunNo>=%s AND RunNo<=%s' % (runmin, runmax)
    connection = MySQLdb.connect(user='guest', passwd='guestpass', host='bes3db2.ihep.ac.cn', db="offlinedb")
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    print 'There are %s files to be uploaded' % len(rows)

    i = 0
    for row in rows:
        run = row[0]
        filepath = row[1]
        filename = row[2]
        uploadAndRegister(run, filepath, filename, basedir, se)
        uploadAndRegister(run, filepath, filename+'.idx', basedir, se)
        i += 1
        print 'File %s/%s uploading OK' % (i, len(rows))

    cursor.close()
    connection.close()

def putByList(listfile, basedir, se):
    f = open(listfile, 'r')
    linenumber = len(f.readlines())

    f.seek(0, 0)
    i = 0
    for line in f:
        line = line.rstrip()
        filepath = os.path.dirname(line)
        filename = os.path.basename(line)
        tmp = filename.split('_')
        run = int(tmp[1])

        uploadAndRegister(run, filepath, filename, basedir, se)
        i += 1
        print 'File %s/%s uploading OK' % (i, linenumber)
        sys.stdout.flush()
    f.close()


def main():
    runmin = 0
    runmax = 0
    listfile = ''
    se = 'IHEPD-USER'
    basedir = '/bes/File/randomtrg'
    for option in options:
        (switch, val) = option
        if switch == 'r' or switch == 'runmin':
            runmin = int(val)
        if switch == 'R' or switch == 'runmax':
            runmax = int(val)
        if switch == 'l' or switch == 'list':
            listfile = val
        if switch == 'e' or switch == 'se':
            se = val
        if switch == 'b' or switch == 'basedir':
            basedir = val

    if listfile:
        putByList(listfile, basedir, se)
    else:
        putByDb(runmin, runmax, basedir, se)


if __name__ == "__main__":
    main()
