#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei

from optparse import OptionParser
import os
import sys
import os.path
import uuid
import time
from itertools import islice
import string

parser=OptionParser()
parser.add_option('-f',dest='linefrm',help='which line will be handled first')
parser.add_option('-o',dest='lineto',help='which line will be handled at last')
parser.add_option('-d',dest='dstfiles',help='a text that contains dst files')
parser.add_option('-r',dest='rootfile',help='root file')

(options,args) = parser.parse_args()
linefrm = string.atoi(options.linefrm)
lineto = string.atoi(options.lineto)
dstfiles = options.dstfiles
rootfile = options.rootfile

#print "rootfile:",rootfile
 
from readAttributes import DataAll,Others
from insertToCatalogue import insert
from judgeType import judgeType


#total time = time of getting attributes + time of inserting into catalogue
totalTime = 0
insertTime = 0
#store number of files which have been inserted into catalogue
linenum = linefrm

totalStart = time.time()
if os.path.exists(dstfiles):
    file = open(dstfiles,"r")

    for f in islice(file,linefrm,lineto):
        dstfile = f.strip()
        #print "dstfile:",dstfile
        if os.path.exists(dstfile):
            type = judgeType(dstfile)
            #print "type of file %s is %s"%(dstfile,type)

            linenum = linenum+1

            if type=='all':
                obj = DataAll(dstfile,rootfile)
            elif type=='others':
                obj = Others(dstfile,rootfile)
            elif type==None:
                print "name of %s is not correct"%dstfile
                continue

            #get bossVer,eventNum,dataType,fileSize,LFN,eventType,expNum,
            #resonance,runH,runL,status,streamId,description
            attributes = obj.getAttributes()
        
            if attributes==0:
                continue
            else:
                #get Guid
                attributes["guid"] = uuid.uuid1()

                #get Date
                now = time.localtime()
                date = time.strftime('%Y-%m-%d %H:%M:%S',now)
                attributes["date"] = date  
                print 100*"#"
                #print attributes
            
                insertTime = insertTime+insert(attributes)
            
                
totalEnd = time.time()
totalTime = totalEnd-totalStart
num = linenum - linefrm

print "The number of files you have inserted :%d" %num
print "Time of inserting into catalogue is:%s" %str(insertTime)
print "Total time is:%s"%str(totalTime)
