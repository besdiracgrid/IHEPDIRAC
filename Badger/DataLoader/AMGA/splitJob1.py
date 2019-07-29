#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei


#from optparse import OptionParser
from argparse import ArgumentParser 
import os
import os.path
import time

#input directories of dst file,we can use getAllFiles to get all dst files underthese directories 
def getAllFiles(dstdir):
    dirs = []
    linenum = 0

    if os.path.exists(dstdir):
        f = open(dstdir,"r")
        allLines = f.readlines()
        f.close()

        for eachLine in allLines:
            dir = eachLine.strip()
            dirs.append(dir)
    else:
        print "Your file %s is not exit"%dstdir

    filename = dstdir + ".files"
    fp = open(filename,"w")
    for d in sorted(dirs):
        if os.path.isabs(d):
            files = os.listdir(d)
            for f in sorted(files):
                #only dst file can be written into the file that contains all dstfiles 
                if f.endswith(".dst"):
                    linenum += 1
                    fulldir = os.path.join(d,f)
                    fp.write(fulldir+'\n')
                else:
                    fulldir = os.path.join(d,f)
                    print "%s is not dst file"%fulldir
                    continue
                    
    fp.close()

    return linenum

#creat script which we can use to sub job 
def createSubScript(linefrm,lineto,flag,dst,num):
    curdir = os.getcwd()
    
    if flag == "dir":
        dstfile = dst + ".files"
        rootfile = dst + str(num) + ".root"
    elif flag == "file":
        dstfile = dst
        rootfile = dst + str(num) + ".root"

    #the most import python instruction,use this can execute control1.py 
    #to get attributes and upload this file to amga 
    executestr = "python "+curdir+"/control1.py -f "+str(linefrm)+" -o "+str(lineto)+" -d "+dstfile+" -r "+rootfile

    #get directory that stores subJobTemplate
    template = curdir + "/subJobTemplate"

    #set name of this subJob script
    job = dst + ".job" + str(num)
    #copy content of subJobTemplate to subjob script
    if os.path.exists(template):
        filefrm = open(template,"r")
        fileto = open(job,"w")
        for eachline in filefrm:
            #print eachline.strip()
            fileto.write(eachline)
        #write python instruction to subjob script
        fileto.write(executestr + "\n")

        filefrm.close()
        fileto.close()
    else:
        print"Programme subJobTemplate has not found in %s"%curdir
        
#split job according filestep
def splitJob_file(linenum,filestep,flag,dst):
    linefrm = 0
    lineto  = 0
    num = 0
    
    for i in range(0,linenum,filestep):
        linefrm = i
        lineto  = i + filestep

        if lineto > linenum:
            lineto = linenum
           
        createSubScript(linefrm,lineto,flag,dst,num)
        num += 1
        
#split job according jobnum
def splitJob_job(linenum,jobnum,flag,dst):
    linefrm = 0
    lineto = 0
    
    list1 = list(divmod(linenum,jobnum))
    
    filestep = list1[0]
    remainder = list1[1]
    print "linenum:",linenum
    print "filestep:",filestep
    print "remainder:",remainder

    for eachjob in range(jobnum):
        if eachjob != jobnum-1:
            linefrm = filestep * eachjob
            lineto = filestep * (eachjob + 1)
            
        else:
            linefrm = filestep * eachjob
            lineto = filestep * (eachjob + 1) + remainder

        createSubScript(linefrm,lineto,flag,dst,eachjob)

if __name__ =="__main__":

    parser =ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--dstdir',dest='dstdir',help='a text file that contains direcotries of dst files')
    group.add_argument('--dstfile',dest='dstfile',help='a text file that contains dst files')
    
    group1 = parser.add_mutually_exclusive_group(required=True)
    group1.add_argument('--filenum',dest='filenum', type=int,help='how many dst files of a small job')
    group1.add_argument('--jobnum',dest='jobnum', type=int,choices=range(1,16),help='a big job will be splited into how many small jobs')

    args = parser.parse_args()
    
    dstdir = args.dstdir
    dstfile = args.dstfile
    filenum = args.filenum
    jobnum = args.jobnum
    
    flag = "null"
    
    #if input is directories of dst files
    if dstdir:
        linenum = getAllFiles(dstdir)
        dst = dstdir
        flag = "dir"
    #if input is dst file
    elif dstfile:
        f = open(dstfile,"r")
        all = f.readlines()
        linenum = len(all)
        dst = dstfile
        flag = "file"

    #if input is filestep
    if filenum:
        print "filestep",filenum
        print "linenum",linenum
        #check whether job number is larger than 15
        res = divmod(linenum,filenum)
        if res[1]:
            n = res[0]+1
        else:
            n = res[0]
        
        if n > 15:
            print "Too many small jobs"
            exit(0) 
        splitJob_file(linenum,filenum,flag,dst)
    #if input is number of job
    elif jobnum:
        if jobnum>15:
            print "Too many small jobs"
            exit(0)
        splitJob_job(linenum,jobnum,flag,dst)
