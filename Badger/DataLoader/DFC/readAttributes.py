
# -*- coding:utf-8 -*-
#data 13/07/26
#for data/all  name of file like run_0023454_All_file014_SFO-2.dst
#for data/skim & mc, we use new file naming rule,
#file name like resonance_eventType_streamId_runL_runH_*.dst

import os
import os.path
import string
import re

def get_module_dir():
  return os.path.dirname( os.path.abspath(__file__) )

from DIRAC.Core.Base import Script
from DIRAC import S_OK,S_ERROR
Script.parseCommandLine( ignoreErrors = True )
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
    
#judge format of file
class JudgeFormat(Exception):
    def __init__(self, format):
        self.format = format
    def __str__(self):
        return repr("the File's format is not ",self.format)

#type of srcformat is list,it includes many formats
def checkFormat(srcformat,file):
    flag = 0
    for format in srcformat:
        #if format of file is in srcformat
        if  file.endswith(format):
            flag = 1
    return flag
                
        
#dstfile like /bes3fs/offline/data/655-1/4040/dst/110504/run_0023474_All_file007_SFO-2.dst
def getLFN(dstfile,format=[".dst",".tag"]):
    flag = checkFormat(format,dstfile)

    if flag==0:
        raise JudgeFormat(format)
        return
    #split dstfile by "/",then get "lfn.dst"    
    items=dstfile.split("/")
    length=len(items)

    filename=items[length-1]
    
    #split "*.dst" by "."
    #get lfn
    #lfn = filename.split('.')[0]
    lfn = filename

    return lfn

#get size of dst file
def getFileSize(dstfile,format = [".dst",".tag"]):
    flag = checkFormat(format,dstfile)
    
    if flag==0:
        raise JudgeFormat(format)
        return
    
    if os.path.exists(dstfile):
        #get file's size
        return os.path.getsize(dstfile)


#lfn like resonance_eventType_streamId_runL_runH_*,get attributes:resonance,eventType,streamId,runL,runH 
#lfn like run_0009947_All_file001_SFO-1,get attribute runId
def splitLFN(lfn,type):
    result = {}
        
    items = lfn.split("_")

    if type == "all":
        if items[2] == "All":
            runId = string.atoi(items[1])
            return runId
    
    else:        
        result["resonance"] = filter(str.isalpha,items[0])
        result["streamId"] = items[1]
        result["runL"] = int(filter(str.isdigit,items[2]))
        result["runH"] = int(filter(str.isdigit,items[2]))
   
        return result
    


#get runIdList from JobOptions
def getRunIdList(jobOptions):
    result = {}    
    runIdList = []
    str1=jobOptions[0]
    pat = re.compile(r'RunIdList= {-\d+(,-?\d+)+}')
    res1 = pat.search(str1)
    
    if res1 is not None:
        #get a string like:RunIdList={-10513,0,-10629}
        str2 = res1.group()

        result["description"] = str2
        pat = re.compile(r'-\d+(,-?\d+)+')
        list = pat.search(str2)
        
        if list is not None:
            #get a string like:-10513,0,-10629
            runIds = list.group()

            #split runIds according ','
            items=runIds.split(',')

            #members' style in items is string,we need to change their style to integer
            for i in items:
                if i!='0':
                    runid=abs(string.atoi(i))
                    runIdList.append(runid)

            result["runIdList"] = runIdList

    return result
        
        
        
#get Boss version, runid, Entry number, JobOptions from root file
def getCommonInfo(dstfile):
    
    import subprocess
    p = subprocess.Popen(["bash", os.path.join(get_module_dir(), "get_info.sh"), dstfile], stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
    output = p.communicate()[0].strip()
    pos = output.find("{")
    if pos >= 0:
      return eval( output[pos:] )

#get bossVer,eventNumber,dataType,fileSize,name,eventType,expNum,
#resonance,runH,runL,status,streamId,description
class DataAll(object):
    def __init__(self,dstfile):
        self.dstfile = dstfile
    
    def getAttributes(self):
        #store all attributes
        attributes = {}
        runIds = []
        
        if getFileSize(self.dstfile)<5000:
            print "Content of this file is null:",self.dstfile
            return "error"
        else:
            attributes = getCommonInfo(self.dstfile)
            #attributes["fileSize"] = getFileSize(self.dstfile)
            #set values of attribute status,streamId,Description,...
            attributes["status"] = 1
            attributes["streamId"] = 'stream0' 
            attributes["description"] = ''
            attributes["LFN"] = getLFN(self.dstfile)
            attributes["eventType"] = "all"
            attributes["count"] = 0

            #NOTE, MUST RESET the follow two metadata
            attributes["round"] = "round06" 
            attributes["resonance"] = "xyz_4360scan"

            #get runId from filename
            runId = splitLFN(attributes["LFN"],"all")

            #compare runid of rootfile with runid in filename
            if attributes["runId"] == runId:
                runIds.append(attributes["runId"])
                #set RunH=RunId and RunL=RunId
                attributes["runH"] = attributes["runId"]
                attributes["runL"] = attributes["runId"]

            else:
                print "runId of %s,in filename is %d,in rootfile is %d"%\
                    (self.dstfile,lfnInfo["runId"],attributes["runId"])
                return "error"

            del attributes["runId"]
            #attributes["jobOptions"] = ""
            return attributes

#get resonance,runL,runH,streamId,LFN from file name
#file name like jpsi2009_stream001_run9996_file7.dst 
#get bossVer,runL,runH,eventNum by reading information from rootfile
class Others(object):
    def __init__(self,dstfile):
        self.dstfile = dstfile
        
    def getAttributes(self):
        attributes = {}
        lfnInfo = {}
        runIds = []

        if getFileSize(self.dstfile)<5000:
            print "Content of this file is null:",self.dstfile
            return "error"
        else:
            #get bossVer,datatype,eventNum,runId(equal runL,runH)
            attributes = getCommonInfo(self.dstfile)
            #attributes["fileSize"] = getFileSize(self.dstfile)
            attributes["LFN"] = getLFN(self.dstfile)
            attributes["status"] = 1
            attributes["count"] = 0
            #NOTE, MUST RESET the follow metadatas
            attributes["round"] = "round02" 
            attributes["resonance"] = "jpsi"
            attributes["eventType"] = "inclusive"

            #get resonance,streamId,runL,runH in filename by calling splitLFN function
            lfnInfo = splitLFN(attributes["LFN"],"others")
            attributes["resonance"] = lfnInfo["resonance"]
            attributes["streamId"] = lfnInfo["streamId"]
            #if runL is equal to runH,this file only has one runId
            if lfnInfo["runL"] == lfnInfo["runH"]:
                #if runId in filename also is equal to runId in rootfile
                #print "a_runId",attributes["runId"],lfnInfo["runL"],type(attributes["runId"]),type(lfnInfo["runL"])
                if attributes["runId"] == lfnInfo["runL"]:
                    runIds.append(attributes["runId"])
                    attributes["runL"] = attributes["runId"]
                    attributes["runH"] = attributes["runId"]
                    attributes["description"] = "null"
                else:
                    print "Error %s:in the filename,runL = runH = %s,but runId in the root file is %s"\
                        %(self.dstfile,lfnInfo["runL"],attributes["runId"])
                    return "error"
            else:
                #this dst file has several runIds,get them from JobOptions by calling getRunIdList function
                result = getRunIdList(attributes["jobOptions"])
                if result is not None:
                    runH = max(result["runIdList"])
                    runL = min(result["runIdList"])
                    attributes["runL"] = runL 
                    attributes["runH"] = runH
                    attributes["description"] = result["description"]
                    
            
            del attributes["runId"]
            #attributes["jobOptions"] = ""
            return attributes


if __name__=="__main__":
   import time
   client = FileCatalogClient()
   start = time.time()
   obj = DataAll("/bes3fs/offline/data/664p01/xyz/4230/dst/130402/run_0032239_All_file001_SFO-1.dst")
   #obj = Others("/besfs2/offline/data/664-1/jpsi/09mc/dst/jpsi2009_stream001_run10319_file1.dst")
   result = obj.getAttributes()
   print time.time()-start
   import pprint
   pprint.pprint(result)

