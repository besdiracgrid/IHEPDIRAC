#!/usr/bin/env python

import os,sys,time
from DIRAC.Core.Base import Script
Script.initialize()

from DIRAC.Core.Utilities.ReturnValues import returnSingleResult

from DIRAC.DataManagementSystem.Client.FileCatalogClientCLI import FileCatalogClientCLI
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Core.Security.ProxyInfo import getProxyInfo

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC import gLogger,S_OK,S_ERROR

from IHEPDIRAC.Badger.DataLoader.DFC.readAttributes import DataAll,Others
from IHEPDIRAC.Badger.DataLoader.DFC.judgeType import judgeType
"""This is the public API for BADGER, the BESIII Advanced Data ManaGER.

   BADGER wraps the DIRAC File Catalog and related DIRAC methods for 
   use in the BESIII distributed computing environment.


"""

class Badger:

    def __init__(self, fcClient = False):
        """Internal initialization of Badger API.
        """       
        if not fcClient:
            _fcType = 'DataManagement/FileCatalog'
            self.client = FileCatalogClient(_fcType)
        else:
            self.client = fcClient
        #self.besclient = FileCatalogClient('DataManagement/DatasetFileCatalog')
    def __createQuery(metaSelections):
        """
        need the function then,need not import
        from DIRAC.DataManagementSystem.Client.FileCatalogClientCLI import FileCatalogClientCLI
        TODO...
        """
        pass

    def getDatasetNamePrefix(self):
        """descide the prefix of a datasetName"""
        prefix = ''
        result = getProxyInfo(False,False)
        if result['OK']:
          userGroup = result['Value']['group']
          if userGroup=='bes_user':
            prefix = 'User_'
          elif userGroup=='production':
            prefix = 'Proc_'
          return prefix
        else:
          return prefix

        
    def getFilenamesByLocaldir(self,localDir):
        """ get all files under the given dir
        example:getFilenamesByLocaldir("/bes3fs/offline/data/663-1/4260/dst/121215/")
        result = [/bes3fs/offline/data/663-1/4260/dst/121215/filename1,
                  /bes3fs/offline/data/663-1/4260/dst/121215/filename2,
                  ...
                  ] 
        """
        fileList = []
        for rootdir,subdirs,files in os.walk(localDir):
          for name in files:
            fullPath = os.path.join(rootdir,name)
            fileList.append(fullPath)
        fileList.sort()
        return fileList

    def __getFileAttributes(self,fullPath):
        """ get all attributes of the given file,return a attribute dict.
        """
        if os.path.exists(fullPath):
          type = judgeType(fullPath)
          if type=="all":
            obj = DataAll(fullPath)
          elif type=="others":
            obj = Others(fullPath)
          elif type==None:
            errorMes= "name if %s is not correct"%fullPath
            print "cannot get attributes of %s"%fullPath
            attributes = {}
            return attributes
            #raise TypeError(errorMes)
          attributes = obj.getAttributes()
          #attributes['date'] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        else:
          attributes = {}

        return attributes

    def testFunction(self):
        result = self.__getFileAttributes('/besfs2/offline/data/664-1/jpsi/dst/090613/run_0009952_All_file019_SFO-2.dst')
        print result

    def __registerDir(self,dir):
        """Internal function to register a new directory in DFC .
           Returns True for success, False for failure.
        """
        fc = self.client
        result = fc.createDirectory(dir)
        if result['OK']:
            if result['Value']['Successful']:
                if result['Value']['Successful'].has_key(dir):
                    return S_OK() 
                elif result['Value']['Failed']:
                    if result['Value']['Failed'].has_key(dir):
                        print 'Failed to create directory %s:%s'%(dir,result['Value']['Failed'][dir])
                        return S_ERROR(result) 
            else:
              return S_ERROR(result)
        else:
            print 'Failed to create directory %s:%s'%(dir,result['Message'])
            return S_ERROR(result) 
    def __registerFileMetadata(self,lfn,attributes):
        """Internal function to set metadata values on a given lfn. 
          Returns True for success, False for failure.
        """
        metadataDict = {}
        metadataDict['runL'] = attributes['runL']
        metadataDict['runH'] = attributes['runH']
        metadataDict['status'] = attributes['status']
        metadataDict['eventNumber'] = attributes['eventNumber']
        metadataDict['count'] = attributes['count']
        result = self.client.setMetadata(lfn,metadataDict)
        if not result['OK']:
          return S_ERROR() 
        else:
          return S_OK() 

    def __registerDirMetadata(self,dir,metaDict):
        """Internal function to set metadata to a directory
           Returns True for success, False for failure.
        """
        fc = self.client
        result = fc.setMetadata(dir,metaDict)
        if result['OK']:
            return S_OK() 
        else:
            message = "Error for setting metadata %s to %s: %s"%(metaDict,dir,result['Message'])
            return S_ERROR('Message') 
        
    def __dirExists(self,dir,parentDir):
        """ Internal function to check whether 'dir' is the subdirectory of 'parentDir'
            Returns 1 for Yes, 0 for NO
        """
        fc = self.client
        dir_exists = 0
        result = fc.listDirectory(parentDir)
        if result['OK']:
            for i,v in enumerate(result['Value']['Successful'][parentDir]['SubDirs']):
                if v == dir: 
                    dir_exists = 1
                    break
        else:
            print 'Failed to list subdirectories of %s:%s'%(parentDir,result['Message'])
        
        return dir_exists

    def __registerSubDirs(self,dirs_dict,dirs_meta):
        """Internal function to create directories in dirs_dict
           Returns True for sucess, False for failure
        """
        creation_ok = True
        
        for dir in dirs_dict:
            if (dir != 'dir_file')&(dir !='dir_data_mc' ):
                if self.__registerDir(dirs_meta[dir][0])['OK']:
                    result = self.__registerDirMetadata(dirs_meta[dir][0],{dir.split('_')[1]:dirs_meta[dir][1]})
                    if not result['OK']:
                        creation_ok = False
                        break
                else:
                    print 'Failed to create %s'%dir
                    creation_ok = False
                    break
            else:
                if not self.__registerDir(dirs_meta[dir])['OK']:
                    print 'Failed to create %s'%dir
                    creation_ok = False
                    break

        return creation_ok
            
    def registerHierarchicalDir(self,metaDict,rootDir='/bes'):
        """
           Create a hierarchical directory according the metadata dictionary
           Return created directory  for sucess,if this directory has been created, return this existing directory .

           Structure of the hierarchical directory:
           for real data:/bes/File/resonance/boss version/data/eventType/round
           for mc data:/bes/File/resonance/boss version/mc/eventType/round/streamId
           The eventType of all real datas is all. 

           Example:
           >>>metaDict = {'dataType': 'dst', 'eventType': 'all', 'streamId': 'stream0','resonance': 'psipp', 'round':'round01','bossVer': '6.6.1'}
           
           >>>badger.registerHierarchicalDir(metaDic)
           1
        """
        #Save about 20 lines compared with last one
        fc = self.client
        
        dir_exists = 0
        #0 for failure,1 for success,2 for existing directory
        creation_OK = 0
        lastDirMetaDict = {'dataType':metaDict['dataType'],'streamId':metaDict['streamId']}

        dir_file = rootDir + '/File'
        dir_resonance = dir_file + '/' + metaDict['resonance']
        dir_bossVer = dir_resonance + '/' + metaDict['bossVer']

        if metaDict['streamId'] == 'stream0':
            dir_data_mc = dir_bossVer + '/data'
        else:
            dir_data_mc = dir_bossVer + '/mc'
        dir_eventType = dir_data_mc + '/' +metaDict['eventType']
        dir_round = dir_eventType + '/' + metaDict['round']
        dir_streamId = dir_round + '/' + metaDict['streamId']

        # if dir_round has been created,create_round=1 
        create_round = 0

        dirs_dict = ['dir_file','dir_resonance','dir_bossVer','dir_data_mc','dir_eventType','dir_round']
        dirs_meta = {'dir_file':dir_file,'dir_data_mc':dir_data_mc,'dir_resonance':[dir_resonance,metaDict['resonance']],'dir_bossVer':[dir_bossVer,metaDict['bossVer']],'dir_eventType':[dir_eventType,metaDict['eventType']],'dir_round':[dir_round,metaDict['round']]}
        dir_exists = self.__dirExists(dir_file,rootDir)
        if not dir_exists:
            result = self.__registerSubDirs(dirs_dict,dirs_meta)
            if result:
                create_round = 1
        else:
            dir_exists = self.__dirExists(dir_resonance,dir_file)
            if not dir_exists:
                dirs_dict = dirs_dict[1:]
                result = self.__registerSubDirs(dirs_dict,dirs_meta)
                if result:
                    create_round = 1
            else:
                dir_exists = self.__dirExists(dir_bossVer,dir_resonance)
                if not dir_exists:
                    dirs_dict = dirs_dict[2:]
                    result = self.__registerSubDirs(dirs_dict,dirs_meta)
                    if result:
                        create_round = 1
                else:
                    dir_exists = self.__dirExists(dir_data_mc,dir_bossVer)
                    if not dir_exists:
                        dirs_dict = dirs_dict[3:]
                        result = self.__registerSubDirs(dirs_dict,dirs_meta)
                        if result:
                            create_round = 1
                    else:
                        dir_exists = self.__dirExists(dir_eventType,dir_data_mc)
                        if not dir_exists:
                            dirs_dict = dirs_dict[4:]
                            result = self.__registerSubDirs(dirs_dict,dirs_meta)
                            if result:
                                create_round = 1
                        else:
                            dir_exists = self.__dirExists(dir_round,dir_eventType)
                            if not dir_exists:
                                dirs_dict = dirs_dict[5:]
                                result = self.__registerSubDirs(dirs_dict,dirs_meta)
                                if result:
                                    create_round = 1
                            else:
                                create_round = 1
        
        if create_round:
            if metaDict['streamId'] != "stream0":
                dir_exists = self.__dirExists(dir_streamId,dir_round)
                if not dir_exists:
                    if self.__registerDir(dir_streamId)['OK']:
                        result = self.__registerDirMetadata(dir_streamId,{'streamId':metaDict['streamId']})
                        if result['OK']:
                            result = self.__registerDirMetadata(dir_streamId,lastDirMetaDict)
                            if result['OK']:
                                creation_OK = 1
                else:
                    creation_OK = 2
            else:
                result = self.__registerDirMetadata(dir_round,lastDirMetaDict)
                if result['OK']:
                    creation_OK = 1
    
        if (creation_OK==1)|(creation_OK==2):
            if metaDict['streamId'] == "stream0":
                return dir_round
            else:   
                return dir_streamId
##########################################################################################
    #dir options
    def removeDir(self,dir):
        """remove the dir include files and subdirs
        """
        result = self.client.listDirectory(dir)
        if result['OK']:
            if not result['Value']['Successful'][dir]['Files'] and not result['Value']['Successful'][dir]['SubDirs']:
                #print 'no file and subDirs in this dir'
                self.client.removeDirectory(dir)
                return S_OK()
            else:
                if result['Value']['Successful'][dir]['Files']:
                    for file in result['Value']['Successful'][dir]['Files']:
                        self.client.removeFile(file)
                else:
                    for subdir in result['Value']['Successful'][dir]['SubDirs']:
                        self.removeDir(subdir)
                    self.removeDir(dir)

    def listDir(self,dir):
        """list the files under the given DFC dir"""
        fileList = []
        result = self.client.listDirectory(dir)
        if result['OK']:
          if result['Value']['Successful'][dir]['Files']:
            fileList = result['Value']['Successful'][dir]['Files'].keys()
            fileList.sort()
        else:
          print "no files under this dir"
        return fileList 

    def getDirMetaVal(self,dir):
      """list the registed metadata value of the given dir"""
      result = self.client.getDirectoryMetadata(dir)
      if result['OK']:
        return result['Value']
      else:
        print "Failed to get meta Value of the directory"
        return {}


    #################################################################################
    # meta fields operations
    #
    def addNewFields(self,fieldName,fieldType,metaType='-d'):
      """add new fields,if metaType is '-f',add file field,
        fileType is datatpye in MySQL notation
      """
      result = self.client.addMetadataField(fieldName,fieldType,metaType)
      if not result['OK']:
        return S_ERROR(result)
      else:
        return S_OK()

    def deleteMetaField(self,fieldName):
      """delete a exist metafield"""
      result = self.client.deleteMetadataField(fieldName)
      if not result['OK']:
        return S_ERROR(result)
      else:
        return S_OK()

    def getAllFields(self):
        """get all meta fields,include file metafield and dir metafield.
        """
        result = self.client.getMetadataFields()
        if not result['OK']:
          return S_ERROR(result['Message'])
        else:
          return result['Value']

    def registerFileMetadata(self,lfn,metaDict):

        """Add file level metadata to an entry
           True for success, False for failure
           (maybe used to registerNewMetadata
           Example:
           >>>lfn = '/bes/File/psipp/6.6.1/data/all/exp1/run_0011414_All_file001_SFO-1'
           >>>entryDict = {'runL':1000,'runH':898898}
           >>>badger.registerFileMetadata(lfn,entryDict)
           True
        """
        fc = self.client
        result = fc.setMetadata(lfn,metaDict)
        if result['OK']:
            return S_OK() 
        else:
            print 'Error:%s'%(result['Message'])
            return S_ERROR(result['Message'])
    #####################################################################
    # File Options
    def registerFile(self,lfn,dfcAttrDict):
        """Register a new file in the DFC.
        
        """
        #TODO:need more tests,if directory of file doesn't exist,
        #addFile will create it without setting any metadata(lin lei)
        #need to check whether directory of file exists in dfc?(lin lei) 
        #pass
        fc = self.client
        result = fc.addFile({lfn:dfcAttrDict})
        if result['OK']:
            if result['Value']['Successful']:
                if result['Value']['Successful'].has_key(lfn):
                    return S_OK()
            elif result['Value']['Failed']:
                if result['Value']['Failed'].has_key(lfn):
                    print 'Failed to add this file:',result['Value']['Failed'][lfn]
                    return S_ERROR()
        else:
            print 'Failed to add this file :',result['Message']
            return S_ERROR()
        # need to register file (inc. creating appropriate directory
        # if it doesn't already exist; and register metadata for that
        # file / directory
        # Q: how / where to pass the metadata?

    def getFileMetaVal(self,lfn):
      """get the File Meta Value of given file

      """
      result = self.client.getFileUserMetadata(lfn)
      if result['OK']:
        return result['Value']
      else:
        print "Failed to get meta Value of this file"
        return {}
    
    def reCalcCount(self,fileList,plus=True):
      """calculate the value of metadata 'count',when a file contain in a dataset
      count+1,when del a dataset,then all file in this dataset count -1
      default plus=True,means count+1,if count-1,set plus=False 
      return the value of count, count = -1 means error.
      NOTE:this function should only be called when create or delete a dataset.
      """
      countDict = {}
      if type(fileList)!=type([]):
        fileList = [fileList]
      for file in fileList:
        result =  self.getFileMetaVal(file)
        if len(result)!=0:
          count = result['count']
          if plus:
            count +=1
          else:
            if count>0:
              count -=1
          cDict = {'count':count}
          self.registerFileMetadata(file,cDict)
          countDict[file] = count
        else:
          print "Failed reCalculate value of count of file %s"%file
          countDict[file] = -1 

      return countDict

    def removeFile(self,lfn):
        """remove file on DFC
        """
        result = self.client.removeFile(lfn)
        if not result['OK']:
          return S_ERROR(result)
        else:
          return S_OK()

    def getPFN(self,lfn):
        """get replicas by lfn"""
        result = self.client.getReplicas(lfn)
        #print result
        if not result['OK']:
          return S_ERROR(result)
        else:
          return S_OK(result['Value']['Successful'][lfn]['IHEPD-USER'])

    def getSize(self,lfns):
        """get the size of the given lfn"""
        result = self.client.getFileSize(lfns)
        if result['OK']:
          if result['Value']['Successful']:
            retVal= result['Value']['Successful']
        else:
          retVal = {} 
        return retVal 

    def getFilesByMetadataQuery(self, query):
        """Return a list of LFNs satisfying given query conditions.

           Example usage:
           >>> brunH_GT_29756adger.getFilesByMetadataQuery('resonance=jpsi bossVer=6.5.5 round=exp1')
           ['/bes/File/jpsi/6.5.5/data/all/exp1/file1', .....]

        """
        #TODO: checking of output, error catching


        fc = self.client
        #TODO: calling the FileCatalog CLI object and its private method
        # is not a good way of doing this! but use it to allow construction of
        # the query meantime, until createQuery is made a public method
        cli = FileCatalogClientCLI(fc)
        metadataDict = cli._FileCatalogClientCLI__createQuery(query)
        result = fc.findFilesByMetadata(metadataDict,'/')
        if result['OK']:
            lfns = fc.findFilesByMetadata(metadataDict,'/')['Value']
            lfns.sort()
            return lfns
        else:
            print "ERROR: No files found which match query conditions."
            return None

    def uploadAndRegisterFiles(self,fileList,SE='IHEPD-USER',guid=None,ePoint=''):
        """upload a set of files to SE and register it in DFC.
        user input the directory of localfile.
        argument:
          ePoint is the energy point,for scan data
        we can treat localDir as a kind of datasetName.
        """          

        result_OK = 1
        errorList = []
        #fileList = self.getFilenamesByLocaldir(localDir)
        for fullpath in fileList:
          #get the attributes of the file
          fileAttr = self.__getFileAttributes(fullpath)
          if len(fileAttr) ==0:
            print "failed to get file %s attributes"%fullpath
            return S_ERROR("failed to get file attributes")
          #create dir and set dirMetadata to associated dir
          lastDir = self.registerHierarchicalDir(fileAttr,rootDir='/bes')
          dirMeta = self.getDirMetaVal(lastDir)
          if not (dirMeta.has_key("jobOptions") or dirMeta.has_key("description")):
            lastDirMetaDict = {}
            lastDirMetaDict['jobOptions'] = fileAttr['jobOptions']
            lastDirMetaDict['description'] = fileAttr['description']
            try:
              self.__registerDirMetadata(lastDir,lastDirMetaDict)
            except:
              pass
          if len(ePoint):
            lastDir = lastDir + os.sep + ePoint
          lfn = lastDir + os.sep + fileAttr['LFN']
          #upload and register file. 
          dirac = Dirac()
          result = dirac.addFile(lfn,fullpath,SE,guid,printOutput=True)
          #register file metadata
          if not result['OK']:
            print 'ERROR %s'%(result['Message'])
            #return S_ERROR(result['Message']) 
            errorList.append(fullpath)
            result_OK = 0
          else:
            result = self.__registerFileMetadata(lfn,fileAttr)
            if not result['OK']:
              result_OK = 0
              print "failed to register file metadata"
        if result_OK:
          return S_OK()
        else:
          return S_ERROR(errorList)

    def downloadFilesByFilelist(self,fileList,destDir=''):
        """downLoad a set of files form SE.
        use getFilesByFilelist() get a list of lfns and download these files.
        fileList get from function getFilesByDatesetName()

           Example usage:
           >>>badger.downloadFilesByFilelist(fileList)
        """
        errorDict = {}
        dirac = Dirac()
        #fileList = self.getFilesByDatasetName(dataset_name)
        for lfn in fileList:
          result = dirac.getFile(lfn,destDir,printOutput = False)
          if not result['OK']:
            errorDict[lfn] = result['Message']
        if errorDict:
          serr = S_ERROR()
          serr["errorDict"] = errorDict
          return serr
        else:
          return S_OK("File download successfully.") 

    ####################################################################
    # dataset functions
    #
    def registerDataset(self, datasetName,path,conditions):
        """Register a new dataset in DFC. Takes dataset name and string with
           conditions for new dataset as arguments.
           datasetname format:  
           resonance_BossVer_eventtype_round_runL_runH_stream0_datatype
           resonance_BossVer_eventtype_round_runL_runH_streamID_datatype
           type(conditions) is str,like "resonance=jpsi bossVer=655 round=round1"
        """
        fc = self.client
        cli = FileCatalogClientCLI(fc)
        metadataDict = cli._FileCatalogClientCLI__createQuery(conditions)
        metadataDict['Path'] = path 
        result = fc.addDataset(datasetName, metadataDict)
        if not result['OK']:
            print ("Error: %s" % result['Message'])
            return S_ERROR()
        else:
            print "Added dataset %s with conditions %s" % (datasetName, conditions)
            return S_OK()
        
    def getDatasetDescription(self, datasetName):
        """Return a string containing a description of metadata with which 
           the given dataset was defined.
           Example usage:
           >>> result = badger.getDatasetDescription('psipp_661_data_all_exp2')
        """
        result = self.client.getDatasetParameters(datasetName)
        if not result['OK']:
          print "ERROR: failed to get status of dataset:", result['Message']
        else:
          parDict = result['Value']
        for par,value in parDict.items():
          print par.rjust(20),':',value 

    def getDatasetMetadata(self, datasetName):
        """Return a dict containing a description of metadata with which 
           the given dataset was defined.
           Example usage:
           >>> result = badger.getDatasetMetadata('psipp_661_data_all_exp2')
        """
        result = self.client.getDatasetParameters(datasetName)
        if not result['OK']:
          print "ERROR: failed to get status of dataset:", result['Message']
          return S_ERROR(result['Message'])
        else:
          parDict = result['Value']
          return S_OK(parDict)

    def removeDataset(self,datasetName):
        """remove a dataset
        """
        result = self.client.removeDataset(datasetName)
        if not result['OK']:
          print "ERROR: failed to remove dataset:", result['Message']
        else:
          print "Successfully removed dataset", datasetName

    def checkDataset(self,datasetName):
        """ check if the dataset parameters are still valid
        """
        result = self.client.checkDataset( datasetName )
        if not result['OK']:
          print "ERROR: failed to check dataset:", result['Message']
        else:
          changeDict = result['Value']
          if not changeDict:
            print "Dataset is not changed"
          else:
            print "Dataset changed:"
            for par in changeDict:
              print "   ",par,': ',changeDict[par][0],'->',changeDict[par][1]
    def updateDataset(self,datasetName):
          """ Update the given dataset parameters
          """
          result = self.client.updateDataset( datasetName )
          if not result['OK']:
            print "ERROR: failed to update dataset:", result['Message']
          else:
            print "Successfully updated dataset", datasetName            

    def freezeDataset(self,datasetName):
        """ Freeze the given dataset
        """
        result = self.client.freezeDataset( datasetName )
        if not result['OK']:
          print "ERROR: failed to freeze dataset:", result['Message']
        else:
          print "Successfully frozen dataset", datasetName      

    def releaseDataset(self,datasetName):
        """ Release the given dataset
        """
        result = self.client.releaseDataset( datasetName )
        if not result['OK']:
          print "ERROR: failed to release dataset:", result['Message']
        else:
          print "Successfully released dataset", datasetName    

    def getFilesByDatasetName(self, datasetName):
        """Return a list of LFNs in the given dataset.
           
           Example usage:
           >>> badger.getFilesByDatasetName('psipp_661_data_all_exp2')
           ['/bes/File/psipp/6.6.1/data/all/exp2/file1', .....]
        """

        fc = self.client
        result = returnSingleResult(fc.getDatasetFiles(datasetName))
        if result['OK']:
          lfns = result['Value']
          lfns.sort()
          return S_OK(lfns)
        else:
          print "ERROR: Dataset", datasetName," not found"
          return S_ERROR(result)
            

    def listDatasets(self):
        """list the exist dataset"""
        datasetName = ''
        result = self.client.getDatasets( datasetName )
        if not result['OK']:
          print "ERROR:failed to get datasets"
          return
        
        datasetDict = result['Value']
        for dName in datasetDict.keys():
          print dName




    def checkDatasetIntegrity():
        pass


