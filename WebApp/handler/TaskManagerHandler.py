
import json

from WebAppDIRAC.Lib.WebHandler import WebHandler, WErr, WOK, asyncGen
from DIRAC import gConfig, S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient
from IHEPDIRAC.WorkloadManagementSystem.Client.TaskClient import TaskClient

STATUS_ICON_MAP = {
  'Init' : 'Checking',
  'Ready' : 'Waiting',
  'Processing' : 'Running',
  'Finished' : 'Done',
  'Expired' : 'Bad',
  'Removed' : 'Deleted'
}

class TaskManagerHandler(WebHandler):

    AUTH_PROPS = "authenticated"

    @asyncGen
    def web_getSelectionData(self):
      sData = self.getSessionData()

      taskClient = TaskClient()

      callback = {}

      user = sData["user"]["username"]
      group = sData["user"]["group"]

      if user == "Anonymous":
        callback["status"] = [["Insufficient rights"]]
      elif 'JobAdministrator' in sData['user']['properties']:
        callback["status"] = [['Init'],['Ready'],['Processing'],['Finished'],['Expired'],['Removed']]
      elif 'NormalUser' in sData['user']['properties']:
        callback["status"] = [['Init'],['Ready'],['Processing'],['Finished'],['Expired']]

      if user == "Anonymous":
        callback["owner"] = [["Insufficient rights"]]
      elif 'NormalUser' in sData['user']['properties']:
        owner = [[user]]
      else:
        result = yield self.threadTask( taskClient.getTaskOwners )
        if result["OK"]:
          owner = []
          if len( result["Value"] ) > 0:
            for i in result["Value"]:
              owner.append( [str( i )] )
          else:
            owner = [["Nothing to display"]]
        else:
            gLogger.error( "getTaskOwners() return error: %s" % result["Message"] )
            owner = [["Error happened on service side"]]
      callback["owner"] = owner

      if user == "Anonymous":
        callback["ownerGroup"] = [["Insufficient rights"]]
      elif 'NormalUser' in sData['user']['properties']:
        ownerGroup = [[group]]
      else:
        result = yield self.threadTask( taskClient.getTaskOwnerGroups )
        if result["OK"]:
          ownerGroup = []
          if len( result["Value"] ) > 0:
            for i in result["Value"]:
              ownerGroup.append( [str( i )] )
          else:
            ownerGroup = [["Nothing to display"]]
        else:
            gLogger.error( "getTaskOwnerGroups() return error: %s" % result["Message"] )
            ownerGroup = [["Error happened on service side"]]
      callback["ownerGroup"] = ownerGroup

      self.finish( callback )

    @asyncGen
    def web_getTaskData( self ):
      taskClient = TaskClient()
      req = self.__request()

      condDict = req
      limit = self.numberOfJobs
      offset = self.pageNumber
      orderAttribute = self.globalSort[0][0] + ':' + self.globalSort[0][1]

      result = yield self.threadTask( taskClient.getTaskCount, condDict )
      if not result["OK"]:
        self.finish( {"success":"false", "result":[], "total":0, "error":result["Message"]} )
        return
      total = result['Value']

      result = yield self.threadTask( taskClient.listTask, condDict, limit, offset, orderAttribute )
      if not result["OK"]:
        self.finish( {"success":"false", "result":[], "total":0, "error":result["Message"]} )
        return

      fields = ['TaskID', 'TaskName', 'Status', 'Owner', 'OwnerGroup', 'OwnerDN', 'Site', 'JobGroup']
      values = []
      for data in result['Value']:
        value = {}
        for d in data:
          if d in fields:
            value[d] = data[d]
        value['IconStatus'] = STATUS_ICON_MAP[data['Status']]
        value['CreationTime'] = str( data['CreationTime'] )
        value['UpdateTime'] = str( data['UpdateTime'] )
        value['Progress'] = data['Progress']

        totalJobs = data['Progress'].get('Total', 0)
        deletedJobs = data['Progress'].get('Deleted', 0)
        value['Total'] = '%s/%s' % (totalJobs-deletedJobs, totalJobs)

        values.append( value )

      callback = {"success":"true", "result":values, "total":total, "date":None}
      self.finish( callback )

    @asyncGen
    def web_getTaskInfo( self ):
      taskClient = TaskClient()

      taskID = int( self.request.arguments["TaskID"][0] )

      result = yield self.threadTask( taskClient.getTaskInfo, taskID )
      if not result["OK"]:
        self.finish( {"success":"false", "result":'', "error":result["Message"]} )
        return

      allInfo = []
      for name, value in sorted(result['Value'].iteritems(), key=lambda d:d[0]):
        if type( value ) == type([]):
          value = ', '.join( value )
        allInfo.append( [name, str(value)] )

      callback = {"success":"true", "result":allInfo}
      self.finish( callback )

    @asyncGen
    def web_getTaskHistory( self ):
      taskClient = TaskClient()

      taskID = int( self.request.arguments["TaskID"][0] )

      result = yield self.threadTask( taskClient.getTaskHistories, taskID )
      if not result["OK"]:
        self.finish( {"success":"false", "result":'', "error":result["Message"]} )
        return

      histories = []
      for status, statusTime, description in result['Value']:
        histories.append( [status, str(statusTime), description] )

      callback = {"success":"true", "result":histories}
      self.finish( callback )

    @asyncGen
    def web_renameTask( self ):
      taskClient = TaskClient()

      taskID = int( self.request.arguments["TaskID"][0] )
      newName = self.request.arguments["NewName"][0]

      result = yield self.threadTask( taskClient.renameTask, taskID, newName )
      if not result["OK"]:
        self.finish( {"success":"false", "result":'', "error":result["Message"]} )
        return

      callback = {"success":"true", "result":result['Value']}
      self.finish( callback )

    @asyncGen
    def web_getTaskProgress( self ):
      taskClient = TaskClient()

      taskID = int( self.request.arguments["TaskID"][0] )

      result = yield self.threadTask( taskClient.getTaskProgress, taskID )
      if not result["OK"]:
        self.finish( {"success":"false", "result":'', "error":result["Message"]} )
        return

      statuses = ['Total', 'Done', 'Failed', 'Running', 'Waiting', 'Deleted']

      allProgress = []
      for status in statuses:
        allProgress.append( [status, result['Value'].get( status, 0 )] )

      callback = {"success":"true", "result":allProgress}
      self.finish( callback )

    @asyncGen
    def web_getTaskJobs( self ):
      taskClient = TaskClient()

      taskID = int( self.request.arguments["TaskID"][0] )

      result = yield self.threadTask( taskClient.getTaskJobs, taskID )
      if not result["OK"]:
        self.finish( {"success":"false", "result":'', "error":result["Message"]} )
        return

      callback = {"success":"true", "result":result['Value']}
      self.finish( callback )

    @asyncGen
    def web_getTaskJobList( self ):
      taskClient = TaskClient()

      taskID = int( self.request.arguments["TaskID"][0] )

      result = yield self.threadTask( taskClient.getTaskJobs, taskID )
      if not result["OK"]:
        self.finish( {"success":"false", "result":'', "error":result["Message"]} )
        return

      jobIDs = result['Value']

      RPC = RPCClient( "WorkloadManagement/JobMonitoring" )
      result = RPC.getJobsStatus( result['Value'] )
      if not result["OK"]:
        self.finish( {"success":"false", "result":'', "error":result["Message"]} )
        return

      jobStatuses = result['Value']
      jobList = []
      for jobID in jobIDs:
        if jobID in jobStatuses:
          jobList.append( [jobID, jobStatuses[jobID]['Status']] )
        else:
          jobList.append( [jobID, 'Deleted'] )

      callback = {"success":"true", "result":jobList}
      self.finish( callback )

    @asyncGen
    def web_getTaskJobsStatistics( self ):
      taskClient = TaskClient()

      taskID = int( self.request.arguments["TaskID"][0] )

      statistics = []

      statistics.append( ['==== Status ====', ''] )
      result = yield self.threadTask( taskClient.getTaskJobsStatistics, taskID, 'Status' )
      if result["OK"]:
        for s in result['Value']:
          statistics.append( [s, result['Value'].get( s, 0 )] )

      statistics.append( ['', ''] )

      statistics.append( ['==== Minor Status ====', ''] )
      result = yield self.threadTask( taskClient.getTaskJobsStatistics, taskID, 'MinorStatus' )
      if result["OK"]:
        for s in result['Value']:
          statistics.append( [s, result['Value'].get( s, 0 )] )

      statistics.append( ['', ''] )

      statistics.append( ['==== Application Status ====', ''] )
      result = yield self.threadTask( taskClient.getTaskJobsStatistics, taskID, 'ApplicationStatus' )
      if result["OK"]:
        for s in result['Value']:
          statistics.append( [s, result['Value'].get( s, 0 )] )

      statistics.append( ['', ''] )

      statistics.append( ['==== Site ====', ''] )
      result = yield self.threadTask( taskClient.getTaskJobsStatistics, taskID, 'Site' )
      if result["OK"]:
        for s in result['Value']:
          statistics.append( [s, result['Value'].get( s, 0 )] )

      statistics.append( ['', ''] )

      statistics.append( ['==== Job Group ====', ''] )
      result = yield self.threadTask( taskClient.getTaskJobsStatistics, taskID, 'JobGroup' )
      if result["OK"]:
        for s in result['Value']:
          statistics.append( [s, result['Value'].get( s, 0 )] )

      callback = {"success":"true", "result":statistics}
      self.finish( callback )

    @asyncGen
    def web_getTaskJobInfo( self ):
      taskClient = TaskClient()

      jobID = int( self.request.arguments["JobID"][0] )

      outFields = ['Info']
      result = yield self.threadTask( taskClient.getJobs, [jobID], outFields )
      if not ( result["OK"] and result['Value'] ):
        self.finish( {"success":"false", "result":'', "error":result["Message"]} )
        return

      info = json.loads( result['Value'][0][0] )

      allInfo = []
      for name, value in sorted(info.iteritems(), key=lambda d:d[0]):
        if type( value ) == type([]):
          value = ', '.join( value )
        allInfo.append( [name, str(value)] )

      callback = {"success":"true", "result":allInfo}
      self.finish( callback )

    @asyncGen
    def web_rescheduleTask( self ):
      taskClient = TaskClient()

      taskIDs = self.request.arguments["TaskID"][0].split( ',' )
      jobStatus = self.request.arguments["JobStatus"][0].split( ',' )
      if '' in taskIDs:
        taskIDs.remove( '' )
      if '' in jobStatus:
        jobStatus.remove( '' )
      taskIDs = [ int(taskID) for taskID in taskIDs ]

      for taskID in taskIDs:
        result = yield self.threadTask( taskClient.rescheduleTask, taskID, jobStatus )
        if not result["OK"]:
          self.finish( {"success":"false", "result":'', "error":result["Message"]} )
          return

      callback = {"success":"true", "result":taskIDs}
      self.finish( callback )

    @asyncGen
    def web_deleteTask( self ):
      taskClient = TaskClient()

      taskIDs = self.request.arguments["TaskID"][0].split( ',' )
      if '' in taskIDs:
        taskIDs.remove( '' )
      taskIDs = [ int(taskID) for taskID in taskIDs ]

      for taskID in taskIDs:
        result = yield self.threadTask( taskClient.deleteTask, taskID )
        if not result["OK"]:
          self.finish( {"success":"false", "result":'', "error":result["Message"]} )
          return

      callback = {"success":"true", "result":taskIDs}
      self.finish( callback )


    def __request( self ):
      self.pageNumber = 0
      self.numberOfJobs = 25
      self.globalSort = [["TaskID", "DESC"]]

      req = {}
      
      if self.request.arguments.has_key( "limit" ) and len( self.request.arguments["limit"][0] ) > 0:
        self.numberOfJobs = int( self.request.arguments["limit"][0] )
        if self.request.arguments.has_key( "start" ) and len( self.request.arguments["start"][0] ) > 0:
          self.pageNumber = int( self.request.arguments["start"][0] )
        else:
          self.pageNumber = 0

      if "TaskID" in self.request.arguments:
        taskids = list( json.loads( self.request.arguments[ 'TaskID' ][-1] ) )
        if len( taskids ) > 0:
          req['TaskID'] = taskids


      if "status" in self.request.arguments:
        status = list( json.loads( self.request.arguments[ 'status' ][-1] ) )
        if len( status ) > 0:
          req["Status"] = status

      if "owner" in self.request.arguments:
        owner = list( json.loads( self.request.arguments[ 'owner' ][-1] ) )
        if len( owner ) > 0:
          req["Owner"] = owner
      
      if "ownerGroup" in self.request.arguments:
        ownerGroup = list( json.loads( self.request.arguments[ 'ownerGroup' ][-1] ) )
        if len( ownerGroup ) > 0:
          req["OwnerGroup"] = ownerGroup
          
      if 'startDate' in self.request.arguments and len( self.request.arguments["startDate"][0] ) > 0:
        if 'startTime' in self.request.arguments and len( self.request.arguments["startTime"][0] ) > 0:
          req["FromDate"] = str( self.request.arguments["startDate"][0] + " " + self.request.arguments["startTime"][0] )
        else:
          req["FromDate"] = str( self.request.arguments["startDate"][0] )

      if 'endDate' in self.request.arguments and len( self.request.arguments["endDate"][0] ) > 0:
        if 'endTime' in self.request.arguments and len( self.request.arguments["endTime"][0] ) > 0:
          req["ToDate"] = str( self.request.arguments["endDate"][0] + " " + self.request.arguments["endTime"][0] )
        else:
          req["ToDate"] = str( self.request.arguments["endDate"][0] )

      if 'date' in self.request.arguments and len( self.request.arguments["date"][0] ) > 0:
        req["LastUpdate"] = str( self.request.arguments["date"][0] )

      if 'sort' in self.request.arguments:
        sort = json.loads( self.request.arguments['sort'][-1] )
        if len( sort ) > 0:
          self.globalSort = []
          for i in sort :
            self.globalSort += [[str( i['property'] ), str( i['direction'] )]]
      else:
        self.globalSort = [["TaskID", "DESC"]]

      gLogger.debug( "Request", str( req ) )
      return req
