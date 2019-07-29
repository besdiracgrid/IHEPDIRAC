__RCSID__ = "8f34a5d (2015-01-12 13:07:33 +0000) Xianghu Zhao <zhaoxh@ihep.ac.cn>"

from DIRAC                                            import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                      import AgentModule
from DIRAC.Core.DISET.RPCClient                       import RPCClient

from DIRAC.WorkloadManagementSystem.DB.JobDB          import JobDB
from IHEPDIRAC.WorkloadManagementSystem.DB.TaskDB      import TaskDB

import time

class TaskAgent( AgentModule ):
  """
      The specific agents must provide the following methods:
      - initialize() for initial settings
      - beginExecution()
      - execute() - the main method called in the agent cycle
      - endExecution()
      - finalize() - the graceful exit of the method, this one is usually used
                 for the agent restart
  """

  def initialize( self ):
    self.__taskDB = TaskDB()
    self.__jobDB = JobDB()
    return S_OK()

  def execute( self ):
    """ Main execution method
    """
    condDict = { 'Status': ['Ready', 'Processing', 'Finished'] }
    result = self.__taskDB.getTasks( [ 'TaskID', 'Status' ], condDict )
    if not result['OK']:
      return result

    tasks = result['Value']

    self.log.info( '%d tasks will be refreshed' % len(tasks) )

    for task in tasks:
      taskID = task[0]
      status = task[1]

      if status in ['Ready', 'Processing', 'Finished']:
        self.__refreshTask( taskID )

    return S_OK()


  def __refreshTask( self, taskID ):
    result = self.__refreshTaskStringAttribute( taskID, 'Site' )
    if result['OK']:
      self.log.debug( 'Task %d site is refreshed' % taskID )
    else:
      self.log.error( 'Task %d site refresh failed: %s' % ( taskID, result['Message'] ) )

    result = self.__refreshTaskStringAttribute( taskID, 'JobGroup' )
    if result['OK']:
      self.log.debug( 'Task %d job group is refreshed' % taskID )
    else:
      self.log.error( 'Task %d job group refresh failed: %s' % ( taskID, result['Message'] ) )

    result = self.__refreshTaskStatus( taskID )
    if result['OK']:
      self.log.debug( 'Task %d status is refreshed' % taskID )
    else:
      self.log.error( 'Task %d status refresh failed: %s' % ( taskID, result['Message'] ) )


################################################################################

  def __getTaskProgress( self, taskID ):
    result = self.__taskDB.getTaskJobs( taskID )
    if not result['OK']:
      return result
    jobIDs = result['Value']

    result = self.__jobDB.getAttributesForJobList( jobIDs, ['Status'] )
    if not result['OK']:
      return result
    statuses = result['Value']

    progress = { 'Total': 0, 'Done': 0, 'Failed': 0, 'Running': 0, 'Waiting': 0, 'Deleted': 0 }
    progress['Total'] = len(jobIDs)
    for jobID in jobIDs:
      if jobID in statuses:
        status = statuses[jobID]['Status']
        if status in ['Done']:
          progress['Done'] += 1
        elif status in ['Failed', 'Stalled', 'Killed']:
          progress['Failed'] += 1
        elif status in ['Running', 'Completed']:
          progress['Running'] += 1
        else:
          progress['Waiting'] += 1
      else:
        progress['Deleted'] += 1

    return S_OK( progress )

  def __analyseTaskStatus( self, progress ):
    totalJob = progress.get( 'Total', 0 )
    runningJob = progress.get( 'Running', 0 )
    waitingJob = progress.get( 'Waiting', 0 )
    deletedJob = progress.get( 'Deleted', 0 )

    status = 'Unknown'
    if deletedJob == totalJob:
      status = 'Expired'
    elif runningJob == 0 and waitingJob == 0:
      status = 'Finished'
    else:
      status = 'Processing'

    return status

  def __refreshTaskStatus( self, taskID ):
    """ Refresh the task status
    """
    # get task progress from the job list
    result = self.__getTaskProgress( taskID )
    if not result['OK']:
      return result
    progress = result['Value']
    self.log.debug( 'Task %d Progress: %s' % ( taskID, progress ) )
    result = self.__taskDB.updateTaskProgress( taskID, progress )
    if not result['OK']:
      return result

    # get previous task status
    result = self.__taskDB.getTaskStatus( taskID )
    if not result['OK']:
      return result
    status = result['Value']

    # get current task status from the progress
    newStatus = self.__analyseTaskStatus( progress )
    self.log.debug( 'Task %d new status: %s' % ( taskID, newStatus ) )
    if newStatus != status:
      self.__taskDB.updateTaskStatus( taskID, newStatus, 'Status refreshed' )
      if not result['OK']:
        return result

    return S_OK( newStatus )


################################################################################

  def __getTaskAttribute( self, taskID, attributeType ):
    """ Get all attributes of the jobs in the task
    """
    result = self.__taskDB.getTaskJobs( taskID )
    if not result['OK']:
      return result
    jobIDs = result['Value']

    condDict = { 'JobID': jobIDs }

    result = self.__jobDB.getDistinctJobAttributes( attributeType, condDict )
    if not result['OK']:
      return result
    attributes = result['Value']

    return S_OK( attributes )

  def __refreshTaskStringAttribute( self, taskID, attributeType ):
    """ Refresh the task attribute. The attribute type must be string and seperated by comma
    """
    # get task attibutes from the job list
    result = self.__getTaskAttribute( taskID, attributeType )
    if not result['OK']:
      return result
    newAttributes = result['Value']

    # get previous task attributes
    result = self.__taskDB.getTask( taskID, [attributeType] )
    if not result['OK']:
      return result
    oldAttributes = result['Value'][0].split( ',' )

    # check whether there are differences
    if set( newAttributes ) == set( oldAttributes ):
      self.log.debug( 'Task %s attribute is the same: %s' % (attributeType, oldAttributes) )
      return S_OK( oldAttributes )

    # make a combination of old and new attributes
    attributes = list( set( oldAttributes ) | set( newAttributes ) )
    for emptyAttr in [ '', 'ANY', 'Multiple' ]:
      if emptyAttr in attributes:
        attributes.remove( emptyAttr )

    # generate a new attribute
    allAttributes = ','.join( attributes )
    result = self.__taskDB.updateTask( taskID, [attributeType], [allAttributes] )
    if not result['OK']:
      return result

    return S_OK( allAttributes )
