#!/usr/bin/env python

import os
import ConfigParser

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base                                  import Script

Script.setUsageMessage('Create JUNO production')
Script.registerSwitch('', 'ini=', 'Ini file')
Script.registerSwitch('', 'dryrun', 'Do not submit transformation')
Script.parseCommandLine(ignoreErrors = False)

from DIRAC.Core.Security                              import CS
from DIRAC.Core.Security.ProxyInfo                    import getProxyInfo
from DIRAC.Resources.Catalog.FileCatalogClient        import FileCatalogClient

from DIRAC.TransformationSystem.Client.Transformation       import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac

DEBUG = True

URL_ROOT = 'http://dirac-code.ihep.ac.cn/juno/ts'


def _getNewStreamDir(outputPath):
  streamIDs = []
  fcc = FileCatalogClient()
  res = fcc.listDirectory(outputPath)
  if not res['OK']:
    return 1
  if res['Value']['Successful']:
    for fn in res['Value']['Successful'][outputPath]['SubDirs'].keys():
      try:
        streamIDs.append(int(os.path.basename(fn)))
      except ValueError:
        pass
  maxID = max(streamIDs) if streamIDs else 0
  return maxID + 1

def _setMetaData(directory, meta):
  fcc = FileCatalogClient()
  res = fcc.createDirectory(directory)
  if not res['OK']:
    raise Exception(res['Message'])
  res = fcc.setMetadata(directory, meta)
  if not res['OK']:
    raise Exception(res['Message'])

def _getMetaData(directory):
  fcc = FileCatalogClient()
  res = fcc.getDirectoryUserMetadata(directory)
  if not res['OK']:
    return {}
  return res['Value']


class Param(object):
  def __init__(self, configFile='', paramCmd={}):
    self.__configFile = configFile
    self.__paramCmd = paramCmd

    self.__param = {}
    self.__loadParam()
    self.__processParam()

    gLogger.debug('Final param: {0}'.format(self.__param))

  def __loadParam(self):
    if not self.__configFile:
      return
    config = ConfigParser.ConfigParser()
    config.optionxform = str
    config.read(self.__configFile)
    self.__param.update(dict(config.items('all')))
    self.__param.update(self.__paramCmd)
    if 'process' in self.__param:
      self.__param.update(dict(config.items(self.__param['process'])))
    self.__param.update(self.__paramCmd)

  def __processParam(self):
    def parseList(s):
      return s.strip().split()

    for key in ['softwareVersion', 'process', 'position']:
      if key not in self.__param or not self.__param[key]:
        raise Exception('Param "{0}" must be specified'.format(key))
    self.__param.setdefault('prodName', 'JUNOProd')
    self.__param.setdefault('outputLocation', 'user')
    self.__param.setdefault('outputSE', 'IHEP-STORM')
    self.__param.setdefault('outputMode', 'closest')
    self.__param.setdefault('seed', '0')
    self.__param.setdefault('workDir', self.__param['process'])

    self.__param['numberOfTasks'] = int(self.__param.get('njobs', '1'))
    self.__param['evtmax'] = int(self.__param.get('evtmax', '1'))
    self.__param['site'] = parseList(self.__param.get('site', ''))
    self.__param['workflow'] = parseList(self.__param.get('workflow', ''))

    if 'dryrun' in self.__param and self.__param['dryrun'].lower() in ['true', 'yes']:
      self.__param['dryrun'] = True
    else:
      self.__param['dryrun'] = False

    if 'tag' in self.__param:
      self.__param['tags'] = [self.__param['tag']]
    else:
      self.__param['tags'] = parseList(self.__param.get('tags', ''))

  @property
  def param(self):
    return self.__param


class ProdStep(object):
  def __init__(self, executable, transType, transGroup, softwareVersion, application, stepName='unknown', description='Production step',
               inputMeta={}, extraArgs='', inputData=None, outputPath='/juno/test/prod', outputSE='IHEP-STORM', outputPattern='*.root',
               isGen=False, site=None, outputMode='closest', maxNumberOfTasks=1):
    self.__executable = executable
    self.__transType = transType
    self.__transGroup = transGroup
    self.__softwareVersion = softwareVersion
    self.__application = application
    self.__stepName = stepName
    self.__description = description
    self.__inputMeta = inputMeta
    self.__extraArgs = extraArgs
    self.__inputData = inputData
    self.__outputPath = outputPath
    self.__outputSE = outputSE
    self.__outputPattern = outputPattern
    self.__isGen = isGen
    self.__site = site
    self.__outputMode = outputMode
    self.__maxNumberOfTasks = maxNumberOfTasks

  def createJob(self):
    job = Job()
    job.setName(self.__stepName)
    job.setOutputSandbox(['*log'])

    job.setExecutable('/usr/bin/wget', arguments='"{0}/{1}"'.format(URL_ROOT, self.__executable))
    job.setExecutable('/bin/chmod', arguments='+x "{0}"'.format(self.__executable))

    arguments = '"{0}" "{1}" "{2}" "{3}" "{4}" "{5}" @{{JOB_ID}}'.format(
        self.__softwareVersion, self.__application, self.__outputPath, self.__outputPattern, self.__outputSE, self.__outputMode)
    if self.__extraArgs:
      arguments += ' ' + self.__extraArgs
    job.setExecutable(self.__executable, arguments=arguments)

    if self.__inputData:
      job.setInputData(self.__inputData)

    if self.__site:
      job.setDestination(self.__site)

    self.__job = job

  def submitJob(self):
    dirac = Dirac()
    res = dirac.submitJob(self.__job)
    print res["Value"]
    return res

  def createTransformation(self):
    ########################################
    # Transformation definition
    ########################################
    t = Transformation()

    t.setTransformationName(self.__stepName)
    t.setType(self.__transType)
    t.setDescription(self.__description)
    t.setLongDescription(self.__description)
    if self.__isGen:
      t.setMaxNumberOfTasks(self.__maxNumberOfTasks)
    else:
      t.setGroupSize(1)
    if self.__transGroup:
      t.setTransformationGroup(self.__transGroup)
    # set the job workflow to the transformation
    t.setBody ( self.__job.workflow.toXML() )

    ########################################
    # Transformation submission
    ########################################
    res = t.addTransformation()
    if not res['OK']:
      raise Exception(res['Message'])

    t.setStatus("Active")
    t.setAgentType("Automatic")

    currtrans = t.getTransformationID()['Value']

    if self.__inputMeta and not self.__isGen:
      client = TransformationClient()
      res = client.createTransformationInputDataQuery(currtrans, self.__inputMeta)
      if not res['OK']:
        raise Exception(res['Message'])

    return str(currtrans)


class ProdChain(object):
  def __init__(self, param):
    self.__param = param
    self.__transIDs = {}

    self.__prodPrefix = '{0}{1}-{2}-{3}'.format(param['prodName'], param.get('nameSuffix', ''), param['softwareVersion'], param['workDir'])

    self.__ownerAndGroup()

    outputDir = self.__param['outputDir'].strip('/')
    if self.__param['outputLocation'] == 'production':
      self.__outputRoot = os.path.join(self.__voHome, outputDir)
    else:
      self.__outputRoot = os.path.join(self.__userHome, outputDir)

    self.__prepareDir()

    gLogger.notice('Owner: {0}'.format(self.__owner))
    gLogger.notice('OwnerGroup: {0}'.format(self.__ownerGroup))
    gLogger.notice('VO: {0}'.format(self.__vo))
    gLogger.notice('OutputRoot: {0}'.format(self.__outputRoot))
    gLogger.notice('ProdRoot: {0}'.format(self.__prodRoot))
    gLogger.notice('ProdPrefix: {0}'.format(self.__prodPrefix))

  def __ownerAndGroup(self):
    res = getProxyInfo(False, False)
    if not res['OK']:
      raise Exception(res['Message'])
    self.__owner = res['Value']['username']
    self.__ownerGroup = res['Value']['group']
    self.__vo = CS.getVOMSVOForGroup(self.__ownerGroup)
    self.__voHome = '/{0}'.format(self.__vo)
    self.__userHome = '/{0}/user/{1:.1}/{1}'.format(self.__vo, self.__owner)

  def __prepareDir(self):
    outputPath = self.__outputRoot
    for d in ['softwareVersion', 'workDir', 'position']:
      if d == 'workDir':
        key = 'process'
      else:
        key = d
      outputPath = os.path.join(outputPath, self.__param[d])
      _setMetaData(outputPath, {key: self.__param[key]})

    self.__prodRoot = outputPath

  def __getOutputPath(self, tag, application):
    return os.path.join(self.__prodRoot, tag, application)

  def __getTransID(self, tag, application):
    meta = _getMetaData(self.__getOutputPath(tag, application))
    if 'transID' not in meta:
      return ''
    return meta['transID']

  def __getMeta(self, tag, application):
    if not application:
      return {}

    meta = {}
    meta['softwareVersion'] = self.__param['softwareVersion']
    meta['process'] = self.__param['process']
    meta['position'] = self.__param['position']
    meta['tag'] = tag
    meta['application'] = application

    if tag in self.__transIDs and application in self.__transIDs[tag]:
      meta['transID'] = self.__transIDs[tag][application]
    else:
      transID = self.__getTransID(tag, application)
      if transID:
        meta['transID'] = transID

    return meta

  def __setMeta(self, tag, application):
    outputPath = self.__prodRoot
    outputPath = os.path.join(outputPath, tag)
    _setMetaData(outputPath, {'tag': tag})

    meta = {}
    meta['application'] = application
    if tag in self.__transIDs and application in self.__transIDs[tag]:
      meta['transID'] = self.__transIDs[tag][application]
    outputPath = os.path.join(outputPath, application)
    _setMetaData(outputPath, meta)

  def __getTagParam(self, tag):
    tagParam = {}
    tagParam['tag'] = tag
    frag = tag.split('_')
    tagParam['particle'] = frag[0]
    if len(frag) > 1:
      unit = frag[1][-3:].lower()
      try:
        num = float(frag[1][:-3])
      except ValueError:
        num = 0
      if unit == 'tev':
        num *= 1000*1000
      elif unit == 'gev':
        num *= 1000
      tagParam['momentum'] = str(num)
    else:
      tagParam['momentum'] = '0'
    return tagParam

  def createStep(self, application, tag, tagParam, transType, prevApp=None, isGen=False):
    transID = self.__getTransID(tag, application)
    if transID:
      gLogger.notice('Transformation already exists for "{0}" with ID {1}'.format(self.__getOutputPath(tag, application), transID))
      return

    step_mode = self.__param.get(application+'-mode', '').format(**tagParam)
    extraArgs = '{0} {1} "{2}"'.format(self.__param['evtmax'], self.__param['seed'], step_mode)
    stepArg = dict(
        executable = 'bootstrap.sh'.format(application),
        transType = transType,
        transGroup = self.__param.get('transGroup'),
        softwareVersion = self.__param['softwareVersion'],
        application = application,
        stepName = '{0}-{1}-{2}'.format(self.__prodPrefix, tag, application),
        description = '{0} for {1} with tag {2}'.format(application, self.__param['process'], tag),
        extraArgs = extraArgs,
        inputMeta = self.__getMeta(tag, prevApp),
        outputPath = self.__getOutputPath(tag, application),
        outputSE = self.__param['outputSE'],
        outputPattern = '{0}-*.root'.format(application),
        isGen = isGen,
        site = self.__param.get('site'),
        outputMode = self.__param['outputMode'],
        maxNumberOfTasks = self.__param['numberOfTasks'],
    )

    if self.__param['dryrun']:
      gLogger.notice('Create: {0}'.format(stepArg))
      return

    prodStep = ProdStep(**stepArg)
    prodStep.createJob()
    transID = prodStep.createTransformation()
    self.__transIDs[tag] = {}
    self.__transIDs[tag][application] = transID
    self.__setMeta(tag, application)

  def createAllTransformations(self):
    for tag in self.__param['tags']:
      tagParam = self.__getTagParam(tag)
      gLogger.notice('Tag param: {0}'.format(tagParam))

      for step in ['detsim', 'elecsim', 'calib', 'rec']:
        if step not in self.__param['workflow']:
          continue

        if step == 'detsim':
          self.createStep('detsim', tag, tagParam, 'MCSimulation-JUNO', None, True)
        if step == 'elecsim':
          self.createStep('elecsim', tag, tagParam, 'ElecSimulation-JUNO', 'detsim')
        if step == 'calib':
          self.createStep('calib', tag, tagParam, 'Calibration-JUNO', 'elecsim')
        if step == 'rec':
          self.createStep('rec', tag, tagParam, 'DataReconstruction-JUNO', 'calib')


def main():
  args = Script.getPositionalArgs()
  switches = Script.getUnprocessedSwitches()

  configFile = 'prod.ini'
  paramCmd = {}

  for k, v in switches:
    if k == 'ini':
      configFile = v
    if k == 'dryrun':
      paramCmd['dryrun'] = 'true'

  if args:
    paramCmd['process'] = args[0]

  par = Param(configFile, paramCmd)

  chain = ProdChain(par.param)
  chain.createAllTransformations()

  return 0

if __name__ == '__main__':
  try:
    exit(main())
  except Exception as e:
    gLogger.error('Exception: {0}'.format(e))
    if DEBUG:
      raise
    exit(1)
