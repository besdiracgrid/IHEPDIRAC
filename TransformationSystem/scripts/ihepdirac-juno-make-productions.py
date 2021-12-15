#!/usr/bin/env python

# pylint: disable=invalid-name,missing-docstring

import os
import sys
import re
import ConfigParser

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

Script.setUsageMessage('''Create JUNO production

{0} [option|cfgfile] [process]

Example: {0} --example > prod.ini
Example: {0} --ini myprod.ini --dryrun
Example: {0} --ini myprod.ini  
Example: {0} --ini myprod.ini Chain'''.format(Script.scriptName))
Script.registerSwitch('i:', 'ini=', 'Ini file, default to "prod.ini"')
Script.registerSwitch(
    'r', 'dryrun', 'Only parse the configuration, do not submit transformation')
Script.registerSwitch('e', 'example', 'Display prod.ini example')
Script.parseCommandLine(ignoreErrors=False)


from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

from DIRAC.Interfaces.API.Job import Job
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Workflow.Parameter import Parameter

DEBUG = True

URL_ROOT = 'http://dirac-code.ihep.ac.cn/juno/ts'

PROD_EXAMPLE = '''\
; Parameters could be put in the [all] section, or the process specified section
; The parameters in the specified section will overwrite those in the [all] section

; Common parameters
[all]
; Optional: Define dryrun mode, this mode only test the configuration and do not really submit any jobs
; This can also be passed in the command argument
;dryrun = false 

; Define user parameter section [Chain], task-specific parameters can be defined the [Chain] section
process = Chain

; Define JUNO offline software version
softwareVersion = centos7_amd64_gcc830/Pre-Release/J21v2r0-Pre0 

; Define production name, the name is used for current production
prodName = JUNOProdTest

; Optional: The prodNameSuffix will be added to the prodName to be sure that each submission has a unique name
;prodNameSuffix = _new

; Define the transformation group name, which is used for identifying monitoring cells 
transGroup = JUNO_prod_test

; Optional: If you just want to test a single tag, not all of them
; With this line enabled, the "tags" parameter will be ignored
;tag = e+_0.0MeV

; Define root directory in Dirac File Catalogue
; If outputType is "production", the root directory will be /juno/production
; If outputType is "user" or something else, the root directory will be under your user directory /juno/user/x/xxx
outputType = user

; Define sub directory relative to the root directory
outputSubDir = positron/prd_2021

; Define the output Mode, "closest" means uploading the job output data to the closest SE to the site
; If no closest SE is found for this site, then upload to the SE defined by "outputSE"
outputMode = closest
;outputSE = IHEP-STORM

; If no site specified, all available sites will be chosen unless the sites was put in "bannedsite"
;site = GRID.INFN-CNAF.it GRID.IHEP.cn GRID.IN2P3.fr GRID.JINR-CONDOR.ru
;bannedsite =   

; How many files to move in a single request
;moveGroupSize = 10

; Define the destination SE where data arrive
;moveTargetSE = IHEP-STORM CNAF-STORM
moveTargetSE = IHEP-STORM

; If ignoreWorkflow is true, the workflow will not be created
;ignoreWorkflow = false 

; If ignoreMove is true, the dataflow will not be processed
;ignoreMove = false

; The parameters in this section will overwrite what's in [all]
[Chain]
; Define the initial seed, "seed" + "jobid" is used for the final seed for each job
seed = 42

; Define maximum event in a job and the number of job, the total job volume is "evtmax" * "njobs"
evtmax = 2
njobs = 10

; Define the number of files to be held in a directory
max2dir = 10000

; Define regular expression for parsing the tags. https://docs.python.org/3/howto/regex.html
; Use group numbers "{0}", "{1}" in "detsim-mode"
tagParser = (.*)_(.*)MeV

; Used to define multi-tasks in one submission, single task can be set to the particle name eg. muon 
tags = e+_0.0MeV e+_1.398MeV e+_4.460MeV

; The final directory will be /<outputType>/<outputSubDir>/<softwareVersion>/<workDir>/<position>/<tags>
; The default is the process name, eg Chain 
; workDir = Chain 

; Define a subdirectory to identify particle positions
; The default is null
; position =  

; Define steps in the work flow
; workflow = detsim elecsim calib rec elecsim_rec
workflow = detsim elecsim_rec

; Define steps in the data flow
; moveType = detsim elecsim calib rec elecsim_rec
moveType = detsim

; Define if user root file need to be generated, the default is 1 (needed)
userOutput = 0 

; Define detsim gentools mode for the detsim step, {0} and {1} are defined by tags
; the default: 
; detsim-mode = --no-gdml --evtmax $evtmax --seed $seed --output detsim-${seed}.root --user-output detsim_user-${seed}.root
detsim-mode = gun --particles {0} --momentums {1} --positions 0 0 0

; Define additional parameters for each step
; The default:
; elecsim-mode = --evtmax -1 --seed $num --input $inputfile --output elecsim-$num.root --rate 1.0
; elecsim_rec-mode = --evtmax -1 --seed $num --input $inputfile --output elecsim_rec-$num.root
; cal-mode = --evtmax -1 --input $inputfile --output calib-$num.root
; rec-mode = --evtmax $evtmax --input $inputfile --output rec-$num.root 
elecsim_rec-mode = --rate 0.001 --enableWP --enableWPDarkPulse --no-evtrec

[Muon_2021]
seed = 42
evtmax = 5
njobs = 2
; Just single task, the default is "Single" 
; tags = Muon 

workflow = detsim
moveType = detsim

; Define the script to be run before detsim with <step>-preScripts, [seed] will be replaced with the one generated in each job
; Note: can't include {}
detsim-preScripts = $JUNOTOP/offline/InstallArea/$CMTCONFIG/bin/Muon.exe -seed [seed] -o Muon.txt -n $evtmax -v Rock -mult 1 -music_dir  $JUNOTOP/data/Generator/Muon/data

detsim-mode = --pmtsd-v2 --pmtsd-merge-twindow 1.0 --pmt-hit-type 2 --anamgr-config-file load_Muon_config.py --detoption Acrylic hepevt --exe Muon --file Muon.txt 
'''

def _setMetaData(directory, meta):
    fcc = FileCatalogClient()
    res = fcc.createDirectory(directory)
    if not res['OK']:
        raise Exception('DFC createDirectory for "{0}" error: {1}'.format(
            directory, res['Message']))
    res = fcc.setMetadata(directory, meta)
    if not res['OK']:
        raise Exception('DFC setMetadata for "{0}" error: {1}'.format(
            directory, res['Message']))


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
        self.__param.update(self.__paramCmd)    # Update paramCmd again to overwrite ini

    def __processParam(self):
        def parseList(s):
            return s.strip().split()

        def parseBool(s):
            if s.lower() in ['true', 'yes']:
                return True
            return False

        for key in ['softwareVersion', 'process']:
            if key not in self.__param or not self.__param[key]:
                raise Exception('Param "{0}" must be specified'.format(key))

        self.__param.setdefault('position', '')
        self.__param.setdefault('prodName', 'JUNOProd')
        self.__param.setdefault('transGroup', 'JUNO-Prod')
        self.__param.setdefault('outputType', 'user')
        self.__param.setdefault('outputSE', 'IHEP-STORM')
        self.__param.setdefault('outputMode', 'closest')
        self.__param.setdefault('tagParser', '')
        self.__param.setdefault('seed', '0')
        self.__param.setdefault('workDir', self.__param['process'])
        self.__param.setdefault('moveFlavor', 'Replication')
        self.__param.setdefault('movePlugin', 'Broadcast')

        self.__param['numberOfTasks'] = int(self.__param.get('njobs', '1'))
        self.__param['evtmax'] = int(self.__param.get('evtmax', '1'))
        self.__param['max2dir'] = int(self.__param.get('max2dir', '10000'))
        self.__param['userOutput'] = int(self.__param.get('userOutput', '1'))
        self.__param['moveGroupSize'] = int(
            self.__param.get('moveGroupSize', '1'))

        self.__param['site'] = parseList(self.__param.get('site', ''))
        self.__param['bannedsite'] = parseList(self.__param.get('bannedsite', ''))
        self.__param['workflow'] = parseList(self.__param.get('workflow', ''))
        self.__param['moveType'] = parseList(self.__param.get('moveType', ''))
        self.__param['moveSourceSE'] = parseList(
            self.__param.get('moveSourceSE', 'IHEP-STORM'))
        self.__param['moveTargetSE'] = parseList(
            self.__param.get('moveTargetSE', 'IHEP-STORM'))

        self.__param['dryrun'] = parseBool(self.__param.get('dryrun', 'false'))
        self.__param['ignoreWorkflow'] = parseBool(
            self.__param.get('ignoreWorkflow', 'false'))
        self.__param['ignoreMove'] = parseBool(
            self.__param.get('ignoreMove', 'false'))

        if 'tag' in self.__param:
            self.__param['tags'] = [self.__param['tag']]
        else:
            self.__param['tags'] = parseList(self.__param.get('tags', 'Single'))

    @property
    def param(self):
        return self.__param


class ProdMove(object):
    def __init__(self, transType, transGroup, transName='unknown', flavour='Replication', description='Production move',
                 plugin='Broadcast', inputMeta={}, sourceSE=[], targetSE='IHEP-STORM', groupSize=1):
        self.__transType = transType
        self.__transGroup = transGroup
        self.__transName = transName
        self.__flavour = flavour
        self.__description = description
        self.__plugin = plugin
        self.__inputMeta = inputMeta
        self.__sourceSE = sourceSE
        self.__targetSE = targetSE
        self.__groupSize = groupSize

    def createTransformation(self):
        ########################################
        # Transformation definition
        ########################################
        t = Transformation()

        t.setTransformationName(self.__transName)
        t.setType(self.__transType)
        t.setDescription(self.__description)
        t.setLongDescription(self.__description)
        t.setGroupSize(self.__groupSize)
        if self.__transGroup:
            t.setTransformationGroup(self.__transGroup)
        t.setPlugin(self.__plugin)

        t.setTargetSE(self.__targetSE)

        transBody = []

        t.setBody(transBody)

        ########################################
        # Transformation submission
        ########################################
        res = t.addTransformation()
        if not res['OK']:
            raise Exception(
                'Add transformation error: {0}'.format(res['Message']))

        t.setStatus("Active")
        t.setAgentType("Automatic")

        currtrans = t.getTransformationID()['Value']

        if self.__inputMeta:
            client = TransformationClient()
            res = client.createTransformationMetaQuery(
                currtrans, self.__inputMeta, 'Input')
            if not res['OK']:
                raise Exception(
                    'Create transformation query error: {0}'.format(res['Message']))

        return str(currtrans)


class ProdStep(object):
    def __init__(self, executable, transType, transGroup, softwareVersion,
                 application, stepName='unknown', description='Production step',
                 inputMeta={}, extraArgs='', inputData=None,
                 outputPath='/juno/test/prod', outputSE='IHEP-STORM', outputPattern='*.root',
                 isGen=False, site=None, bannedsite=None, outputMode='closest', maxNumberOfTasks=1):
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
        self.__bannedsite = bannedsite
        self.__outputMode = outputMode
        self.__maxNumberOfTasks = maxNumberOfTasks

        self.__job = None

    def createJob(self):
        job = Job()
        job.setName(self.__stepName)
        job.setOutputSandbox(['*log'])

        job.setExecutable(
            '/usr/bin/wget', arguments='"{0}/{1}"'.format(URL_ROOT, self.__executable))
        job.setExecutable(
            '/bin/chmod', arguments='+x "{0}"'.format(self.__executable))

        arguments = '"{0}" "{1}" "{2}" "{3}" "{4}" "{5}" @{{JOB_ID}}'.format(
            self.__softwareVersion, self.__application, self.__outputPath,
            self.__outputPattern, self.__outputSE, self.__outputMode)
        if self.__extraArgs:
            arguments += ' ' + self.__extraArgs
        job.setExecutable(self.__executable, arguments=arguments)

        # failover for failed jobs
        #job.setExecutable('/bin/ls -l', modulesList=['Script', 'FailoverRequest'])

        if self.__inputData:
            job.setInputData(self.__inputData)

        if self.__site:
            job.setDestination(self.__site)

        if self.__bannedsite:
            job.setBannedSites(self.__bannedsite)

        job.setOutputSandbox(['app.out','app.err','Script3_CodeOutput.log'])

        self.__job = job

    def submitJob(self):
        dirac = Dirac()
        res = dirac.submitJob(self.__job)
        gLogger.notice('Job submitted: {0}'.format(res["Value"]))
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
        t.setBody(self.__job.workflow.toXML())

        ########################################
        # Transformation submission
        ########################################
        res = t.addTransformation()
        if not res['OK']:
            raise Exception(
                'Add transformation error: {0}'.format(res['Message']))

        t.setStatus("Active")
        t.setAgentType("Automatic")

        currtrans = t.getTransformationID()['Value']

        if self.__inputMeta and not self.__isGen:
            client = TransformationClient()
            res = client.createTransformationMetaQuery(
                currtrans, self.__inputMeta, 'Input')
            if not res['OK']:
                raise Exception(
                    'Create transformation query error: {0}'.format(res['Message']))

        return str(currtrans)


class ProdChain(object):
    def __init__(self, param):
        self.__param = param
        self.__transIDs = {}

        self.__prodPrefix = '{0}{1}-{2}-{3}'.format(param['prodName'], param.get(
            'prodNameSuffix', ''), param['softwareVersion'], param['workDir'])

        self.__tagRE = re.compile(param['tagParser'], re.IGNORECASE)

        self.__ownerAndGroup()

        outputSubDir = self.__param['outputSubDir'].strip('/')
        if self.__param['outputType'] == 'production':
            self.__outputRoot = os.path.join(self.__prodHome, outputSubDir)
        else:
            self.__outputRoot = os.path.join(self.__userHome, outputSubDir)

        self.__prepareDir()

        gLogger.notice('Owner: {0}'.format(self.__owner))
        gLogger.notice('OwnerGroup: {0}'.format(self.__ownerGroup))
        gLogger.notice('VO: {0}'.format(self.__vo))
        gLogger.notice('ProdRoot: {0}'.format(self.__prodRoot))

    def __ownerAndGroup(self):
        res = getProxyInfo(False, False)
        if not res['OK']:
            raise Exception('GetProxyInfo error: {0}'.format(res['Message']))
        self.__owner = res['Value']['username']
        self.__ownerGroup = res['Value']['group']
        self.__vo = Registry.getVOMSVOForGroup(self.__ownerGroup)
        self.__voHome = '/{0}'.format(self.__vo)
        self.__prodHome = '/{0}/production'.format(self.__vo)
        self.__userHome = '/{0}/user/{1:.1}/{1}'.format(
            self.__vo, self.__owner)

    def __prepareDir(self):
        outputPath = self.__outputRoot
        for d in ['softwareVersion', 'workDir', 'position']:
            if d == 'workDir':
                key = 'process'
            else:
                key = d
            if self.__param[d]:
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

    def __parseTagParam(self, tag):
        if not self.__param['tagParser']:
            return [], {}

        m = self.__tagRE.match(tag)
        if not m:
            return [], {}

        return m.groups(), m.groupdict()

    def createStep(self, application, tag, tagParam, transType, prevApp=None):
        transID = self.__getTransID(tag, application)
        if transID:
            gLogger.error('{0}: Transformation already exists for with ID {1} on {2}'.format(
                application, transID, self.__getOutputPath(tag, application)))
            return

        inputMeta = {}
        if prevApp:
            inputMeta = self.__getMeta(tag, prevApp)
            #add userdata to distinguish detsim.root and detsim_user.root
            inputMeta['userdata'] = 0
            inputMeta['userdata_s'] = "0" 
            if 'transID' not in inputMeta:
                gLogger.error('{0}: Transformation not found for previous application "{1}"'.format(
                    application, prevApp))
                return
            gLogger.notice('{0}: Input transformation "{1}" from "{2}" with meta userdata={3}'.format(
                application, inputMeta['transID'], prevApp, inputMeta['userdata']))
        
        step_mode = self.__param.get(
            application + '-mode', '').format(*tagParam[0], **tagParam[1])
        step_preScripts = self.__param.get(
            application + '-preScripts', '').format(*tagParam[0], **tagParam[1])
        if step_mode:
            gLogger.notice('{0}-mode: {1}'.format(application, step_mode))
        if step_preScripts:
            gLogger.notice('{0}-preScripts: {1}'.format(application, step_preScripts))

        extraArgs = '{0} {1} "{2}" {3} {4} "{5}"'.format(
            self.__param['evtmax'], self.__param['seed'], step_mode, self.__param['max2dir'], self.__param['userOutput'], step_preScripts)
        stepArg = dict(
            executable='bootstrap.sh',
            transType=transType,
            transGroup=self.__param.get('transGroup'),
            softwareVersion=self.__param['softwareVersion'],
            application=application,
            stepName='{0}-{1}-{2}'.format(self.__prodPrefix, tag, application),
            description='{0} for {1} with tag {2}'.format(
                application, self.__param['process'], tag),
            extraArgs=extraArgs,
            inputMeta=inputMeta,
            outputPath=self.__getOutputPath(tag, application),
            outputSE=self.__param['outputSE'],
            outputPattern='{0}*-*.root'.format(application),
            isGen=prevApp is None,
            site=self.__param.get('site'),
            bannedsite=self.__param.get('bannedsite'),
            outputMode=self.__param['outputMode'],
            maxNumberOfTasks=self.__param['numberOfTasks'],
        )

        gLogger.notice('{0}: Create transformation...'.format(application))

        if self.__param['dryrun']:
            transID = 'dryrun'
        else:
            prodStep = ProdStep(**stepArg)
            prodStep.createJob()
            transID = prodStep.createTransformation()

        self.__transIDs.setdefault(tag, {})
        self.__transIDs[tag][application] = transID

        if not self.__param['dryrun']:
            self.__setMeta(tag, application)

    def createMove(self, application, tag, transType):
        inputMeta = self.__getMeta(tag, application)
        if 'transID' not in inputMeta:
            gLogger.error(
                '{0}-move: Transformation not found for application "{0}"'.format(application))
            return
        gLogger.notice(
            '{0}-move: Input transformation "{1}" from "{0}"'.format(application, inputMeta['transID']))

        moveArg = dict(
            transType=transType,
            transGroup=self.__param.get('transGroup'),
            transName='{0}-{1}-{2}-{3}'.format(self.__prodPrefix,
                                               tag, application, self.__param.get('moveFlavor')),
            flavour=self.__param.get('moveFlavor'),
            description='Move {0} for {1} with tag {2}'.format(
                application, self.__param['process'], tag),
            plugin=self.__param['movePlugin'],
            inputMeta=inputMeta,
            sourceSE=self.__param['moveSourceSE'],
            targetSE=self.__param['moveTargetSE'],
            groupSize=self.__param['moveGroupSize'],
        )

        gLogger.notice(
            '{0}-move: Create transformation...'.format(application))

        if self.__param['dryrun']:
            transID = 'dryrun'
        else:
            prodMove = ProdMove(**moveArg)
            transID = prodMove.createTransformation()

    def createAllTransformations(self):
        for tag in self.__param['tags']:
            if not self.__param['ignoreWorkflow']:
                tagParam = self.__parseTagParam(tag)
                gLogger.notice(
                    '\nTag "{0}" with param: {1}'.format(tag, tagParam))

                for step in ['detsim', 'elecsim_rec', 'elecsim', 'calib', 'rec']:
                    if step not in self.__param['workflow']:
                        continue

                    gLogger.notice('')
                    if step == 'detsim':
                        self.createStep('detsim', tag, tagParam,
                                        'MCSimulation-JUNO', None)
                    if step == 'elecsim_rec':
                        self.createStep('elecsim_rec', tag, tagParam,
                                        'ElecSimulation-JUNO', 'detsim')
                    if step == 'elecsim':
                        self.createStep('elecsim', tag, tagParam,
                                        'ElecSimulation-JUNO', 'detsim')
                    if step == 'calib':
                        self.createStep('calib', tag, tagParam,
                                        'Calibration-JUNO', 'elecsim')
                    if step == 'rec':
                        self.createStep('rec', tag, tagParam,
                                        'DataReconstruction-JUNO', 'calib')

            if not self.__param['ignoreMove']:
                for step in ['detsim', 'elecsim_rec', 'elecsim', 'calib', 'rec']:
                    if step not in self.__param['moveType']:
                        continue
                    gLogger.notice('')
                    self.createMove(step, tag, 'Replication-JUNO')


def main():
    args = Script.getPositionalArgs()
    switches = Script.getUnprocessedSwitches()

    configFile = 'prod.ini'
    paramCmd = {}
    displayExample = False

    for k, v in switches:
        if k == 'i' or k == 'ini':
            configFile = v
        if k == 'r' or k == 'dryrun':
            paramCmd['dryrun'] = 'true'
        if k == 'e' or k == 'example':
            displayExample = True

    if displayExample:
        sys.stdout.write(PROD_EXAMPLE)
        return 0

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
        if DEBUG:
            raise
        gLogger.error('{0}'.format(e))
        exit(1)
