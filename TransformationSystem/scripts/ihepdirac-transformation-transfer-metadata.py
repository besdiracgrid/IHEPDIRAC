#!/usr/bin/env python

import sys

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

Script.setUsageMessage('''Start transfer according to DFC query with transformation system

{0} [option|cfgfile] TransformationName MetaTransfer TargetSE

Example: {0} Meassurements_DAQ_CNAF juno_transfer=Pmt/container/Meassurements CNAF-STORM
Example: {0} Meassurements_DAQ_IN2P3 -p Standard juno_transfer=Pmt/container/Meassurements IN2P3-DCACHE
Example: {0} Meassurements_DAQ_JINR -t Transfer-JUNO juno_transfer=Pmt/container/Meassurements -f IHEP-STORM JINR-JUNO'''.format(Script.scriptName))
Script.registerSwitch( 't:', 'transformationType=', 'Specify transformation type')
Script.registerSwitch( 'g:', 'groupSize=', 'Group size for each task')
Script.registerSwitch( 'p:', 'plugin=', 'Plugin, default to "Broadcast"')
Script.registerSwitch( 'f:', 'sourceSE=', 'SourceSE')
Script.parseCommandLine(ignoreErrors = False)

from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery, FILE_STANDARD_METAKEYS

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

args = Script.getPositionalArgs()

if len(args) != 3:
    Script.showHelp()
    exit(1)

transformationName = args[0]
metaTransfer = args[1]
targetSE = args[2]

sourceSE = ''
groupSize = 100
transformationType = 'Transfer-JUNO'
plugin = 'Broadcast'

switches = Script.getUnprocessedSwitches()
for switch in switches:
    if switch[0] == 'g' or switch[0] == 'groupSize':
        groupSize = int(switch[1])
    if switch[0] == 't' or switch[0] == 'transformationType':
        transformationType = switch[1]
    if switch[0] == 'p' or switch[0] == 'plugin':
        plugin = switch[1]
    if switch[0] == 'f' or switch[0] == 'sourceSE':
        sourceSE = switch[1]


from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

fcc = FileCatalogClient('DataManagement/FileCatalog')

result = fcc.getMetadataFields()
if not result['OK']:
  gLogger.error("Error: %s" % result['Message'])
  exit(0)
if not result['Value']:
  gLogger.error("Error: no metadata fields defined")
  exit(0)
typeDict = result['Value']['FileMetaFields']
typeDict.update(result['Value']['DirectoryMetaFields'])
# Special meta tags
typeDict.update(FILE_STANDARD_METAKEYS)
mq = MetaQuery(typeDict=typeDict)
mq.setMetaQuery([metaTransfer])
query = mq.getMetaQuery()
gLogger.notice('Query: {0}'.format(query))

t = Transformation( )
tc = TransformationClient( )
t.setTransformationName(transformationName) # Must be unique
t.setTransformationGroup("Transfer")
t.setType(transformationType)
t.setPlugin(plugin)
t.setDescription("Data Transfer")
t.setLongDescription("Data Transfer") # Mandatory
t.setGroupSize(groupSize) # Here you specify how many files should be grouped within he same request, e.g. 100

transBody = ''
t.setBody(transBody)

if sourceSE:
    res = t.setSourceSE(sourceSE)
    if not res['OK']:
        gLogger.error("SourceSE not valid: %s" % res['Message'])
        exit(1)

res = t.setTargetSE(targetSE)
if not res['OK']:
    gLogger.error("TargetSE not valid: %s" % res['Message'])
    exit(1)

result = t.addTransformation() # Transformation is created here
if not result['OK']:
    gLogger.error('Can not add transformation: %s' % result['Message'])
    exit(2)

t.setStatus("Active")
t.setAgentType("Automatic")
transID = t.getTransformationID()

result = tc.createTransformationInputDataQuery(transID['Value'], query)
if not result['OK']:
    gLogger.error('Can not create query to transformation: %s' % result['Message'])
    exit(2)
