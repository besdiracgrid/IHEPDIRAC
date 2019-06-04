#!/usr/bin/env python

import sys

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

Script.setUsageMessage('''Start transfer according to DFC query with transformation system

{0} [option|cfgfile] TransformationName MetaTransfer SourceSE DestSE

Example: {0} Meassurements_DAQ_JINR -t Transfer-JUNO juno_transfer=PmtCharacterization/container_data/Meassurements_DAQ IHEP-STORM JINR-JUNO'''.format(Script.scriptName))
Script.registerSwitch( 't:', 'transformationType=', 'Specify transformation type')
Script.registerSwitch( 'g:', 'groupSize=', 'Group size for each task')
Script.parseCommandLine(ignoreErrors = False)

from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery, FILE_STANDARD_METAKEYS

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

args = Script.getPositionalArgs()

if len(args) != 4:
    Script.showHelp()
    exit(1)

transformationName = args[0]
metaTransfer = args[1]
fromSE = args[2]
toSE = args[3]

groupSize = 100
transformationType = 'Transfer-JUNO'

switches = Script.getUnprocessedSwitches()
for switch in switches:
    if switch[0] == 'g' or switch[0] == 'groupSize':
        groupSize = int(switch[1])
    if switch[0] == 't' or switch[0] == 'transformationType':
        transformationType = switch[1]


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
#t.setPlugin("Standard") # Not needed. The default is 'Standard'
t.setDescription("Data Transfer")
t.setLongDescription("Data Transfer") # Mandatory
t.setGroupSize(groupSize) # Here you specify how many files should be grouped within he same request, e.g. 100

transBody = [ ( "ReplicateAndRegister", { "SourceSE": fromSE, "TargetSE": toSE }) ]

t.setBody(transBody)

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
