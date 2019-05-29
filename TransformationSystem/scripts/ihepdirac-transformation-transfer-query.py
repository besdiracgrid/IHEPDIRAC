#!/usr/bin/env python

import sys

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script

Script.setUsageMessage('''Start transfer according to DFC query with transformation system

%s [option|cfgfile] MetaTransfer SourceSE DestSE''' % Script.scriptName)
Script.registerSwitch( 't:', 'transformationName=', 'Specify transformation name, or use the query name')
Script.registerSwitch( 'g:', 'groupSize=', 'Group size for each task')
Script.parseCommandLine(ignoreErrors = False)

from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

args = Script.getPositionalArgs()

if len(args) != 3:
    Script.showHelp()
    exit(1)

metaTransfer = args[0]
fromSE = args[1]
toSE = args[2]

groupSize = 100
transformationName = metaTransfer

switches = Script.getUnprocessedSwitches()
for switch in switches:
    if switch[0] == 'g' or switch[0] == 'groupSize':
        groupSize = int(switch[1])
    if switch[0] == 't' or switch[0] == 'transformationName':
        transformationName = switch[1]

t = Transformation( )
tc = TransformationClient( )
t.setTransformationName(transformationName) # Must be unique
t.setTransformationGroup("Transfer")
t.setType("Transfer-JUNO")
#t.setPlugin("Standard") # Not needed. The default is 'Standard'
t.setDescription("Juno Data Transfer")
t.setLongDescription("Juno Data Transfer") # Mandatory
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

result = tc.createTransformationInputDataQuery(transID['Value'], {'juno_transfer': metaTransfer})
if not result['OK']:
    gLogger.error('Can not create query to transformation: %s' % result['Message'])
    exit(2)
