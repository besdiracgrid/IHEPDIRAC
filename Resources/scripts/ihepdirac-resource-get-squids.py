#!/usr/bin/env python

import os
import sys

from DIRAC import S_OK, S_ERROR, gConfig, gLogger, exit
from DIRAC.Core.Base import Script
import DIRAC
usageMsg = '''Get squids for a site.
{0} [option|cfgfile] site'''.format(Script.scriptName)
Script.setUsageMessage(usageMsg)

Script.parseCommandLine(ignoreErrors=False)

args = Script.getPositionalArgs()
switches = Script.getUnprocessedSwitches()

if len(args) == 0:
    site = DIRAC.siteName() 
else: 
    site = args[0]

squidurl = ""

for squid in gConfig.getValue( 'Resources/Squids/%s' % ( site ), [] ):
  squidurl = "http://" + squid + ":3128 "
  print "%s" % squidurl 
