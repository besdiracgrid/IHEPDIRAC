# -*- coding: utf-8 -*-

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Add A Transfer Record in Accounting System
""")

Script.parseCommandLine( ignoreErrors = True )

# TODO
# Add an entry by hand
from IHEPDIRAC.AccountingSystem.Client.Types.DataTransfer import DataTransfer
from DIRAC.Core.DISET.RPCClient                     import RPCClient

rpcclient = RPCClient("Accounting/DataStore")
#print rpcclient.deleteType("DataTransfer")
