# -*- coding: utf-8 -*-

import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Core.DISET.RPCClient import RPCClient

transferRequest = RPCClient("Transfer/TransferRequest")

print transferRequest.setEndPoint("endpoint1", "endpoint1 url")
print transferRequest.setEndPoint("endpoint2", "endpoint2 url")
print transferRequest.getEndPoint()

