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

dt = DataTransfer()
res = dt.registerToServer()
print res

# Add From a dict
d = {}
d["User"] = "lintao"
d["Source"] = "IHEPD-USER"
d["Destination"] = "JINR-USER"
d["Protocol"] = "DIRACDMS"
d["FinalStatus"] = "OK"

d["TransferSize"] = 800 * 2**20 # 800 MB
d["TransferTime"] = 10 * 60 # 10min
d["TransferOK"] = 1
d["TransferTotal"] = 1

res = dt.setValuesFromDict(d)
print res
dt.setNowAsStartAndEndTime()
res = dt.commit()
print res
