#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei

from amga import mdclient,mdinterface
from insertToCatalogue import addAttributes

client=mdclient.MDClient('badger01.ihep.ac.cn',8822,'amga','Amg@Us3r')
dir='/BES3/File/psipp/6.6.1/data/all/exp1/'
#client.listEntries(dir)
#entry = client.getEntry()[0]
#print "entry under /BES3/linlei",entry

#addAttributes(client,dir)
keys=['status','LFN','eventType','streamId','eventNum','fileSize','runH','runL','resonance','date','expNum','guid','bossVer','dataType','description']
values=['-1','/BES3/File/psipp/6.6.1/data/all/exp1/run_0011414_All_file001_SFO-2','all','stream0',139375,415222312,11414,11414,'psipp','2011-12-13 09:47:33','exp1','6d32e104-252c-11e1-8352-00266cf9c72c','6.6.1','dst','null']

entry='/BES3/File/psipp/6.6.1/data/all/exp1/ll1'
client.addEntry(entry,keys,values)

#client.listEntries('BES3/linlei')
#entry = client.getEntry()[0]
#print "after:entry under /BES3/linlei",entry
