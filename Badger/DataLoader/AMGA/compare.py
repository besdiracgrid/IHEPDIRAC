#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei
from amga import mdclient,mdinterface
from insertToCatalogue import getEventType,getExpNum

def compare(attributes,input):
    list = {}
    inamga = {}
    n = 0
    
    #print "input keys:",input.keys()
    keys = sorted(input.keys())
    #print "keys after being sorted:",keys
    
    expNum = getExpNum(attributes["expNum"])
    eventType = getEventType(attributes["eventType"])
    dir = "/BES3/File/"+attributes["resonance"]+"/"+attributes["bossVer"]

    if attributes["streamId"]=="stream0":
        dir += "/data"+"/"+eventType+"/"+expNum
    else:
        dir += "/mc"+"/"+eventType+"/"+expNum+"/"+attributes["streamId"]

    entry=dir+"/"+attributes["LFN"]

    client=mdclient.MDClient('badger01.ihep.ac.cn',8822,'amga','Amg@Us3r')

    client.getattr(entry,['resonance','streamId','eventType','bossVer','expNum'])
    result = client.getEntry()[1]
    inamga["resonance"] = result[0]
    inamga["streamId"] = result[1]
    inamga["eventType"] = result[2]
    inamga["bossVer"] = result[3]
    inamga["expNum"] = result[4]
        
    for key in keys:
        if input[key] != inamga[key]:
            list[key] = inamga[key]

        n += 1

    if list is not None:
        return list
