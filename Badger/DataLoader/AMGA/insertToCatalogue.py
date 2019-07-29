#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei

from amga import mdclient,mdinterface
import time
import re

#if eventType is userN in filename, eventType in catalogue of amga is "user"
#else eventTypes are same in filename and in catalogue of amga 
def getEventType(eventType):
    #check whether userN exists in filename
    pat = re.compile(r'user\d+')
    des = pat.search(eventType)
    
    # userN exists in filename
    if des is not None:
        return "user"
    else:
        return eventType
    
#if string "mexp" exists in filename, that is to say, this file has several expNum
#this file will be stored under "mexp" catalogue in amga
def getExpNum(expNum):
    #check whether "mexp" exists in filename
    pat = re.compile(r'mexp')
    des = pat.search(expNum)

    if des is not None:
        return "mexp"
    else:
        return expNum
    
#when we create a new catalogue,15 attributes should be added to the last directory
#this last directory may be expN,mexp or streamId
def addAttributes(client,directory):
    try:
        client.addAttr(directory,"guid","varchar(40)")
        client.addAttr(directory,"LFN","varchar(100)")
        client.addAttr(directory,"dataType","varchar(30)")
        client.addAttr(directory,"eventType","varchar(30)")
        client.addAttr(directory,"bossVer","varchar(30)")
        client.addAttr(directory,"runL","int")
        client.addAttr(directory,"runH","int")
        client.addAttr(directory,"fileSize","int")
        client.addAttr(directory,"eventNum","int")
        client.addAttr(directory,"resonance","varchar(30)")
        client.addAttr(directory,"expNum","varchar(30)")
        client.addAttr(directory,"streamId","varchar(30)")
        client.addAttr(directory,"status","int")
        client.addAttr(directory,"date","timestamp")
        client.addAttr(directory,"description","varchar(100)")
    except mdinterface.CommandException,ex:
        print "Error:",ex
        
#check whether dir+eventType,dir+eventType+expNum and dir+eventType+expNum+streamId exist in amga
#if not,create them
def createCatalog(client,dir,eventType,expNum,streamId):
    
    dir_evtType = dir+"/"+eventType
    dir_expNum = dir+"/"+eventType+"/"+expNum
    dir_streamId = dir+"/"+eventType+"/"+expNum+"/"+streamId


    dir_entries = []
    dir_evtType_entries = []
    dir_expNum_entries = []

    evtType_exists = 0
    expNum_exists = 0
    streamId_exists = 0

    #check whether dir+eventType exists in amga
    #list all eventTypes under "dir" catalog
    client.listEntries(dir)
    evtType_entry = client.getEntry()[0]
    
    while evtType_entry:
        dir_entries.append(evtType_entry)
        evtType_entry = client.getEntry()[0]
        
    for entry in dir_entries:
        #dir+eventType exists in amga
        if entry == dir_evtType :
            evtType_exists = 1
            break
    #if dir+eventType doesn't exist in amga,then create it and dir+"/"+eventType+"/"+expNum
    if  evtType_exists== 0:
        try:
            client.createDir(dir_evtType)
            client.createDir(dir_expNum)
            if streamId == "stream0":
                addAttributes(client,dir_expNum)
        except mdinterface.CommandException,ex:
            print "Error",ex

    else:
        #if dir+eventType  exists in amga,then check for expNum subdir 
        client.listEntries(dir_evtType)
        

        expNum_entry = client.getEntry()[0]
        while expNum_entry:
            dir_evtType_entries.append(expNum_entry)
            expNum_entry = client.getEntry()[0]

        for entry in dir_evtType_entries:
            if entry == dir_expNum:
                expNum_exists = 1
                break
        
        if expNum_exists == 0:
            try:
                client.createDir(dir_expNum)
                if streamId == "stream0":
                    addAttributes(client,dir_expNum)
            except mdinterface.CommandException,ex:
                print "Error",ex

    if streamId != "stream0":
        
        client.listEntries(dir_expNum)
        streamId_entry = client.getEntry()[0]

        while streamId_entry:
            dir_expNum_entries.append(streamId_entry)
            streamId_entry = client.getEntry()[0]
                
        for entry in dir_expNum_entries:
            if entry ==dir_streamId:
                streamId_exists=1
                break
            
        if streamId_exists==0:
            try:
                client.createDir(dir_streamId)
                addAttributes(client,dir_streamId)
            except mdinterface.CommandException,ex:
                print "Error",ex
                    
        
    if streamId=="stream0":
        return dir_expNum
    else:
        return dir_streamId

    
def insert(attributes):
    values = []
    keys = []
                  
    dir1 = "/BES3_test/File/"+attributes["resonance"]+"/"+attributes["bossVer"]
    
    #get real eventType and expNum in catalogue in amga
    eventType = getEventType(attributes["eventType"])
    expNum = getExpNum(attributes["expNum"])

    if attributes["streamId"]=="stream0":
        dir=dir1+"/data"
    else:
        dir=dir1+"/mc"

    client=mdclient.MDClient('badger01.ihep.ac.cn',8822,'amga','Amg@Us3r')   
    #client=mdclient.MDClient('besdev01.ihep.ac.cn',8822,'root')   
  
    #get insertion directory in amga
    insertDir = createCatalog(client,dir,eventType,expNum,attributes["streamId"])
    
    #LFN is directory + filename in amga
    entry=insertDir+"/"+attributes["LFN"]
    attributes["LFN"] = entry
    
    for key in attributes.keys():
        keys.append(key)
        values.append(attributes[key])

    try:
        #insert dst file to amga
        client.addEntry(entry,keys,values)
    except mdinterface.CommandException,ex:
        print "Error",ex

