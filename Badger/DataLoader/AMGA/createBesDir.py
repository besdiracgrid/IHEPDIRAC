#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei

###############################################################################
# The function of this programme                                              #
# Create directories as follows:                                              #
# /BES3/File/Resonance/BossVersion/Data                                       #
# /BES3/File/Resonance/BossVersion/MC                                         #
# /BES3/SearchExp                                                             #
# /BES3/EventTypeList                                                         #
# If you want to delete these catalogues, execute deleteBesDir.py please      #
###############################################################################

from amga import mdclient,mdinterface

        
def createFileDir(client,resonance,bossVer,type):
    try:
        #create BES3/File directory
        client.createDir("/BES3/File")
    except mdinterface.CommandException,ex:
        print "Error",ex


    for r in resonance:

        try:
            client.createDir("/BES3/File/"+r)
        except mdinterface.CommandException,ex:
            print "Error",ex
            
        for b in bossVer:
            try:
                client.createDir("/BES3/File/"+r+"/"+b)
            except mdinterface.CommandException,ex:
                print "Error",ex
                
            for t in type:
                try:
                    client.createDir("/BES3/File/"+r+"/"+b+"/"+t)
                except mdinterface.CommandException,ex:
                    print "Error",ex

def createExpSearch(client):
    try:
        client.createDir("/BES3/ExpSearch")
        client.addAttr("/BES3/ExpSearch","id","int")
        client.addAttr("/BES3/ExpSearch","runFrm","int")
        client.addAttr("/BES3/ExpSearch","runTo","int")
        client.addAttr("/BES3/ExpSearch","dateFrm","timestamp")
        client.addAttr("/BES3/ExpSearch","dateTo","timestamp")
        client.addAttr("/BES3/ExpSearch","expNum","varchar(20)")
        client.addAttr("/BES3/ExpSearch","resonance","varchar(20)")
        client.addAttr("/BES3/ExpSearch","roundId","varchar(20)")
    except mdinterface.CommandException,ex:
        print "Error",ex
                    

def createEventTypeList(client):
    try:
        client.createDir("/BES3/EventTypeList")
    except mdinterface.CommandException,ex:
        print "Error",ex


if __name__=="__main__":
   client=mdclient.MDClient('badger01.ihep.ac.cn',8822,'amga','Amg@Us3r')
   resonance=["jpsi","psip","psipp","psi4040","con3650","psippscan"]
   bossVer=["6.5.5","6.6.1"]
   type=["data","mc"]
   
   try:
        client.createDir("/BES3")
   except mdinterface.CommandException,ex:
       print "Error",ex
       
   createFileDir(client,resonance,bossVer,type)
   createExpSearch(client)
   createEventTypeList(client)
