#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei

###############################################################################
# Function:                                                                   #
#         Read content of ExpSearch.txt                                       #
#         Insert information of each line ExpSearch.txt to /BES3/ExpSearch    #
#         If you want to add new entris to /BES3/ExpSearch, please add new    #
#         lines to a txt file according this order:                           #
#         runFrm,runTo,dateFrm,dateTo,expNum,resonance,roundId                #  
#                                                                             #
###############################################################################


from amga import mdclient,mdinterface
import string
import uuid

def insertToExp(items):
    #items is a list,and contains values of 
    #'runFrm','runTo','dateFrm','dateTo','expNum','resonance','roundId'
    
    client=mdclient.MDClient('badger01.ihep.ac.cn',8822,'amga','Amg@Us3r')
    
    keys=['Id','runFrm','runTo','dateFrm','dateTo','expNum','resonance','roundId']
    
    #In items, the types of runFrm and runTo are str
    runfrm=string.atoi(items[0])
    runto=string.atoi(items[1])
    expnum = string.lower(items[4])
    resonance = string.lower(items[5])
    roundid = string.lower(items[6])
    
    id = uuid.uuid1()
    values=[id,runfrm,runto,items[2],items[3],expnum,resonance,roundid]

    #set entry name is resonance+"_"+expnum
    entryName='/BES3/ExpSearch/'+resonance+"_"+expnum
    try:
        client.addEntry(entryName,keys,values)
    except mdinterface.CommandException,ex:
        print "Error",ex
        
    
    
if __name__=='__main__':
    filename = raw_input('Enter file name:')
    f = open(filename,'r')
    allLines = f.readlines()
    f.close()

    for eachLine in allLines:
        data=eachLine.strip()
        items=data.split(',')
                
        insertToExp(items)
        
        #print items
