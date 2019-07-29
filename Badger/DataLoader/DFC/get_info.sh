#!/bin/bash
source ~/.660.sh

python << EOF
import ROOT
from ROOT import gROOT
def getCommonInfo(rootfile):
    
    commoninfo = {}

    gROOT.ProcessLine('gSystem->Load("libRootEventData.so");')
    gROOT.ProcessLine('TFile file("%s");'%rootfile)
    gROOT.ProcessLine('TTree* tree =(TTree*)file.Get("JobInfoTree");')
    gROOT.ProcessLine('TTree* tree1 =(TTree*)file.Get("Event");')
    gROOT.ProcessLine('TBranch* branch =(TBranch*)tree->GetBranch("JobInfo");')
    gROOT.ProcessLine('TBranch* branch1 =(TBranch*)tree1->GetBranch("TEvtHeader");')
    gROOT.ProcessLine('TJobInfo* jobInfo = new TJobInfo();')
    gROOT.ProcessLine('TEvtHeader* evtHeader = new TEvtHeader();')
    gROOT.ProcessLine('branch->SetAddress(&jobInfo);')
    gROOT.ProcessLine('branch1->SetAddress(&evtHeader);')
    gROOT.ProcessLine('branch->GetEntry(0);')
    gROOT.ProcessLine('branch1->GetEntry(0);')
    gROOT.ProcessLine('Int_t num=tree1.GetEntries()')
    
    #get Boss Version
    commoninfo["bossVer"] = ROOT.jobInfo.getBossVer().replace('.','')
  
    #get RunId
    commoninfo["runId"] = abs(ROOT.evtHeader.getRunId())
    #get all entries
    commoninfo["eventNumber"] = ROOT.num
    #get TotEvtNo
    #commoninfo["TotEvtNo"] = list(i for i in ROOT.jobInfo.getTotEvtNo())
    #get JobOption
    commoninfo["jobOptions"] = list(i for i in ROOT.jobInfo.getJobOptions())

    #set DataType
    commoninfo["dataType"]='dst'
    
    return commoninfo

dstfile = "$1"
import os.path
if os.path.exists(dstfile):
  print getCommonInfo(dstfile)

EOF
