#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author: linlei

import re
import os.path


def judgeType(dstfile):
    if os.path.exists(dstfile):    
        #judge whether type of this dst file is "all"
        all = re.compile(r"run_\d+_All_file\d+_SFO-\d+.")
        #judge whether type of this dst file is "others"
        others = re.compile(r"[A-Za-z]+_[A-Za-z]+\d*_stream\d+_\d+_\d+.*")
       
        flag_all = all.search(dstfile)
        flag_others = others.search(dstfile)

        if flag_all is not None:
            #return flag_all.group()
            return "all"
        elif flag_others is not None:
            #return flag_others.group()
            return "others"
        else:
            print "can't check out this file's type"
            return  
    

    
