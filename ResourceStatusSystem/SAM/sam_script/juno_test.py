#!/usr/bin/env python

import socket, subprocess, time

hostname = socket.gethostname()

commands = """
ls /cvmfs/juno.ihep.ac.cn
more /etc/redhat-release
export CMTCONFIG=amd64_linux26
source /cvmfs/juno.ihep.ac.cn/centos7_amd64_gcc830/Pre-Release/J20v2r0-Pre0/setup.sh
echo $LD_LIBRARY_PATH
ls /usr/include/
ls /cvmfs/juno.ihep.ac.cn/centos7_amd64_gcc830/contrib/compat/
source /cvmfs/juno.ihep.ac.cn/centos7_amd64_gcc830/contrib/compat/bashrc
python /cvmfs/juno.ihep.ac.cn/centos7_amd64_gcc830/Pre-Release/J20v2r0-Pre0/offline/Examples/Tutorial/share/tut_detsim.py gun
"""

start = time.time()
subp = subprocess.Popen( ['bash', '-c', commands], stdout = subprocess.PIPE, stderr = subprocess.PIPE )
stdout, stderr = subp.communicate()
runningTime = time.time() - start

print 'Host Name :', hostname
print 'Running Time :', runningTime
print '\n'
if stdout:
    print '==============================Standard Output==============================\n'
    print stdout
    print '\n'
if stderr:
    print '==============================Standard Error===============================\n'
    print stderr
