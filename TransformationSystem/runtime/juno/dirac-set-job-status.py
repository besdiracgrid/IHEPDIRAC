#!/usr/bin/env python

import os
import sys

from DIRAC.Core.Base import Script
Script.initialize(ignoreErrors=True)

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.Job import Job

from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport

jobID = os.environ.get('DIRACJOBID', '0')

if not jobID:
    print 'DIRAC job ID not found'
    sys.exit(1)


jobReport = JobReport(jobID, 'JUNO_JobScript')
result = jobReport.setApplicationStatus(', '.join(sys.argv[1:]))
if not result['OK']:
    print 'Set application status error: %s' % result
