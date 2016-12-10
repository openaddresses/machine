# coding=utf8
"""
Run Python test suite via the standard unittest mechanism.
Usage:
  python test.py
  python test.py --logall
  python test.py TestConformTransforms
  python test.py -l TestOA.test_process
All logging is suppressed unless --logall or -l specified
~/.openaddr-logging-test.json can also be used to configure log behavior
"""
import unittest
import sys, os
import logging

if 'DATABASE_URL' not in os.environ:
    # Default to the testing DB if no DATABASE_URL env var is found.
    os.environ['DATABASE_URL'] = 'postgres://openaddr:openaddr@localhost/openaddr'

from openaddr import jobs

from openaddr.tests import TestOA, TestState, TestPackage
from openaddr.tests.sample import TestSample
from openaddr.tests.cache import TestCacheExtensionGuessing, TestCacheEsriDownload
from openaddr.tests.conform import TestConformCli, TestConformTransforms, TestConformMisc, TestConformCsv, TestConformLicense
from openaddr.tests.render import TestRender
from openaddr.tests.dotmap import TestDotmap
from openaddr.tests.preview import TestPreview
from openaddr.tests.util import TestUtilities
from openaddr.tests.summarize import TestSummarizeFunctions
from openaddr.tests.parcels import TestParcelsUtils, TestParcelsParse
from openaddr.tests.dashboard_stats import TestDashboardStats

from openaddr.tests.ci import (
    TestHook, TestRuns, TestWorker, TestBatch, TestObjects, TestCollect,
    TestAPI, TestQueue, TestAuth, TestTileIndex
    )

if __name__ == '__main__':
    # Allow the user to turn on logging with -l or --logall
    # unittest.main() has its own command line so we slide this in first
    level = logging.CRITICAL
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "-l" or arg == "--logall":
            level = logging.DEBUG
            del sys.argv[i]

    jobs.setup_logger(log_level = level, log_config_file = "~/.openaddr-logging-test.json")
    unittest.main()
