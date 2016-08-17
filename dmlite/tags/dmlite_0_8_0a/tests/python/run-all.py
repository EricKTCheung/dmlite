#!/usr/bin/python

import unittest

from catalog import TestDmliteCatalog

suite = unittest.TestLoader().loadTestsFromTestCase(TestDmliteCatalog)
unittest.TextTestRunner(verbosity=2).run(suite)


