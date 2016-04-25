#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Martin Raspaud

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unittests for triggers
"""

import unittest
from mock import patch
from trollduction.collectors.trigger import PostTrollTrigger
from datetime import datetime, timedelta
import time

messages = ['']


class FakeMessage(object):

    def __init__(self, data):
        self.data = data


class TestPostTrollTrigger(unittest.TestCase):

    @patch('trollduction.collectors.trigger.NSSubscriber')
    @patch('trollduction.collectors.region_collector.RegionCollector')
    def test_timeout(self, rc, nssub):
        collector = rc()
        collector.timeout = datetime.utcnow() + timedelta(seconds=.2)
        collector.return_value = None

        def terminator(obj, publish_topic=None):
            collector.timeout = None
        ptt = PostTrollTrigger([collector], terminator, None, None,
                               publish_topic=None)

        sub = ptt.msgproc.nssub.start.return_value
        sub.recv.return_value = iter([FakeMessage({"a": "a"}),
                                      FakeMessage({"b": "b"}),
                                      FakeMessage({"c": "c"})])

        ptt.start()
        time.sleep(.4)
        ptt.stop()
        self.assertTrue(collector.timeout is None)


def suite():
    """The suite for test_trigger
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestPostTrollTrigger))

    return mysuite

if __name__ == '__main__':
    unittest.main()
