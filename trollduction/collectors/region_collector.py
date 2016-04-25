#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012, 2014, 2015 Martin Raspaud

# Author(s):

#   Kristian Rune Larsen <krl@dmi.dk>
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>

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

"""Region collector.
"""

import os
from datetime import timedelta, datetime

from trollsched.satpass import Pass

import logging

LOG = logging.getLogger(__name__)

PLOT = False

class RegionCollector(object):

    """This is the region collector. It collects granules that overlap on a
    region of interest and return the collection of granules when it's done.

    *timeliness* defines the max allowed age of the granule.

    """

    def __init__(self, region,
                 timeliness=timedelta(seconds=600),
                 granule_duration=None):
        self.region = region  # area def
        self.granule_times = set()
        self.granules = []
        self.planned_granule_times = set()
        self.timeliness = timeliness
        self.timeout = None
        self.granule_duration = granule_duration

    def __call__(self, granule_metadata):
        return self.collect(granule_metadata)

    def collect(self, granule_metadata):
        """ 
            Parameters:

                granule_metadata : metadata

        """

        # Check if input data is being waited for

        platform = granule_metadata['platform_name']

        start_time = granule_metadata['start_time']
        end_time = granule_metadata['end_time']

        LOG.debug("Adding area ID to metadata: %s", str(self.region.area_id))
        granule_metadata['collection_area_id'] = self.region.area_id

        for ptime in self.planned_granule_times:
            if abs(start_time - ptime) < timedelta(seconds=3) and \
               ptime not in self.granule_times:
                self.granule_times.add(ptime)
                self.granules.append(granule_metadata)
                LOG.info("Added %s (%s) granule to area %s",
                         platform,
                         str(start_time),
                         self.region.area_id)
                # If last granule return swath and cleanup
                # if self.granule_times == self.planned_granule_times:
                if self.is_swath_complete():
                    LOG.info("Collection finished for area: %s",
                             str(self.region.area_id))
                    return self.finish()
                else:
                    try:
                        new_timeout = (max(self.planned_granule_times -
                                           self.granule_times) +
                                       self.granule_duration +
                                       self.timeliness)
                    except ValueError:
                        LOG.error("Calculation of new timeout failed, "
                                  "keeping previous timeout.")
                        LOG.error("Planned: %s", self.planned_granule_times)
                        LOG.error("Received: %s", self.granule_times)
                        return

                    if new_timeout < self.timeout:
                        self.timeout = new_timeout
                        LOG.info("Adjusted timeout: %s",
                                 self.timeout.isoformat())

                    return

        # Get corners from input data

        if self.granule_duration is None:
            self.granule_duration = end_time - start_time
            LOG.debug("Estimated granule duration to %s",
                      str(self.granule_duration))

        granule_pass = Pass(platform, start_time, end_time,
                            instrument=granule_metadata["sensor"])

        # If file is within region, make pass prediction to know what to wait
        # for
        if granule_pass.area_coverage(self.region) > 0:
            self.granule_times.add(start_time)
            self.granules.append(granule_metadata)

            # Computation of the predicted granules within the region

            if not self.planned_granule_times:
                self.planned_granule_times.add(start_time)
                LOG.info("Added %s (%s) granule to area %s",
                         platform,
                         str(start_time),
                         self.region.area_id)
                LOG.debug("Predicting granules covering %s",
                          self.region.area_id)
                gr_time = start_time
                while True:
                    gr_time += self.granule_duration
                    gr_pass = Pass(platform, gr_time,
                                   gr_time + self.granule_duration,
                                   instrument=granule_metadata["sensor"])
                    if not gr_pass.area_coverage(self.region) > 0:
                        break
                    self.planned_granule_times.add(gr_time)

                gr_time = start_time
                while True:
                    gr_time -= self.granule_duration
                    gr_pass = Pass(platform, gr_time,
                                   gr_time + self.granule_duration,
                                   instrument=granule_metadata["sensor"])
                    if not gr_pass.area_coverage(self.region) > 0:
                        break
                    self.planned_granule_times.add(gr_time)

                LOG.info("Planned granules for %s: %s", self.region.name,
                         str(sorted(self.planned_granule_times)))
                self.timeout = (max(self.planned_granule_times) +
                                self.granule_duration +
                                self.timeliness)
                LOG.info("Planned timeout for %s: %s", self.region.name,
                         self.timeout.isoformat())

        else:
            try:
                LOG.debug("Granule %s is not overlapping %s",
                          granule_metadata["uri"], self.region.name)
            except KeyError:
                try:
                    LOG.debug("Granule with start and end times = %s  %s  "
                              "is not overlapping %s",
                              str(granule_metadata["start_time"]),
                              str(granule_metadata["end_time"]),
                              str(self.region.name))
                except KeyError:
                    LOG.debug("Failed printing debug info...")
                    LOG.debug("Keys in granule_metadata = %s",
                              str(granule_metadata.keys()))

        # If last granule return swath and cleanup
        if self.is_swath_complete():
            LOG.debug("Collection finished for area: %s",
                      str(self.region.area_id))
            return self.finish()

    def is_swath_complete(self):
        '''Check if the swath is complete'''
        if self.granule_times:
            if self.planned_granule_times.issubset(self.granule_times):
                return True
            try:
                new_timeout = (max(self.planned_granule_times -
                                   self.granule_times) +
                               self.granule_duration +
                               self.timeliness)
            except ValueError:
                LOG.error("Calculation of new timeout failed, "
                          "keeping previous timeout.")
                LOG.error("Planned: %s", self.planned_granule_times)
                LOG.error("Received: %s", self.granule_times)
                return False
            if new_timeout < self.timeout:
                self.timeout = new_timeout
                LOG.info("Adjusted timeout: %s", self.timeout.isoformat())

        return False


    def cleanup(self):
        '''Clear members.
        '''
        self.granule_times = set()
        self.granules = []
        self.planned_granule_times = set()
        self.timeout = None

    def finish(self):
        '''Finish collection, add area ID to metadata, cleanup and return
        granule metadata.
        '''
        granules = self.granules
        self.cleanup()
        return granules


def read_granule_metadata(filename):
    """Read granule metadata.
    """
    import json
    with open(filename) as jfp:
        metadata = json.load(jfp)[0]

    metadata['uri'] = "file://" + os.path.abspath(filename)

    for attr in ["start_time", "end_time"]:
        try:
            metadata[attr] = datetime.strptime(
                metadata[attr], "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            metadata[attr] = datetime.strptime(
                metadata[attr], "%Y-%m-%dT%H:%M:%S")
    return metadata
