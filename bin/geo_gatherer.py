#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015 Panu Lahtinen

# Author(s): Panu Lahtinen

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

"""Gather GEO stationary segments and send them in a bunch as a dataset.
"""

from ConfigParser import RawConfigParser, NoOptionError
import time
import logging
import logging.handlers
import os.path
import Queue
import datetime as dt
from collections import OrderedDict

from posttroll import message, publisher
from trollduction.listener import ListenerContainer
from trollsift import Parser, compose


class GeoGatherer(object):

    """Gatherer for geostationary satellite segments"""

    def __init__(self, config, section):
        self._config = config
        self._section = section
        topics = config.get(section, 'topics').split()
        self._listener = ListenerContainer(topics=topics)
        self._publisher = publisher.NoisyPublisher("geo_gatherer")
        self._subject = config.get(section, "publish_topic")
        self._pattern = config.get(section, 'pattern')
        self._parser = Parser(self._pattern)

        try:
            self._timeliness = dt.timedelta(seconds=config.getint(section,
                                                                  "timeliness"))
        except (NoOptionError, ValueError):
            self._timeliness = dt.timedelta(seconds=20)

        self._timeout = None
        self.metadata = {}
        self.received_files = set()
        self.wanted_files = set()
        self.all_files = set()
        self.critical_files = set()
        self.delayed_files = OrderedDict()

        self.logger = logging.getLogger("geo_gatherer")
        self._loop = False

    def _clear_data(self):
        """Clear data."""
        self._timeout = None
        self.metadata = {}
        self.received_files = set()
        self.wanted_files = set()
        self.all_files = set()
        self.critical_files = set()
        self.delayed_files = OrderedDict()

    def _init_data(self, msg):
        """Init wanted, all and critical files"""
        # Init metadata struct
        for key in msg.data:
            if key not in ("uid", "uri", "channel_name", "segment"):
                self.metadata[key] = msg.data[key]
        self.metadata['dataset'] = []

        # Critical files that are required, otherwise production will fail
        self.critical_files = \
            self._compose_filenames(self._config.get(self._section,
                                                     "critical_files"))
        # These files are wanted, but not critical for production
        self.wanted_files = \
            self._compose_filenames(self._config.get(self._section,
                                                     "wanted_files"))
        self.all_files = \
            self._compose_filenames(self._config.get(self._section,
                                                     "all_files"))

    def _compose_filenames(self, itm_str):
        """Compose filename set()s based on a pattern and item string.
        itm_str is formated like ':PRO,:EPI' or 'VIS006:8,VIS008:1-8,...'"""

        # Empty set
        result = set()

        # Get copy of metadata
        meta = self.metadata.copy()
        for itm in itm_str.split(','):
            channel_name, segments = itm.split(':')
            segments = segments.split('-')
            if len(segments) > 1:
                segments = ['%06d' % i for i in range(int(segments[0]),
                                                          int(segments[-1])+1)]
            meta['channel_name'] = channel_name
            for seg in segments:
                meta['segment'] = seg
                fname = self._parser.compose(meta)
                result.add(fname)

        return result

    def _publish(self):
        """Publish file dataset and reinitialize gatherer."""

        # Diagnostic logging about delayed ...
        if len(self.delayed_files) > 0:
            file_str = ''
            for key in self.delayed_files:
                file_str += "%s %f seconds, " % (key, self.delayed_files[key])
            self.logger.warning("Files received late: %s", file_str.strip(', '))
        # and missing files
        missing_files = self.all_files.difference(self.received_files)
        if len(missing_files) > 0:
            self.logger.warning("Missing files: %s", ', '.join(missing_files))

        msg = message.Message(self._subject, "dataset", self.metadata)
        self.logger.info("Sending: %s", str(msg))
        self._publisher.send(str(msg))

        self._clear_data()

    def set_logger(self, logger):
        """Set logger."""
        self.logger = logger

    def collection_ready(self):
        """Determine if collection is ready to be published."""
        # If no files have been collected, return False
        if len(self.received_files) == 0:
            return False
        # If all wanted files have been received, return True
        if self.wanted_files.union(self.critical_files).issubset(\
                self.received_files):
            return True
        # If all critical files have been received ...
        if self.critical_files.issubset(self.received_files):
            # and timeout is reached, return True
            if self._timeout is not None and \
               self._timeout <= dt.datetime.utcnow():
                return True
            # else, set timeout if not already running
            else:
                if self._timeout is None:
                    self._timeout = dt.datetime.utcnow() + self._timeliness
                    self.logger.info("Setting timeout to %s",
                                     str(self._timeout))
                return False

        # In other cases continue gathering
        return False

    def run(self):
        """Run GeoGatherer"""
        self._publisher.start()
        self._loop = True
        while self._loop:
            # Check if collection is ready for publication
            if self.collection_ready():
                self._publish()

            # Check listener for new messages
            msg = None
            try:
                msg = self._listener.queue.get(True, 1)
            except KeyboardInterrupt:
                self.stop()
                continue
            except Queue.Empty:
                continue

            if msg.type == "file":
                self.logger.info("New message received: %s", str(msg))
                self.process(msg)

    def stop(self):
        """Stop gatherer."""
        self.logger.info("Stopping gatherer.")
        self._loop = False
        if self._listener is not None:
            self._listener.stop()
        if self._publisher is not None:
            self._publisher.stop()

    def process(self, msg):
        """Process message"""
        mda = self._parser.parse(msg.data["uid"])
        if msg.data['uid'] in self.received_files:
            return
        # Init metadata etc if this is the first file
        if len(self.metadata) == 0:
            self._init_data(msg)
        # If the nominal time of the new segment is later than the
        # current metadata has, ...
        elif mda["nominal_time"] > self.metadata["nominal_time"]:
            # timeout ...
            self._timeout = dt.datetime.utcnow()
            # and check if the collection is ready and publish
            if self.collection_ready():
                self._publish()
                self._clear_data()
                self._init_data(msg)
            # or discard data and start new collection
            else:
                self.logger.warning("Collection not finished before new "
                                    "started")
                missing_files = self.all_files.difference(self.received_files)
                self.logger.warning("Missing files: %s", missing_files)
                self._clear_data()
                self._init_data(msg)

        # Add uid and uri
        self.metadata['dataset'].append({'uri': msg.data['uri'],
                                         'uid': msg.data['uid']})

        # If critical files have been received but the collection is
        # not complete, add the file to list of delayed files
        if self.critical_files.issubset(self.received_files):
            delay = dt.datetime.utcnow() - (self._timeout - self._timeliness)
            self.delayed_files[msg.data['uid']] = delay.total_seconds()

        # Add to received files
        self.received_files.add(msg.data['uid'])



def arg_parse():
    '''Handle input arguments.
    '''
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log",
                        help="File to log to (defaults to stdout)",
                        default=None)
    parser.add_argument("-v", "--verbose", help="print debug messages too",
                        action="store_true")
    parser.add_argument("-c", "--config", help="config file to be used")
    parser.add_argument("-C", "--config_item", help="config item to use")

    return parser.parse_args()


def main():
    '''Main. Parse cmdline, read config etc.'''

    args = arg_parse()

    config = RawConfigParser()
    config.read(args.config)

    print "Setting timezone to UTC"
    os.environ["TZ"] = "UTC"
    time.tzset()

    if args.log:
        handler = logging.handlers.TimedRotatingFileHandler(args.log,
                                                            "midnight",
                                                            backupCount=7)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s: %(asctime)s :"
                                           " %(name)s] %(message)s",
                                           '%Y-%m-%d %H:%M:%S'))
    if args.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    handler.setLevel(loglevel)
    logging.getLogger('').setLevel(loglevel)
    logging.getLogger('').addHandler(handler)
    logging.getLogger("posttroll").setLevel(logging.INFO)
    logger = logging.getLogger("geo_gatherer")


    gatherer = GeoGatherer(config, args.config_item)
    gatherer.set_logger(logger)
    gatherer.run()


if __name__ == "__main__":
    main()
