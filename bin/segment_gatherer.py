#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015, 2016 Panu Lahtinen

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

"""Gather GEO stationary segments, or polar satellite granules for one
timestep, and send them in a bunch as a dataset.
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


class SegmentGatherer(object):

    """Gatherer for geostationary satellite segments and multifile polar
    satellite granules."""

    def __init__(self, config, section):
        self._config = config
        self._section = section
        topics = config.get(section, 'topics').split()
        self._listener = ListenerContainer(topics=topics)
        self._publisher = publisher.NoisyPublisher("segment_gatherer")
        self._subject = config.get(section, "publish_topic")
        self._pattern = config.get(section, 'pattern')
        self._parser = Parser(self._pattern)

        try:
            self._timeliness = dt.timedelta(seconds=config.getint(section,
                                                                  "timeliness"))
        except (NoOptionError, ValueError):
            self._timeliness = dt.timedelta(seconds=1200)

        self.slots = OrderedDict()

        self.time_name = config.get(section, 'time_name')

        self.logger = logging.getLogger("segment_gatherer")
        self._loop = False

    def _clear_data(self, time_slot):
        """Clear data."""
        if time_slot in self.slots:
            del self.slots[time_slot]

    def _init_data(self, msg, mda):
        """Init wanted, all and critical files"""
        # Init metadata struct
        metadata = {}
        for key in msg.data:
            if key not in ("uid", "uri", "channel_name", "segment"):
                metadata[key] = msg.data[key]
        metadata['dataset'] = []

        # Use also metadata parsed from the filenames
        metadata.update(mda)

        time_slot = str(metadata[self.time_name])
        self.slots[time_slot] = {}
        self.slots[time_slot]['metadata'] = metadata.copy()

        # Critical files that are required, otherwise production will fail.
        # If there are no critical files, empty set([]) is used.
        try:
            critical_segments = self._config.get(self._section,
                                                 "critical_files")
            self.slots[time_slot]['critical_files'] = \
                    self._compose_filenames(time_slot, critical_segments)
        except (NoOptionError, ValueError):
            self.slots[time_slot]['critical_files'] = set([])

        # These files are wanted, but not critical to production
        self.slots[time_slot]['wanted_files'] = \
            self._compose_filenames(time_slot,
                                    self._config.get(self._section,
                                                     "wanted_files"))
        # Name of all the files
        self.slots[time_slot]['all_files'] = \
            self._compose_filenames(time_slot,
                                    self._config.get(self._section,
                                                     "all_files"))

        self.slots[time_slot]['received_files'] = set([])
        self.slots[time_slot]['delayed_files'] = set([])
        self.slots[time_slot]['missing_files'] = set([])
        self.slots[time_slot]['timeout'] = None

    def _compose_filenames(self, time_slot, itm_str):
        """Compose filename set()s based on a pattern and item string.
        itm_str is formated like ':PRO,:EPI' or 'VIS006:8,VIS008:1-8,...'"""

        # Empty set
        result = set()

        # Get copy of metadata
        meta = self.slots[time_slot]['metadata'].copy()

        # Replace variable tags (such as processing time) with a
        # wildcard, as these can't be forecasted.
        try:
            for tag in self._config.get(self._section,
                                        'variable_tags').split(','):
                meta[tag] = '*'
        except NoOptionError:
            pass

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

    def _publish(self, time_slot):
        """Publish file dataset and reinitialize gatherer."""

        data = self.slots[time_slot]

        # Diagnostic logging about delayed ...
        delayed_files = data['delayed_files']
        if len(delayed_files) > 0:
            file_str = ''
            for key in delayed_files:
                file_str += "%s %f seconds, " % (key, delayed_files[key])
            self.logger.warning("Files received late: %s", file_str.strip(', '))
        # and missing files
        missing_files = data['all_files'].difference(data['received_files'])
        if len(missing_files) > 0:
            self.logger.warning("Missing files: %s", ', '.join(missing_files))

        msg = message.Message(self._subject, "dataset", data['metadata'])
        self.logger.info("Sending: %s", str(msg))
        self._publisher.send(str(msg))

        self._clear_data(time_slot)

    def set_logger(self, logger):
        """Set logger."""
        self.logger = logger

    def slot_ready(self, slot):
        """Determine if slot is ready to be published."""
        # If no files have been collected, return False
        if len(slot['received_files']) == 0:
            return False

        time_slot = str(slot['metadata'][self.time_name])
        # If all wanted files have been received, return True
        if slot['wanted_files'].union(\
                slot['critical_files']).issubset(\
                slot['received_files']):
            self.logger.info("All files received for slot %s.",
                             time_slot)
            return True

        if slot['critical_files'].issubset(slot['received_files']):
            # All critical files have been received
            if slot['timeout'] is None:
                # Set timeout
                slot['timeout'] = dt.datetime.utcnow() + self._timeliness
                self.logger.info("Setting timeout to %s for slot %s.",
                                 str(slot['timeout']),
                                 time_slot)
                return False
            elif slot['timeout'] < dt.datetime.utcnow():
                # Timeout reached, collection ready
                self.logger.info("Timeout occured, required files received "
                                 "for slot %s.", time_slot)
                return True
            else:
                pass
        else:
            if slot['timeout'] is None:
                slot['timeout'] = dt.datetime.utcnow() + self._timeliness
                self.logger.info("Setting timeout to %s for slot %s",
                                 str(slot['timeout']),
                                 time_slot)
                return False

            elif slot['timeout'] < dt.datetime.utcnow():
                # Timeout reached, collection is obsolete
                self.logger.warning("Timeout occured and required files "
                                    "were not present, data discarded for "
                                    "slot %s.",
                                    time_slot)
                return None
            else:
                pass

        # Timeout not reached, wait for more files
        return False

    def run(self):
        """Run SegmentGatherer"""
        self._publisher.start()
        self._loop = True
        while self._loop:
            # Check if there are slots ready for publication
            slots = self.slots.copy()
            for slot in slots:
                slot = str(slot)
                status = self.slot_ready(slots[slot])
                if status is True:
                    # Collection ready, publish and remove
                    self._publish(slot)
                    self._clear_data(slot)
                elif status is None:
                    # Collection unfinished and obslote, discard
                    self._clear_data(slot)
                else:
                    # Collection unfinished, wait for more data
                    pass

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
        try:
            mda = self._parser.parse(msg.data["uid"])
        except ValueError:
            self.logger.debug("Unknown file, skipping.")
            return
        time_slot = str(mda[self.time_name])

        # Init metadata etc if this is the first file
        if time_slot not in self.slots:
            self._init_data(msg, mda)

        slot = self.slots[time_slot]

        # Replace variable tags (such as processing time) with a
        # wildcard, as these can't be forecasted.
        try:
            for tag in self._config.get(self._section,
                                       'variable_tags').split(','):
                mda[tag] = '*'
        except NoOptionError:
            pass

        mask = self._parser.compose(mda)

        if mask in slot['received_files']:
            return

        # Add uid and uri
        slot['metadata']['dataset'].append({'uri': msg.data['uri'],
                                            'uid': msg.data['uid']})

        # If critical files have been received but the slot is
        # not complete, add the file to list of delayed files
        if len(slot['critical_files']) > 0 and \
           slot['critical_files'].issubset(slot['received_files']):
            delay = dt.datetime.utcnow() - (slot['timeout'] - self._timeliness)
            slot['delayed_files'][msg.data['uid']] = delay.total_seconds()

        # Add to received files
        slot['received_files'].add(mask)


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

    handlers = []
    if args.log:
        handlers.append(\
            logging.handlers.TimedRotatingFileHandler(args.log,
                                                      "midnight",
                                                      backupCount=7))

    handlers.append(logging.StreamHandler())

    if args.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    for handler in handlers:
        handler.setFormatter(logging.Formatter("[%(levelname)s: %(asctime)s :"
                                               " %(name)s] %(message)s",
                                               '%Y-%m-%d %H:%M:%S'))
        handler.setLevel(loglevel)
        logging.getLogger('').setLevel(loglevel)
        logging.getLogger('').addHandler(handler)

    logging.getLogger("posttroll").setLevel(logging.INFO)
    logger = logging.getLogger("segment_gatherer")


    gatherer = SegmentGatherer(config, args.config_item)
    gatherer.set_logger(logger)
    gatherer.run()


if __name__ == "__main__":
    main()
