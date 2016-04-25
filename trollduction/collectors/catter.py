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

"""Concatenate granules at low level if needed.
"""

from posttroll.subscriber import Subscribe
from posttroll.publisher import Publish
from posttroll.message import Message
from ConfigParser import RawConfigParser, NoOptionError
import logging
from trollsift import compose
from datetime import datetime, timedelta
import bz2
import os.path

logger = logging.getLogger(__name__)

config = RawConfigParser()


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", help="File to log to (defaults to stdout)",
                        default=None)
    parser.add_argument("-v", "--verbose", help="print debug messages too",
                        action="store_true")
    parser.add_argument("config", help="config file to be used")
    opts = parser.parse_args()

    config.read(opts.config)

    if opts.log:
        import logging.handlers
        handler = logging.handlers.TimedRotatingFileHandler(opts.log,
                                                            "midnight",
                                                            backupCount=7)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s: %(asctime)s :"
                                           " %(name)s] %(message)s",
                                           '%Y-%m-%d %H:%M:%S'))
    if opts.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    handler.setLevel(loglevel)
    logging.getLogger('').setLevel(loglevel)
    logging.getLogger('').addHandler(handler)
    logging.getLogger("posttroll").setLevel(logging.INFO)
    logger = logging.getLogger("catter")

    subjects = list(set([config.get(section, "subject")
                         for section in config.sections()]))
    logger.info("Listening to %s", str(subjects))
    with Publish("catter") as pub:
        with Subscribe("", subjects, addr_listener=True) as sub:
            logger.info("Ready, waiting for messages")
            for msg in sub.recv():
                logger.info("Got message for %s", msg.subject)
                if msg.type == "collection":
                    mda = msg.data[0].copy()
                    section = (mda["platform"] + " " +
                               mda["number"] + "/" +
                               mda["level"])
                    if section not in config.sections():
                        logger.debug("Skipping %s", section)
                        continue
                    logger.debug("Starting catting for %s", section)
                    cat = config.get(section, "cat")
                    pattern = config.get(section, "pattern")
                    mda["proc_time"] = datetime.utcnow()
                    mda["end_time"] = msg.data[-1]["end_time"]
                    try:
                        min_length = config.getint(section, 'min_length')
                    except NoOptionError:
                        min_length = 0
                    if mda["end_time"] - mda["start_time"] < timedelta(minutes=min_length):
                        logger.info('Pass too short, skipping: %s to %s', str(mda["start_time"]), str(mda["end_time"]))
                        continue
                    fname = compose(pattern, mda)
                    mda["uri"] = fname
                    mda["filename"] = os.path.basename(fname)
                    if cat == "bz2":
                        with open(fname, "wb") as out:
                            for cmda in msg.data:
                                infile = bz2.BZ2File(cmda["uri"], "r")
                                out.write(infile.read())
                                infile.close()
                    new_msg = Message(msg.subject, "file", mda)
                    logger.info("Done")
                    logger.debug("Sending %s", str(new_msg))
                    pub.send(str(new_msg))
