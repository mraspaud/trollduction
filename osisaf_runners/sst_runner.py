#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015, 2016 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c14526.ad.smhi.se>

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

"""Posttroll runner for OSISAF SST post-processing
"""
import os
import ConfigParser
import shutil

import logging
LOG = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get('SSTRUNNER_CONFIG_DIR', './')

CONF = ConfigParser.ConfigParser()
CONF.read(os.path.join(CONFIG_PATH, "osisaf_sst_config.cfg"))

MODE = os.getenv("SMHI_MODE")
if MODE is None:
    MODE = "offline"

OPTIONS = {}
for option, value in CONF.items(MODE, raw=True):
    OPTIONS[option] = value

SST_OUTPUT_DIR = OPTIONS['sst_outdir']
try:
    SST_OLD_OUTPUT_DIR = OPTIONS['sst_old_outdir']
    SST_SIR_DIR = OPTIONS['sir_dir']
    SST_SIR_LOCALDIR = OPTIONS['sir_local_dir']
except KeyError:
    SST_OLD_OUTPUT_DIR = None
    SST_SIR_DIR = None
    SST_SIR_LOCALDIR = None


#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'


import sys
from urlparse import urlparse
import posttroll.subscriber
from posttroll.publisher import Publish

from mpop.utils import debug_on
debug_on()
from mpop.satellites import PolarFactory
from datetime import timedelta, datetime

#METOPS = ['Metop-A', 'Metop-B', 'Metop-C']
METOP_NAMES = ['metop02', 'metop01', 'metop03']

INSTRUMENT_NAME = {}  # {'avhrr/3': 'avhrr'}


class SstRunError(Exception):
    pass


def start_sst_processing(sst_file, message, **kwargs):
    """From a posttroll/trallstalker message start the osisaf sst post-
    processing"""

    LOG.info("")
    LOG.info("sst-file dict: " + str(sst_file))
    LOG.info("\tMessage:")
    LOG.info(message)
    urlobj = urlparse(message.data['uri'])

    if 'start_time' in message.data and 'start_date' in message.data:
        dtdate = message.data['start_date']
        dttime = message.data['start_time']
        start_time = datetime(dtdate.year,
                              dtdate.month,
                              dtdate.day,
                              dttime.hour,
                              dttime.minute)
        scene_id = start_time.strftime('%Y%m%d%H%M')
    else:
        LOG.warning("No start time in message!")
        start_time = None
        return sst_file

    if 'end_time' in message.data and 'end_date' in message.data:
        dtdate = message.data['end_date']
        dttime = message.data['end_time']
        end_time = datetime(dtdate.year,
                            dtdate.month,
                            dtdate.day,
                            dttime.hour,
                            dttime.minute)

    else:
        LOG.warning("No end time in message!")
        end_time = None

    if (message.data['platform_name'] in ['Suomi-NPP', ] and
            message.data['instruments'] == 'viirs'):

        path, fname = os.path.split(urlobj.path)
        LOG.debug("path " + str(path) + " filename = " + str(fname))

        instrument = str(message.data['instruments'])
        platform_name = message.data['platform_name']
        sst_file[scene_id] = os.path.join(path, fname)

    elif (message.data['satellite'] in METOP_NAMES and
            message.data['instruments'] == 'avhrr/3'):

        path, fname = os.path.split(urlobj.path)
        LOG.debug("path " + str(path) + " filename = " + str(fname))

        instrument = str(message.data['instruments'])
        platform_name = str("Metop-" + str(message.data['metop_letter']))
        sst_file[scene_id] = os.path.join(path, fname)

    else:
        LOG.debug("Scene is not supported")
        LOG.debug("platform and instrument: " +
                  str(message.data['platform_name']) + " " +
                  str(message.data['instruments']))
        return sst_file

    # Check that the input file really exists:
    if not os.path.exists(sst_file[scene_id]):
        LOG.error("File %s does not exist. Don't do anything...",
                  str(sst_file))
        return sst_file

    LOG.info("Sat and Instrument: " + platform_name + " " + instrument)

    areaid = 'euron1'
    prfx = platform_name.lower() + start_time.strftime("_%Y%m%d_%H") + \
        '_' + str(areaid)
    outname = os.path.join(SST_OUTPUT_DIR, 'osisaf_sst_float_%s.tif' % prfx)
    LOG.info("Output file name: " + str(outname))
    if os.path.exists(outname):
        LOG.warning("File " + str(outname) + " already there. Continue...")
        return sst_file

    orbit = "00000"
    endtime = start_time + timedelta(seconds=60 * 12)
    tslot = start_time
    glbd = PolarFactory.create_scene(
        platform_name, "", INSTRUMENT_NAME.get(instrument, instrument),
        tslot, orbit, variant="DR")

    LOG.debug("Load sst data...")
    glbd.load(['SST'], time_interval=(start_time, endtime))

    LOG.debug("Project data...")
    localdata = glbd.project(areaid)
    img = localdata.image.sst_float()
    img.save(outname, floating_point=True)
    LOG.debug("SST Tiff file stored on area %s", str(areaid))

    # Store some other products for Diana and other users:
    # We should rather use producer.py and the XML product list!
    # FIXME!
    # if SST_OLD_OUTPUT_DIR:
    #     prfx = start_time.strftime("%Y%m%d%H%M")
    #     outname = os.path.join(
    #         SST_OLD_OUTPUT_DIR, 'noaa_osisaf_sstbaltic_%s.png' % prfx)
    #     LOG.info("Output file name: " + str(outname))
    #     img = localdata.image.sst_with_landseamask()
    #     img.save(outname)
    #     LOG.debug("SST PNG file stored on area %s", str(areaid))

    if SST_SIR_LOCALDIR and SST_SIR_DIR:
        img = localdata.image.sst_with_landseamask()
        areaid_str = '{s:{c}<{n}}'.format(s=areaid, n=8, c='_')
        local_filename = os.path.join(SST_SIR_LOCALDIR,
                                      "osafsst_%s%s.png" % (areaid_str,
                                                            start_time.strftime('%y%m%d%H%M')))
        img.save(local_filename)
        sir_filename = os.path.join(SST_SIR_DIR,
                                    "osafsst_%s%s.png_original" % (areaid_str,
                                                                   start_time.strftime('%y%m%d%H%M')))
        shutil.copy(local_filename, sir_filename)

    prfx = platform_name.lower() + start_time.strftime("_%Y%m%d_%H") + \
        '_' + str(areaid)
    outname = os.path.join(SST_OUTPUT_DIR, 'osisaf_sst_%s.tif' % prfx)
    LOG.info("Output file name: " + str(outname))
    LOG.debug("Project data...")
    localdata = glbd.project(areaid)
    img = localdata.image.sst()
    img.save(outname)
    LOG.debug("SST Tiff file stored on area %s!", str(areaid))

    if SST_SIR_LOCALDIR and SST_SIR_DIR:
        areaid = 'baws'
        localdata = glbd.project(areaid)
        img = localdata.image.sst_with_landseamask()
        areaid_str = '{s:{c}<{n}}'.format(s=areaid, n=8, c='_')
        local_filename = os.path.join(SST_SIR_LOCALDIR,
                                      "osafsst_%s%s.png" % (areaid_str,
                                                            start_time.strftime('%y%m%d%H%M')))
        img.save(local_filename)
        sir_filename = os.path.join(SST_SIR_DIR,
                                    "osafsst_%s%s.png_original" % (areaid_str,
                                                                   start_time.strftime('%y%m%d%H%M')))
        shutil.copy(local_filename, sir_filename)

    return sst_file


def sst_live_runner():
    """Listens and triggers processing"""

    LOG.info("*** Start the OSISAF SST post-processing runner:")
    with posttroll.subscriber.Subscribe('', ['OSISAF/GHRSST', ], True) as subscr:
        with Publish('sst_runner', 0) as publisher:
            sstfile = {}
            for msg in subscr.recv():
                sstfile = start_sst_processing(
                    sstfile, msg, publisher=publisher)
                # Cleanup in sstfile registry (keep only the last 5):
                keys = sstfile.keys()
                if len(keys) > 5:
                    keys.sort()
                    sstfile.pop(keys[0])


if __name__ == "__main__":

    handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('sst_runner')

    sst_live_runner()
