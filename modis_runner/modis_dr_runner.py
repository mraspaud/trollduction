#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013, 2014, 2015, 2016

# Author(s):

#   Adam Dybbroe   <adam.dybbroe@smhi.se>
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

"""Level-1 processing for Terra/Aqua Modis Direct Readout data. Using the SPA
modis level-1 processor from the NASA Direct Readout Lab (DRL). Listens for
pytroll messages from the Nimbus server (PDS file dispatch) and triggers
processing on direct readout data
"""

import os
import glob
import logging
import socket
from trollduction import get_local_ips

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

SPA_HOME = os.environ.get("SPA_HOME", '')
APPL_HOME = os.environ.get('MODIS_LVL1PROC', '')
ETC_DIR = os.path.join(SPA_HOME, 'etc')

NAVIGATION_HELPER_FILES = ['utcpole.dat', 'leapsec.dat']

MODE = os.getenv("SMHI_MODE")
if MODE is None:
    MODE = "offline"

from datetime import datetime

PACKETFILE_AQUA_PRFX = "P154095715409581540959"
MODISFILE_AQUA_PRFX = "P1540064AAAAAAAAAAAAAA"
MODISFILE_TERRA_PRFX = "P0420064AAAAAAAAAAAAAA"

from urlparse import urlparse
import posttroll.subscriber
from posttroll.publisher import Publish
from posttroll.message import Message


def clean_utcpole_and_leapsec_files(thr_days=60):
    """Clean any old *leapsec.dat* and *utcpole.dat* backup files, older than
    *thr_days* old

    """
    from glob import glob
    from datetime import datetime, timedelta
    import os

    now = datetime.utcnow()
    deltat = timedelta(days=int(thr_days))

    # Make the list of files to clean:
    flist = glob(os.path.join(ETC_DIR, '*.dat_*'))
    for filename in flist:
        lastpart = os.path.basename(filename).split('dat_')[1]
        tobj = datetime.strptime(lastpart, "%Y%m%d%H%M")
        if (now - tobj) > deltat:
            LOG.info("File to old, cleaning: %s " % filename)
            os.remove(filename)

    return


def check_utcpole_and_leapsec_files(thr_days=14):
    """Check if the files *leapsec.dat* and *utcpole.dat* are available in the
    etc directory and check if they are fresh.
    Return True if fresh/new files exists, otherwise False

    """

    from glob import glob
    from datetime import datetime, timedelta

    now = datetime.utcnow()
    tdelta = timedelta(days=int(thr_days))

    files_ok = True
    for bname in NAVIGATION_HELPER_FILES:
        LOG.info("File " + str(bname) + "...")
        filename = os.path.join(ETC_DIR, bname)
        if os.path.exists(filename):
            # Check how old it is:
            realpath = os.path.realpath(filename)
            # Get the timestamp in the file name:
            try:
                tstamp = os.path.basename(realpath).split('.dat_')[1]
            except IndexError:
                files_ok = False
                break
            tobj = datetime.strptime(tstamp, "%Y%m%d%H%M")

            if (now - tobj) > tdelta:
                LOG.info("File too old! File=%s " % filename)
                files_ok = False
                break
        else:
            LOG.info("No navigation helper file: %s" % filename)
            files_ok = False
            break

    return files_ok


def update_utcpole_and_leapsec_files():
    """
    Function to update the ancillary data files *leapsec.dat* and
    *utcpole.dat* used in the navigation of MODIS direct readout data.

    These files need to be updated at least once every 2nd week, in order to
    achieve the best possible navigation.

    """
    import urllib2
    import os
    import sys
    from datetime import datetime

    # Start cleaning any possible old files:
    clean_utcpole_and_leapsec_files(DAYS_KEEP_OLD_ETC_FILES)

    try:
        usock = urllib2.urlopen(URL)
    except urllib2.URLError:
        LOG.warning('Failed opening url: ' + URL)
        return
    else:
        usock.close()

    LOG.info("Start downloading....")
    now = datetime.utcnow()
    timestamp = now.strftime('%Y%m%d%H%M')
    for filename in NAVIGATION_HELPER_FILES:
        try:
            usock = urllib2.urlopen(URL + filename)
        except urllib2.HTTPError:
            LOG.warning("Failed opening file " + filename)
            continue

        data = usock.read()
        usock.close()
        LOG.info("Data retrieved from url...")

        # I store the files with a timestamp attached, in order not to remove
        # the existing files. In case something gets wrong in the download, we
        # can handle this by not changing the sym-links below:
        newname = filename + '_' + timestamp
        outfile = os.path.join(ETC_DIR, newname)
        linkfile = os.path.join(ETC_DIR, filename)
        fd = open(outfile, 'w')
        fd.write(data)
        fd.close()

        LOG.info("Data written to file " + outfile)
        # Here we could make a check on the sanity of the downloaded files:
        # TODO!

        # Update the symlinks (assuming the files are okay):
        LOG.debug("Adding symlink %s -> %s", linkfile, outfile)
        if os.path.islink(linkfile):
            LOG.debug("Unlinking %s", linkfile)
            os.unlink(linkfile)

        try:
            os.symlink(outfile, linkfile)
        except OSError as err:
            LOG.warning(str(err))

    return


# ---------------------------------------------------------------------------
def run_terra_l0l1(pdsfile):
    """Process Terra MODIS level 0 PDS data to level 1a/1b"""

    from subprocess import Popen, PIPE, STDOUT

    working_dir = get_working_dir()

    #fdwork = os.open(working_dir, os.O_RDONLY)
    # os.fchdir(fdwork)

    level1b_home = OPTIONS['level1b_home']
    filetype_terra = OPTIONS['filetype_terra']
    geofile_terra = OPTIONS['geofile_terra']
    level1a_terra = OPTIONS['level1a_terra']
    level1b_terra = OPTIONS['level1b_terra']
    level1b_250m_terra = OPTIONS['level1b_250m_terra']
    level1b_500m_terra = OPTIONS['level1b_500m_terra']

    # Get the observation time from the filename as a datetime object:
    bname = os.path.basename(pdsfile)
    obstime = datetime.strptime(bname, filetype_terra)

    # level1_home
    proctime = datetime.now()
    lastpart = proctime.strftime("%Y%j%H%M%S.hdf")
    firstpart = obstime.strftime(level1b_terra)
    mod021km_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)
    firstpart = obstime.strftime(level1b_250m_terra)
    mod02qkm_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)
    firstpart = obstime.strftime(level1b_500m_terra)
    mod02hkm_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)
    lastpart = proctime.strftime("%Y%j%H%M%S.hdf")
    firstpart = obstime.strftime(level1a_terra)
    mod01_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)
    firstpart = obstime.strftime(geofile_terra)
    mod03_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)

    retv = {'mod021km_file': mod021km_file,
            'mod02hkm_file': mod02hkm_file,
            'mod02qkm_file': mod02qkm_file,
            'level1a_file': mod01_file,
            'geo_file': mod03_file}

    mod01files = glob.glob("%s/%s*hdf" % (level1b_home, firstpart))
    if len(mod01files) > 0:
        LOG.warning(
            "Level 1 file for this scene already exists: %s" % mod01files[0])
        # return retv

    LOG.info("Level-1 filename: " + str(mod01_file))
    satellite = "Terra"
    wrapper_home = os.path.join(SPA_HOME, "modisl1db/wrapper/l0tol1")
    # cmdstr = ("%s/run modis.pds %s sat %s modis.mxd01 %s modis.mxd03 %s" %
    #           (wrapper_home, pdsfile, satellite, mod01_file, mod03_file))
    cmdl = ["%s/run" % wrapper_home,
            "modis.pds", pdsfile,
            "sat", satellite,
            "modis.mxd01", mod01_file,
            "modis.mxd03", mod03_file]
    LOG.debug("Run command: " + str(cmdl))
    # Run the command:
    modislvl1b_proc = Popen(cmdl, shell=False,
                            cwd=working_dir,
                            stderr=PIPE, stdout=PIPE)

    while True:
        line = modislvl1b_proc.stdout.readline()
        if not line:
            break
        LOG.info(line)

    while True:
        errline = modislvl1b_proc.stderr.readline()
        if not errline:
            break
        LOG.info(errline)

    modislvl1b_proc.poll()
    modislvl1b_status = modislvl1b_proc.returncode
    LOG.debug("Return code from modis lvl1b proc = " + str(modislvl1b_status))
    if modislvl1b_status != 0:
        LOG.error("Failed in the Terra level-1 processing!")
        return None

    # Now do the level1a-1b processing:
    lut_home = os.path.join(SPA_HOME, "modisl1db/algorithm/data/modist/cal")
    refl_lut = os.path.join(lut_home, "MOD02_Reflective_LUTs.V6.1.6.0_OC.hdf")
    emiss_lut = os.path.join(lut_home, "MOD02_Emissive_LUTs.V6.1.6.0_OC.hdf")
    qa_lut = os.path.join(lut_home, "MOD02_QA_LUTs.V6.1.6.0_OC.hdf")

    wrapper_home = os.path.join(SPA_HOME, "modisl1db/wrapper/l1atob")
    # cmdstr = ("%s/run modis.mxd01 %s modis.mxd03 %s modis_reflective_luts %s modis_emissive_luts %s modis_qa_luts %s modis.mxd021km %s modis.mxd02hkm %s modis.mxd02qkm %s" %
    #           (wrapper_home, mod01_file, mod03_file,
    # refl_lut, emiss_lut, qa_lut, mod021km_file, mod02hkm_file,
    # mod02qkm_file))
    cmdl = ["%s/run" % wrapper_home, "modis.mxd01",
            mod01_file, "modis.mxd03", mod03_file,
            "modis_reflective_luts", refl_lut,
            "modis_emissive_luts", emiss_lut,
            "modis_qa_luts", qa_lut,
            "modis.mxd021km", mod021km_file,
            "modis.mxd02hkm", mod02hkm_file,
            "modis.mxd02qkm", mod02qkm_file]

    LOG.debug("Run command: " + str(cmdl))
    # Run the command:
    modislvl1b_proc = Popen(cmdl, shell=False,
                            cwd=working_dir,
                            stderr=PIPE, stdout=PIPE)

    while True:
        line = modislvl1b_proc.stdout.readline()
        if not line:
            break
        LOG.info(line)

    while True:
        errline = modislvl1b_proc.stderr.readline()
        if not errline:
            break
        LOG.info(errline)

    modislvl1b_proc.poll()
    modislvl1b_status = modislvl1b_proc.returncode
    LOG.debug("Return code from modis lvl1b proc = " + str(modislvl1b_status))
    if modislvl1b_status != 0:
        LOG.error("Failed in the Terra level-1 processing!")
        return None

    return retv


def get_working_dir():
    working_dir = OPTIONS['working_dir']
    if not os.path.exists(working_dir):
        try:
            os.makedirs(working_dir)
        except OSError:
            LOG.error("Failed creating working directory %s" % working_dir)
            working_dir = '/tmp'
            LOG.info("Will use /tmp")
    return working_dir


def run_aqua_gbad(obs_time):
    """Run the gbad for aqua"""

    from subprocess import Popen, PIPE, STDOUT

    working_dir = get_working_dir()

    level0_home = OPTIONS['level0_home']
    packetfile = os.path.join(level0_home,
                              obs_time.strftime(OPTIONS['packetfile_aqua']))

    att_dir = OPTIONS['attitude_home']
    eph_dir = OPTIONS['ephemeris_home']
    spa_config_file = OPTIONS['spa_config_file']
    att_file = os.path.basename(packetfile).split('.PDS')[0] + '.att'
    att_file = os.path.join(att_dir, att_file)
    eph_file = os.path.basename(packetfile).split('.PDS')[0] + '.eph'
    eph_file = os.path.join(eph_dir, eph_file)
    LOG.info("eph-file = " + eph_file)

    wrapper_home = SPA_HOME + "/gbad/wrapper/gbad"
    # cmdstr = ("%s/run aqua.gbad.pds %s aqua.gbad_att %s aqua.gbad_eph %s configurationfile %s" %
    #           (wrapper_home, packetfile, att_file, eph_file, spa_config_file))
    cmdl = ["%s/run" % wrapper_home, "aqua.gbad.pds",
            packetfile, "aqua.gbad_att",  att_file,
            "aqua.gbad_eph", eph_file,
            "configurationfile", spa_config_file
            ]
    LOG.info("Command: " + str(cmdl))
    # Run the command:
    modislvl1b_proc = Popen(cmdl, shell=False,
                            cwd=working_dir,
                            stderr=PIPE, stdout=PIPE)

    while True:
        line = modislvl1b_proc.stdout.readline()
        if not line:
            break
        LOG.info(line)

    while True:
        errline = modislvl1b_proc.stderr.readline()
        if not errline:
            break
        LOG.info(errline)

    modislvl1b_proc.poll()
    modislvl1b_status = modislvl1b_proc.returncode
    LOG.debug("Return code from modis lvl1b proc = " + str(modislvl1b_status))
    if modislvl1b_status != 0:
        LOG.error("Failed in the Aqua gbad processing!")
        return None, None

    return att_file, eph_file


def run_aqua_l0l1(pdsfile):
    """Process Aqua MODIS level 0 PDS data to level 1a/1b"""
    import os
    from subprocess import Popen, PIPE, STDOUT

    # unicode -> ascii ignoring any non-ascii characters!:
    #pdsfile = pdsfile.encode('ascii', 'ignore')

    working_dir = get_working_dir()

    #ephemeris_home = OPTIONS['ephemeris_home']
    #attitude_home = OPTIONS['attitude_home']
    level1b_home = OPTIONS['level1b_home']
    filetype_aqua = OPTIONS['filetype_aqua']
    geofile_aqua = OPTIONS['geofile_aqua']
    level1a_aqua = OPTIONS['level1a_aqua']
    level1b_aqua = OPTIONS['level1b_aqua']
    level1b_250m_aqua = OPTIONS['level1b_250m_aqua']
    level1b_500m_aqua = OPTIONS['level1b_500m_aqua']

    # Get the observation time from the filename as a datetime object:
    bname = os.path.basename(pdsfile)
    obstime = datetime.strptime(bname, filetype_aqua)

    # Get ephemeris and attitude names! FIXME!
    attitude, ephemeris = run_aqua_gbad(obstime)
    #ephemeris = "%s/P15409571540958154095911343000923001.eph" % ephemeris_home
    #attitude  = "%s/P15409571540958154095911343000923001.att" % attitude_home
    if not attitude or not ephemeris:
        LOG.error("Failed producing the attitude and/or the ephemeris file(s)")
        return None

    leapsec_name = os.path.join(ETC_DIR, "leapsec.dat")
    utcpole_name = os.path.join(ETC_DIR, "utcpole.dat")
    geocheck_threshold = 50  # Hardcoded threshold!

    proctime = datetime.now()
    lastpart = proctime.strftime("%Y%j%H%M%S.hdf")
    firstpart = obstime.strftime(level1a_aqua)
    mod01files = glob.glob("%s/%s*hdf" % (level1b_home, firstpart))
    if len(mod01files) > 0:
        LOG.warning(
            "Level 1 file for this scene already exists: %s" % mod01files[0])
        # return

    mod01_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)
    firstpart = obstime.strftime(geofile_aqua)
    mod03_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)

    LOG.info("Level-1 filename: " + str(mod01_file))
    satellite = "Aqua"
    wrapper_home = os.path.join(SPA_HOME, "modisl1db/wrapper/l0tol1")
    # cmdstr = ("%s/run modis.pds %s sat %s modis.mxd01 %s modis.mxd03 %s gbad_eph %s gbad_att %s leapsec %s utcpole %s geocheck_threshold %s" %
    #           (wrapper_home, pdsfile, satellite, mod01_file, mod03_file,
    #            ephemeris, attitude, leapsec_name, utcpole_name, geocheck_threshold))
    # import shlex
    # cmdstr = shlex.split(cmdstr)

    cmdlist = ['%s/run' % wrapper_home]
    cmdlist.append('modis.pds')
    cmdlist.append(pdsfile)
    cmdlist.append('sat')
    cmdlist.append(satellite)
    cmdlist.append('modis.mxd01')
    cmdlist.append(mod01_file)
    cmdlist.append('modis.mxd03')
    cmdlist.append(mod03_file)
    cmdlist.append('gbad_eph')
    cmdlist.append(ephemeris)
    cmdlist.append('gbad_att')
    cmdlist.append(attitude)
    cmdlist.append('leapsec')
    cmdlist.append(leapsec_name)
    cmdlist.append('utcpole')
    cmdlist.append(utcpole_name)
    cmdlist.append('geocheck_threshold')
    cmdlist.append(str(geocheck_threshold))
    LOG.debug("Run command: " + str(cmdlist))
    #my_env = os.environ.copy()
    # Run the command:
    modislvl1b_proc = Popen(cmdlist, shell=False,
                            cwd=working_dir,
                            # env=my_env,
                            stderr=PIPE, stdout=PIPE)

    while True:
        line = modislvl1b_proc.stdout.readline()
        if not line:
            break
        LOG.info(line)

    while True:
        errline = modislvl1b_proc.stderr.readline()
        if not errline:
            break
        LOG.info(errline)

    modislvl1b_proc.poll()
    modislvl1b_status = modislvl1b_proc.returncode
    LOG.debug("Return code from modis lvl1b proc = " + str(modislvl1b_status))
    if modislvl1b_status != 0:
        LOG.error("Failed in the Aqua level-1 processing!")
        return None

    # Now do the level1a-1b processing:
    lut_home = os.path.join(SPA_HOME, "modisl1db/algorithm/data/modisa/cal")
    refl_lut = os.path.join(lut_home, "MYD02_Reflective_LUTs.V6.1.7.1_OCb.hdf")
    emiss_lut = os.path.join(lut_home, "MYD02_Emissive_LUTs.V6.1.7.1_OCb.hdf")
    qa_lut = os.path.join(lut_home, "MYD02_QA_LUTs.V6.1.7.1_OCb.hdf")

    wrapper_home = os.path.join(SPA_HOME, "modisl1db/wrapper/l1atob")
    # level1_home
    proctime = datetime.now()
    lastpart = proctime.strftime("%Y%j%H%M%S.hdf")
    firstpart = obstime.strftime(level1b_aqua)
    mod021km_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)
    firstpart = obstime.strftime(level1b_250m_aqua)
    mod02qkm_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)
    firstpart = obstime.strftime(level1b_500m_aqua)
    mod02hkm_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)
    cmdstr = ("%s/run modis.mxd01 %s modis.mxd03 %s modis_reflective_luts %s modis_emissive_luts %s modis_qa_luts %s modis.mxd021km %s modis.mxd02hkm %s modis.mxd02qkm %s" %
              (wrapper_home, mod01_file, mod03_file,
               refl_lut, emiss_lut, qa_lut, mod021km_file, mod02hkm_file, mod02qkm_file))
    import shlex
    cmdstr = shlex.split(cmdstr)
    LOG.debug("Run command: " + str(cmdstr))
    modislvl1b_proc = Popen(cmdstr, shell=False,
                            cwd=working_dir,
                            stderr=PIPE, stdout=PIPE)

    while True:
        line = modislvl1b_proc.stdout.readline()
        if not line:
            break
        LOG.info(line)

    while True:
        errline = modislvl1b_proc.stderr.readline()
        if not errline:
            break
        LOG.info(errline)

    modislvl1b_proc.poll()
    modislvl1b_status = modislvl1b_proc.returncode
    LOG.debug("Return code from modis lvl1b proc = " + str(modislvl1b_status))
    if modislvl1b_status != 0:
        LOG.error("Failed in the Aqua level-1 processing!")
        return None

    retv = {'mod021km_file': mod021km_file,
            'mod02hkm_file': mod02hkm_file,
            'mod02qkm_file': mod02qkm_file,
            'level1a_file': mod01_file,
            'geo_file': mod03_file}

    return retv


def send_message(this_publisher, msg):
    """Send a message for down-stream processing"""

    LOG.debug("sending: " + str(msg))
    this_publisher.send(msg)

    return


def create_message(mda, filename, level):
    LOG.debug("mda: = " + str(mda))
    LOG.debug("type(mda): " + str(type(mda)))
    to_send = mda.copy()
    if isinstance(filename, (list, tuple, set)):
        del to_send['uri']
        del to_send['uid']
        to_send['dataset'] = [{'uri': 'file://' + fname,
                               'uid': os.path.basename(fname)}
                              for fname in filename]
        mtype = 'dataset'
    else:
        to_send['uri'] = ('file://' + filename)
        to_send['uid'] = os.path.basename(filename)
        mtype = 'file'
    to_send['format'] = 'EOS'
    to_send['data_processing_level'] = level
    to_send['type'] = 'HDF4'
    to_send['sensor'] = 'modis'

    message = Message('/'.join(('',
                                str(to_send['format']),
                                str(to_send['data_processing_level']),
                                'norrköping',
                                MODE,
                                'polar'
                                'direct_readout')),
                      mtype, to_send).encode()
    return message


def start_modis_lvl1_processing(eos_files,
                                mypublisher, message):
    """From a posttroll message start the modis lvl1 processing"""

    LOG.info("")
    LOG.info("EOS files: " + str(eos_files))
    LOG.info("\tMessage:")
    LOG.info(message)

    urlobj = urlparse(message.data['uri'])
    LOG.info("Server = " + str(urlobj.netloc))
    server = urlobj.netloc
    url_ip = socket.gethostbyname(server)
    if urlobj.netloc and (url_ip not in get_local_ips()):
        LOG.warning("Server %s not the current one: %s",
                    str(server), socket.gethostname())
        return eos_files

    LOG.info("Ok... " + str(urlobj.netloc))
    LOG.info("Sat and Instrument: " + str(message.data['platform_name']) + " "
             + str(message.data['sensor']))

    if 'start_time' in message.data:
        start_time = message.data['start_time']
    else:
        LOG.warning("No start time in message!")
        start_time = None

    if 'end_time' in message.data:
        end_time = message.data['end_time']
    else:
        LOG.warning("No end time in message!")
        end_time = None

    if (message.data['platform_name'] == "EOS-Terra" and
            message.data['sensor'] == 'modis'):
        orbnum = message.data.get('orbit_number', None)

        path, fname = os.path.split(urlobj.path)
        LOG.debug("path " + str(path) + " filename = " + str(fname))
        if fname.startswith(MODISFILE_TERRA_PRFX) and fname.endswith('001.PDS'):
            # Check if the file exists:
            if not os.path.exists(urlobj.path):
                LOG.warning("File is reported to be dispatched " +
                            "but is not there! File = " +
                            urlobj.path)
                return eos_files

            # Do processing:
            LOG.info("Level-0 to lvl1 processing on terra start!" +
                     " Start time = " + str(start_time))
            # Start checking and dowloading the luts (utcpole.dat and
            # leapsec.dat):
            LOG.info("Checking the modis luts and updating " +
                     "from internet if necessary!")
            fresh = check_utcpole_and_leapsec_files(DAYS_BETWEEN_URL_DOWNLOAD)
            if fresh:
                LOG.info(
                    "Files in etc dir are fresh! No url downloading....")
            else:
                LOG.warning("Files in etc are non existent or too old. " +
                            "Start url fetch...")
                update_utcpole_and_leapsec_files()

            if orbnum:
                LOG.info("Orb = %d" % orbnum)
            LOG.info("File = " + str(urlobj.path))
            result_files = run_terra_l0l1(urlobj.path)
            if not result_files:
                LOG.error("No files produced...")
                # Clean register: eos_files dict
                LOG.info('Clean the internal eos_files register')
                eos_files = {}
                return eos_files

            LOG.info("Result files: " + str(result_files))
            # Assume everything has gone well!
            # Add intelligence to run-function. FIXME!
            # Now publish:

            l1b_files = [result_files[key] for key in ['geo_file',
                                                       'mod021km_file',
                                                       'mod02hkm_file',
                                                       'mod02qkm_file']]
            LOG.debug("Message:")
            LOG.debug(message)
            LOG.debug("Message data: " + str(message.data))
            send_message(mypublisher, create_message(message.data,
                                                     l1b_files,
                                                     "1B"))

            l1a_file = result_files['level1a_file']

            send_message(mypublisher, create_message(message.data,
                                                     l1a_file,
                                                     "1A"))

    elif (message.data['platform_name'] == "EOS-Aqua" and
          (message.data['sensor'] == 'modis' or
           message.data['sensor'] == 'gbad')):
        orbnum = message.data.get('orbit_number')

        if start_time:
            scene_id = start_time.strftime('%Y%m%d%H%M')
        else:
            LOG.warning("No start time!!!")
            return eos_files

        path, fname = os.path.split(urlobj.path)
        LOG.debug("Path and filename: " + str(path) + ' ' + str(fname))
        if ((fname.find(MODISFILE_AQUA_PRFX) == 0 or
             fname.find(PACKETFILE_AQUA_PRFX) == 0) and
                fname.endswith('001.PDS')):
            # Check if the file exists:
            if not os.path.exists(urlobj.path):
                LOG.warning("File is reported to be dispatched " +
                            "but is not there! File = " +
                            urlobj.path)
                return eos_files

            if not scene_id in eos_files:
                eos_files[scene_id] = []
            if len(eos_files[scene_id]) == 0:
                eos_files[scene_id] = [urlobj.path]
            else:
                if not urlobj.path in eos_files[scene_id]:
                    eos_files[scene_id].append(urlobj.path)

        if scene_id in eos_files and len(eos_files[scene_id]) == 2:
            LOG.info("aqua files with scene-id = %r :" % scene_id +
                     str(eos_files[scene_id]))

            aquanames = [os.path.basename(s) for s in eos_files[scene_id]]
            LOG.info('aquanames: ' + str(aquanames))

            LOG.debug("Message:")
            LOG.debug(message)
            LOG.debug("Message data: " + str(message.data))

            if (aquanames[0].find(MODISFILE_AQUA_PRFX) == 0 and
                    aquanames[1].find(PACKETFILE_AQUA_PRFX) == 0):
                modisfile = eos_files[scene_id][0]
            elif (aquanames[1].find(MODISFILE_AQUA_PRFX) == 0 and
                  aquanames[0].find(PACKETFILE_AQUA_PRFX) == 0):
                modisfile = eos_files[scene_id][1]
            else:
                LOG.error("Either MODIS file or packet file not there!?")
                return eos_files

            # Do processing:
            LOG.info("Level-0 to lvl1 processing on aqua start! " +
                     "Scene = %r" % scene_id)

            # Start checking and dowloading the luts (utcpole.dat and
            # leapsec.dat):
            LOG.info("Checking the modis luts and updating " +
                     "from internet if necessary!")
            fresh = check_utcpole_and_leapsec_files(DAYS_BETWEEN_URL_DOWNLOAD)
            if fresh:
                LOG.info(
                    "Files in etc dir are fresh! No url downloading....")
            else:
                LOG.warning("Files in etc are non existent or too old. " +
                            "Start url fetch...")
                update_utcpole_and_leapsec_files()

            LOG.info("File = " + str(modisfile))
            result_files = run_aqua_l0l1(modisfile)

            # Clean register: aqua_files dict
            LOG.info('Clean the internal eos_files register')
            eos_files = {}

            if not result_files:
                LOG.error("No files produced...")
                return eos_files

            LOG.debug("Result files: " + str(result_files))

            LOG.debug("Message:")
            LOG.debug(message)
            LOG.debug("Message data: " + str(message.data))
            # Now publish:
            l1b_files = [result_files[key] for key in ['geo_file',
                                                       'mod021km_file',
                                                       'mod02hkm_file',
                                                       'mod02qkm_file']]
            send_message(mypublisher, create_message(message.data,
                                                     l1b_files,
                                                     "1B"))

            l1a_file = result_files['level1a_file']
            send_message(mypublisher, create_message(message.data,
                                                     l1a_file,
                                                     "1B"))

    return eos_files


def modis_live_runner():
    """Listens and triggers processing"""

    # Roll over log files at application start:
    try:
        LOG.handlers[0].doRollover()
    except AttributeError:
        LOG.warning("No log rotation supported for this handler...")
    except IndexError:
        LOG.debug("No handlers to rollover")
    LOG.info("*** Start the MODIS level-1 runner:")
    with posttroll.subscriber.Subscribe('receiver', ['PDS/0', ], True) as subscr:
        with Publish('modis_dr_runner', 0) as publisher:
            modisfiles = {}
            for msg in subscr.recv():
                modisfiles = start_modis_lvl1_processing(modisfiles,
                                                         publisher, msg)


# ---------------------------------------------------------------------------
if __name__ == "__main__":

    import sys
    from logging import handlers
    import argparse
    import ConfigParser

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config-file",
                        required=True,
                        dest="config_file",
                        type=str,
                        default=None,
                        help="The file containing configuration parameters.")
    parser.add_argument("-l", "--log-file", dest="log",
                        type=str,
                        default=None,
                        help="The file to log to (stdout per default).")

    args = parser.parse_args()

    CONF = ConfigParser.ConfigParser()

    print "Read config from", args.config_file

    CONF.read(args.config_file)

    OPTIONS = {}
    for option, value in CONF.items(MODE, raw=True):
        OPTIONS[option] = value

    DAYS_BETWEEN_URL_DOWNLOAD = OPTIONS.get('days_between_url_download', 14)
    DAYS_KEEP_OLD_ETC_FILES = OPTIONS.get('days_keep_old_etc_files', 60)
    URL = OPTIONS['url_modis_navigation']

    if args.log is not None:
        ndays = int(OPTIONS.get("log_rotation_days", 1))
        ncount = int(OPTIONS.get("log_rotation_backup", 7))
        handler = handlers.TimedRotatingFileHandler(args.log,
                                                    when='midnight',
                                                    interval=ndays,
                                                    backupCount=ncount,
                                                    encoding=None,
                                                    delay=False,
                                                    utc=True)
        handler.doRollover()
    else:
        handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('modis_dr_runner')
    LOG.debug("Welcome to the modis_dr_runner!")
    LOG.debug("SMHI_MODE = " + str(MODE))

    modis_live_runner()

    #aqua_modis_file = '/san1/polar_in/direct_readout/modis/P1540064AAAAAAAAAAAAAA12298130323001.PDS'

    # print DAYS_BETWEEN_URL_DOWNLOAD
    # LOG.info("Checking the modis luts and updating " +
    #         "from internet if necessary!")
    #fresh = check_utcpole_and_leapsec_files(DAYS_BETWEEN_URL_DOWNLOAD)
    # print "fresh: ", fresh
    # if fresh:
    #    LOG.info("Files in etc dir are fresh! No url downloading....")
    # else:
    #    LOG.warning("Files in etc are non existent or too old. " +
    #                "Start url fetch...")
    #    update_utcpole_and_leapsec_files()

    #LOG.info("File = " + str(aqua_modis_file))

    #lvl1filename = run_aqua_l0l1(aqua_modis_file)
