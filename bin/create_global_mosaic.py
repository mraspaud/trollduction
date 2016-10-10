#!/usr/bin/env python

import sys
import numpy as np
import Image
import logging
import logging.handlers
import datetime as dt
import Queue
import yaml
import time
import os

try:
    import scipy.ndimage as ndi
except ImportError:
    ndi = None

from trollsift import compose
from mpop.imageo.geo_image import GeoImage
from mpop.projector import get_area_def
from trollduction.listener import ListenerContainer

# These longitudinally valid ranges are mid-way points calculated from
# satellite locations assuming the given satellites are in use
LON_LIMITS = {'Meteosat-11': [-37.5, 20.75],
              'Meteosat-10': [-37.5, 20.75],
              'Meteosat-8': [20.75, 91.1],
              'Himawari-8': [91.1, -177.15],
              'GOES-15': [-177.15, -105.],
              'GOES-13': [-105., -37.5],
              'Meteosat-7': [41.5, 41.50001], # placeholder
              'GOES-R': [-90., -90.0001] # placeholder
}

def calc_pixel_mask_limits(adef, lon_limits):
    """Calculate pixel intervals from longitude ranges."""
    # We'll assume global grid from -180 to 180 longitudes
    scale = 360./adef.shape[1] # degrees per pixel
    
    left_limit = int((lon_limits[0] + 180)/scale)
    right_limit = int((lon_limits[1] + 180)/scale)

    # Satellite data spans 180th meridian
    if right_limit < left_limit:
        return [[right_limit, left_limit]]
    else:
        return [[0, left_limit], [right_limit, adef.shape[1]]]

def read_image(fname, tslot, adef, lon_limits=None):
    """Read image to numpy array"""
    print "Reading", fname
    # Convert to float32 to save memory in later steps
    img = np.array(Image.open(fname)).astype(np.float32)
    mask = img[:, :, 3]

    # Mask overlapping areas away
    if lon_limits:
        for sat in lon_limits:
            if sat in fname:
                mask_limits = calc_pixel_mask_limits(adef, lon_limits[sat])
                for lim in mask_limits:
                    mask[:, lim[0]:lim[1]] = 0
                break

    mask = mask == 0

    chans = []
    for i in range(4):
        chans.append(np.ma.masked_where(mask, img[:, :, i]/255.))

    return GeoImage(chans, adef, tslot, fill_value=None, mode="RGBA",
                    crange=((0, 1), (0, 1), (0, 1), (0, 1)))

def create_world_composite(fnames, tslot, adef_name, sat_limits,
                           blend=None, img=None):
    adef = get_area_def(adef_name)
    for fname in fnames:
        next_img = read_image(fname, tslot, adef, sat_limits)

        if img is None:
            img = next_img
        else:
            img_mask = reduce(np.ma.mask_or,
                              [chn.mask for chn in img.channels])
            next_img_mask = reduce(np.ma.mask_or,
                                   [chn.mask for chn in next_img.channels])

            chmask = np.logical_and(img_mask, next_img_mask)

            if blend and ndi:
                scaled_erosion_size = \
                    blend["erosion_width"] * (float(img.width) / 1000.0)
                scaled_smooth_width = \
                    blend["smooth_width"] * (float(img.width) / 1000.0)
                alpha = np.ones(next_img_mask.shape, dtype='float')
                alpha[next_img_mask] = 0.0
                smooth_alpha = ndi.uniform_filter(
                    ndi.grey_erosion(alpha, size=(scaled_erosion_size,
                                                  scaled_erosion_size)),
                    scaled_smooth_width)
                smooth_alpha[img_mask] = alpha[img_mask]


            dtype = img.channels[0].dtype
            chdata = np.zeros(img_mask.shape, dtype=dtype)

            for i in range(3):
                if blend and ndi:
                    if blend["scale"]:
                        chmask2 = np.invert(chmask)
                        idxs = img.channels[i] == 0
                        chmask2[idxs] = False
                        if np.sum(chmask2) == 0:
                            scaling = 1.0
                        else:
                            scaling = \
                                np.nanmean(next_img.channels[i][chmask2]) / \
                                np.nanmean(img.channels[i][chmask2])
                            if not np.isfinite(scaling):
                                scaling = 1.0
                        if scaling == 0.0:
                            scaling = 1.0
                    else:
                        scaling = 1.0

                    chdata = \
                        next_img.channels[i].data * smooth_alpha / scaling + \
                        img.channels[i].data * (1 - smooth_alpha)
                else:
                    chdata[img_mask] = next_img.channels[i].data[img_mask]
                    chdata[next_img_mask] = img.channels[i].data[next_img_mask]

                img.channels[i] = np.ma.masked_where(chmask, chdata)

            chdata = np.max(np.dstack((img.channels[3].data,
                                       next_img.channels[3].data)),
                            2)
            img.channels[3] = np.ma.masked_where(chmask, chdata)

    return img


class WorldCompositeDaemon(object):

    logger = logging.getLogger(__name__)

    def __init__(self, config):
        self.config = config
        self.slots = {}
        # slots = {tslot: {composite: {"img": None,
        #                              "num": 0},
        #                  "timeout": None}}

        self._listener = ListenerContainer(topics=config["topics"])
        self._loop = False
        self.adef = get_area_def(config["area_def"])

    def run(self):
        """Listen to messages and make global composites"""

        num_expected = self.config["num_expected"]
        lon_limits = LON_LIMITS.copy()
        try:
            lon_limits.update(self.config["lon_limits"])
        except KeyError:
            pass
        except TypeError:
            lon_limits = None

        try:
            blend = self.config["blend_settings"]
        except KeyError:
            blend = None

        self._loop = True

        while self._loop:

            # Check timeouts and completed composites
            check_time = dt.datetime.utcnow()

            empty_slots = []
            for slot in self.slots:
                for composite in self.slots[slot].keys():
                    if (check_time > self.slots[slot][composite]["timeout"] or
                        self.slots[slot][composite]["num"] == num_expected):
                        file_parts = {'composite': composite,
                                      'nominal_time': slot,
                                      'areaname': self.config["area_def"]}
                        self.logger.info("Building composite %s for slot %s",
                                         composite, str(slot))
                        fnames = self.slots[slot][composite]["fnames"]
                        fname_out = compose(self.config["out_pattern"],
                                            file_parts)
                        # Check if we already have an image with this filename
                        try:
                            img = read_image(fname_out, slot,
                                             self.config["area_def"],
                                             lon_limits)
                        except IOError:
                            img = None
                        img = create_world_composite(fnames,
                                                     slot,
                                                     self.config["area_def"],
                                                     lon_limits,
                                                     blend=blend, img=img)
                        self.logger.info("Saving %s", fname_out)
                        img.save(fname_out)
                        del self.slots[slot][composite]
                        del img
                        img = None
                # Remove empty slots
                if len(self.slots[slot]) == 0:
                    empty_slots.append(slot)

            for slot in empty_slots:
                self.logger.debug("Removing empty time slot")
                del self.slots[slot]

            msg = None
            try:
                msg = self._listener.queue.get(True, 1)
            except KeyboardInterrupt:
                self._listener.stop()
                return
            except Queue.Empty:
                continue

            if msg.type == "file":
                self.logger.debug("New message received: %s", str(msg.data))
                fname = msg.data["uri"]
                tslot = msg.data["nominal_time"]
                composite = msg.data["productname"]
                if tslot not in self.slots:
                    self.slots[tslot] = {}
                if composite not in self.slots[tslot]:
                    self.slots[tslot][composite] = \
                        {"fnames": [], "num": 0,
                         "timeout": dt.datetime.utcnow() + \
                         dt.timedelta(minutes=self.config["timeout"])}
                self.slots[tslot][composite]["fnames"].append(fname)
                self.slots[tslot][composite]["num"] += 1

    def stop(self):
        """Stop"""
        self.logger.info("Stopping WorldCompositor")
        self._listener.stop()

    def set_logger(self, logger):
        """Set logger."""
        self.logger = logger

def main():
    """main()"""

    with open(sys.argv[1], "r") as fid:
        config = yaml.load(fid)

    try:
        if config["use_utc"]:
            os.environ["TZ"] = "UTC"
            time.tzset()
    except KeyError:
        pass

    # TODO: move log config to config file

    handlers = []
    handlers.append(\
            logging.handlers.TimedRotatingFileHandler(config["log_fname"],
                                                      "midnight",
                                                      backupCount=21))

    handlers.append(logging.StreamHandler())

    try:
        loglevel = getattr(logging, config["log_level"])
    except KeyError:
        loglevel = logging.INFO

    for handler in handlers:
        handler.setFormatter(logging.Formatter("[%(levelname)s: %(asctime)s :"
                                               " %(name)s] %(message)s",
                                               '%Y-%m-%d %H:%M:%S'))
        handler.setLevel(loglevel)
        logging.getLogger('').setLevel(loglevel)
        logging.getLogger('').addHandler(handler)

    logger = logging.getLogger("WorldComposite")

    # Create and start compositor
    compositor = WorldCompositeDaemon(config)
    compositor.set_logger(logger)
    compositor.run()


if __name__ == "__main__":
    main()
