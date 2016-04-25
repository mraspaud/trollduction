==========================================================
 Using the gatherer to detect and merge granules together
==========================================================

Make sure mpop is configured. (Add templates for metop)

There are several types of triggers.

Provide a gatherer configuration file.

.. code-block:: ini

 [DEFAULT]
 regions=euron1 afghanistan afhorn

 [local_viirs]
 timeliness=15
 duration=85.4
 service=
 topics=/segment/SDR/1

 [ears_viirs]
 pattern=/data/prod/satellit/ears/viirs/SVMC_{platform_name}_d{start_date:%Y%m%d}_t{start_time:%H%M%S%f}_e{end_time:%H%M%S%f}_b{orbit_number:5d}_c{proctime:%Y%m%d%H%M%S%f}_eum_ops.h5.bz2
 format=SDR_compact
 type=HDF5
 data_processing_level=1B
 platform_name=Suomi-NPP
 sensor=viirs
 timeliness=30
 duration=85.4
 variant=regional

 [ears_avhrr]
 pattern=/data/prod/satellit/ears/avhrr/avhrr_{start_time:%Y%m%d_%H%M%S}_{platform_name}.hrp.bz2
 platform_name=NOAA-19
 format=HRPT
 type=binary
 data_processing_level=0
 duration=60
 sensor=avhrr/3
 timeliness=15
 variant=regional

 [ears_metop-b]
 pattern=/data/prod/satellit/ears/avhrr/AVHR_HRP_{data_processing_level:2s}_M01_{start_time:%Y%m%d%H%M%S}Z_{end_time:%Y%m%d%H%M%S}Z_N_O_{proc_time:%Y%m%d%H%M%S}Z.bz2
 format=EPS
 type=binary
 platform_name=Metop-B
 sensor=avhrr/3
 timeliness=15
 data_processing_level=0
 variant=regional

 [ears_metop-a]
 pattern=/data/prod/satellit/ears/avhrr/AVHR_HRP_{data_processing_level:2s}_M02_{start_time:%Y%m%d%H%M%S}Z_{end_time:%Y%m%d%H%M%S}Z_N_O_{proc_time:%Y%m%d%H%M%S}Z.bz2
 format=EPS
 type=binary
 platform_name=Metop-A
 sensor=avhrr/3
 timeliness=15
 data_processing_level=0
 variant=regional

 [gds_metop-b]
 pattern=/data/prod/satellit/metop2/AVHR_xxx_{data_processing_level:2s}_M01_{start_time:%Y%m%d%H%M%S}Z_{end_time:%Y%m%d%H%M%S}Z_N_O_{proc_time:%Y%m%d%H%M%S}Z
 format=EPS
 type=binary
 platform_name=Metop-B
 sensor=avhrr/3
 timeliness=100
 variant=global

 [gds_metop-a]
 pattern=/data/prod/satellit/metop2/AVHR_xxx_{data_processing_level:2s}_M02_{start_time:%Y%m%d%H%M%S}Z_{end_time:%Y%m%d%H%M%S}Z_N_O_{proc_time:%Y%m%d%H%M%S}Z
 format=EPS
 type=PDS
 platform_name=Metop-A
 sensor=avhrr/3
 timeliness=100
 variant=global

 [regional_terra]
 pattern=/data/prod/satellit/modis/lvl1/thin_MOD021KM.A{start_time:%Y%j.%H%M}.005.{proc_time:%Y%j%H%M%S}.NRT.hdf
 format=EOS_thinned
 type=HDF4
 data_processing_level=1B
 platform_name=EOS-Terra
 sensor=modis
 timeliness=180
 duration=300
 variant=regional

 [regional_aqua]
 pattern=/data/prod/satellit/modis/lvl1/thin_MYD021KM.A{start_time:%Y%j.%H%M}.005.{proc_time:%Y%j%H%M%S}.NRT.hdf
 format=EOS_thinned
 type=HDF4
 data_processing_level=1B
 platform_name=EOS-Aqua
 sensor=modis
 timeliness=180
 duration=300
 variant=regional



Start nameserver if it's not already running.


