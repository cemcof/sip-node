import mrcfile
import tifffile
import PIL.Image as PILImage
import common
import numpy as np
import os
import sys

class Image:
    def __init__(self ,data=[] ,data1D=[] ,xsize=1 ,ysize=1 ,zsize=1 ,xapix=0 ,yapix=0 ,zapix=0):
        self.data = data
        self.data1D = data1D
        self.xsize = xsize
        self.ysize = ysize
        self.zsize = zsize
        self.xapix = xapix
        self.yapix = yapix
        self.zapix = zapix

    def readMRC(self ,name ,header=False):
        # reads the MRC file and assigns the data and header parameters to the corresponding objects
        # name - name of the image file in the MRC format
        # header - True - reads only the image header

        try:
            if header:
                with mrcfile.open(name, 'r' ,header_only=True) as mrc:
                    self.xsize = mrc.header.nx
                    self.ysize = mrc.header.ny
                    self.zsize = mrc.header.nz
                    self.xapix, self.yapix, self.zapix = mrc.voxel_size.tolist()
            else:
                with mrcfile.open(name, 'r') as mrc:
                    self.data = mrc.data
                    self.xsize = mrc.header.nx
                    self.ysize = mrc.header.ny
                    self.zsize = mrc.header.nz
                    self.xapix, self.yapix, self.zapix = mrc.voxel_size.tolist()
        except:
            sys.exit('%s is not a valid file' % str(name))

    def writeMRC(self ,name ,dt='float32' ,overwrite=True):
        # save the data object to the mrc file
        # overwrite - create new file or overwrite existing file

        if os.path.isfile(name):
            if overwrite:
                os.remove(name)
            else:
                name = common.timestamp() + '_' + name

        try:
            with mrcfile.new(name) as mrc:
                mrc.header.nx = self.xsize
                mrc.header.ny = self.ysize
                mrc.header.nz = self.zsize
                mrc.voxel_size = (self.xapix ,self.yapix ,self.zapix)
                mrc.set_data(np.array(self.data ,dtype=getattr(np, dt)))
        except:
            sys.exit('could not save to %s' % name)


    def readTIF(self ,name ,key=''):
        # read data from tif file to numpy array
        # key - allows to specify which sub-part of the image should be read
        # currently considers only 2D and 3D data

        try:
            tif = tifffile.imread(name)
            self.xsize = tif.shape[0]
            self.ysize = tif.shape[1]
            if len(tif.shape) == 3:
                self.zsize = tif.shape[2]
            self.data = tif
        except:
            sys.exit('Cannot read TIF file %s' % name)
    # TO DO - add compression
    def writeTIF(self ,name ,dt='uint8' ,color='minisblack' ,overwrite=True):

        if os.path.isfile(name):
            if overwrite:
                os.remove(name)
            else:
                name = common.timestamp() + '_' + name

        tifffile.imwrite(name ,self.data ,photometric=color ,dtype=dt)

    def readJPG(self ,name):

        with PILImage.open(name) as im:
            self.data = np.asarray(im)

    def writeJPG(self ,name ,overwrite=True):

        if os.path.isfile(name):
            if overwrite:
                os.remove(name)
            else:
                name = common.timestamp() + '_' + name

        im = PILImage.fromarray(np.uint8(self.data))
        im.save(name ,'JPEG' ,quality=9)


#     def writeSMV(self ,name ,configFile ,imageNumberInSeries=0):
#         try:
#             os.stat(configFile)
#         except:
#             sys.exit('%s - Configuration file for SMV format not found' % str(configFile))
#
#         try:
#             # pc = yamlIO({}) TODO
#             pc.readConfigFile(configFile)
#         except:
#             sys.exit \
#                 ('%s - Could not read .yaml configuration file for SMV format, maybe wrong format' % str(configFile))
#
#         if os.path.isfile(name):
#             os.remove(name)
#
#         if len(self.data.shape) == 2:
#             vals = self.data
#             dim = vals.shape[0]
#         else:
#             vals = self.data[imageNumberInSeries ,: ,:]
#             dim = vals.shape[1]
#
#         header ='''{
# HEADER_BYTES=512;
# DIM=2;
# BYTE_ORDER=little_endian;
# TYPE=unsigned_short;
# SIZE1=%d;
# SIZE2=%d;
# PIXEL_SIZE=%.3f;
# BIN=1x1;
# BIN_TYPE=HW;
# ADC=fast;
# CREV=1;
# BEAMLINE=CEMCOF;
# DETECTOR_SN=901;
# DATE=%s;
# TIME=%.3f;
# DISTANCE=%.4f;
# TWOTHETA=0.0;
# PHI=%.4f;
# OSC_START=%.4f;
# OSC_RANGE=%.4f;
# WAVELENGTH=%.6f;
# BEAM_CENTER_X=%.3f;
# BEAM_CENTER_Y=%.3f;
# DENZO_X_BEAM=0.0000;
# DENZO_Y_BEAM=0.0000;
# }''' % (int(dim) ,int(dim),
#         float(pc.properties.physicalPixelSize) ,str(datetime.fromisoformat(pc.properties.date ) +timedelta
#             (seconds=imageNumberInSerie s *float(pc.properties.frameTime))),
#         float(pc.properties.frameTime), float(dim) * float(pc.properties.physicalPixelSize) * float(pc.properties.magnifiedPixelSize) / float
#             (pc.properties.lam), float(pc.properties.initialTiltAngle ) +imageNumberInSerie s *float(pc.properties.frameTime ) *float
#             (pc.properties.tiltSpeed),
#         float(pc.properties.initialTiltAngle ) +imageNumberInSerie s *float(pc.properties.frameTime ) *float
#             (pc.properties.tiltSpeed),
#         float(pc.properties.frameTime ) *float(pc.properties.tiltSpeed), float(pc.properties.lam),
#         (float(dim) + 1) / 2.0, (float(dim) + 1) / 2.0)
#
#         with open(name ,'w') as smvFile:
#             smvFile.write(header)
#
#         vals_bytes = vals.tobytes()
#
#         with open(name ,'r+b') as smvFile:
#             smvFile.seek(512)
#             smvFile.write(vals_bytes)

    def readSMV(self ,inFile):

        try:
            f = open(inFile ,'rb').read(512).decode('utf-8').split('\n')
            dim = int([x.split('=')[1].rstrip(';') for x in f if 'SIZE1' in x][0])
            physPixel = float([x.split('=')[1].rstrip(';') for x in f if 'PIXEL_SIZE' in x][0])
            D = float([x.split('=')[1].rstrip(';') for x in f if 'DISTANCE' in x][0])
            lam = float([x.split('=')[1].rstrip(';') for x in f if 'WAVELENGTH' in x][0])
            apix = D* lam / (dim * physPixel)
            self.xsize = dim
            self.ysize = dim
            self.xapix = apix
            self.yapix = apix

        except:
            sys.exit('%s - Could not read metadata from SMV file' % str(inFile))

        try:
            self.data = np.fromfile(inFile, dtype='uint16', offset=512).reshape(dim, dim)
        except:
            sys.exit('%s - Could not read data from SMV file' % str(inFile))

