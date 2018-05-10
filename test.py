from dataUlt import *
from HdfUtility import *
from FutureTickData import * 
from HisDayData import *

a = HisDayData()
a.getData()

a = HisFutureTick('F:\\data_hft','C:\\Users\\user\\GitHub\\Project1\\out.hdf5','CFE')
a.packedTick2Bar()

hdf=HdfUtility()
x = hdf.hdfRead('C:\\Users\\user\\GitHub\\Project1\\out.hdf5','CFE','IC','Rawdata',None,'60m')