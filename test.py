from dataUlt import *
from HdfUtility import *
from FutureTickData import * 


hdf=HdfUtility()
x = hdf.hdfRead('C:\\Users\\user\\GitHub\\Project1\\out.hdf5','CFE','IC','Rawdata',None,'60m')