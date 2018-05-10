from dataUlt import *
from HdfUtility import *
from FutureTickData import * 
from HisDayData import *


# 从本地获取高频原始数据写入HDF
a = HisFutureTick('F:\\data_hft','C:\\Users\\user\\GitHub\\Project1\\out.hdf5','DCE')
a.packedTick2Bar(path_packedtick='DCE')
# 从万德获取日度交易数据写入HDF,计算StitchRule写入HDF，计算StitchData判断是否写入HDF
a = HisDayData()
a.getData(is_save_stitch=True)
# HDF读
hdf=HdfUtility()
# 读各个频率的Rawdata: kind1='Rawdata',kind2=None,kind3='1d'
x = hdf.hdfRead('C:\\Users\\user\\GitHub\\Project1\\out.hdf5','CFE','IC','Rawdata',None,'1d',startdate=EXT_Start,enddate=EXT_End)
# 读StitchRule:       kind1='Stitch', kind2='00',kind3=None
x = hdf.hdfRead('C:\\Users\\user\\GitHub\\Project1\\out.hdf5','CFE','IC','Stitch','00',None,startdate=EXT_Start,enddate=EXT_End)
# 读STitchData:       kind1='Stitch', kind2='00',kind3='1d'
x = hdf.hdfRead('C:\\Users\\user\\GitHub\\Project1\\out.hdf5','CFE','IC','Stitch','00','1d',startdate=EXT_Start,enddate=EXT_End)
