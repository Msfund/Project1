import numpy as np
import pandas as pd
import h5py
from getdata3 import *

class HDFutility():

    def __init__(self,path,excode,vt,startdate,enddate):
        self.path = path
        self.excode = excode
        self.vt = vt
        self.startdate = startdate
        self.enddate = enddate


    # 读StitchData peroid为频率，kind为'01'、'02'
    def HDFread(self,period,kind):
        f = h5py.File(self.path,'r')
        data = f[self.excode][self.vt][period][kind]
        f.close()
        return data

    def HDFwrite(self,content,period,kind):
        f = h5py.File(self.path,'w')
        f[self.excode][self.vt][period].create_dataset(kind,data=concent)
        f.close()

    def HDFcombine(self):
        pass

if __name__  ==  '__main__':
    try:
        f = h5py.File('C:\\Users\\user\\GitHub\\Project1\\out.hdf5','a')

        # 一级目录
        f.create_group('RawData')
        f.create_group('Stitch')
        f.create_group('Indicator')
        # 二级目录 Excode
        f['Stitch'].create_group('CFE')
        f['Stitch'].create_group('SHF')
        f['Stitch'].create_group('DCE')
        f['Stitch'].create_group('CZC')
        # 三级目录 AssetCode
        f['Stitch']['CFE'].create_group('IF')
        # 四级目录 period
        f['Stitch']['CFE']['IF'].create_group('1d')
        # 五级目录 kind
        f['Stitch']['CFE']['IF']['1d'].create_group('00')
        
        f.close()
        f["Stitch/CFE/IF/1d/00"].keys()
        f = h5py.File(path,'w')
        f["Stitch/CFE/IF/1d/00"].create_dataset('Close',data=dom_data['S_DQ_CLOSE'].values)

        f.close()
    except:
        print('File Aready Exist')
    else:
        print('Successfully Created')
