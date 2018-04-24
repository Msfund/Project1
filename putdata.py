import numpy as np
import pandas as pd
import h5py
from getdata3 import *

class HDFutility():

    def __init__(self,path,excode,vt,period,startdate,enddate):
        self.path = path
        self.excode = excode
        self.vt = vt
        self.period = period
        self.startdate = startdate
        self.enddate = enddate


    # 读StitchData peroid为频率，kind为'raw','00'、'01'
    def HDFread(self,kind='raw'):
        f = h5py.File(self.path,'r')
        if kind = 'raw':
            date  = f['RawData/Date']
            asset = f['RawData/Asset']
            open  = f['RawData/Open']
            high  = f['RawData/High']
            low   = f['RawData/Low']
            close = f['RawData/Close']
            data = pd.DataFrame({'Date':date,'Asset':asset,'Open':open,'High':high,'Low':low,'Close':close})
            data = data[data.date > self.startdate & data.date < self.enddate]
        else:
            date  = f['Stitch'][self.excode][self.vt][kind][Date]
            asset = f['Stitch'][self.excode][self.vt][kind][Asset]
            open  = f['Stitch'][self.excode][self.vt][kind][Open]
            high  = f['Stitch'][self.excode][self.vt][kind][High]
            low   = f['Stitch'][self.excode][self.vt][kind][Low]
            close = f['Stitch'][self.excode][self.vt][kind][Close]
            adjfactor = f['Stitch'][self.excode][self.vt][kind][AdjFactor]
            data = pd.DataFrame({'Date':date,'Asset':asset,'Open':open,'High':high,'Low':low,'Close':close,'AdjFactor':adjfactor})
        f.close()
        return data
    # 写StitchData peroid为频率，kind为'raw','00'、'01'
    def HDFwrite(self,indata,kind='raw'):
        f = h5py.File(self.path,'w')
        if kind = 'raw':
            f['RawData/Date']  = indata.date.values
            f['RawData/Asset'] = indata.Asset.values
            f['RawData/Open']  = indata.open.values
            f['RawData/High']  = indata.high.values
            f['RawData/Low']   = indata.low.values
            f['RawData/Close'] = indata.close.values
        else:
            f['Stitch'][self.excode][self.vt][kind][Date] = indata.date.values
            f['Stitch'][self.excode][self.vt][kind][Asset] = indata.asset.values
            f['Stitch'][self.excode][self.vt][kind][Open] = indata.open.values
            f['Stitch'][self.excode][self.vt][kind][High] = indata.high.values
            f['Stitch'][self.excode][self.vt][kind][Low] = indata.low.values
            f['Stitch'][self.excode][self.vt][kind][Close] = indata.close.values
            f['Stitch'][self.excode][self.vt][kind][AdjFactor] = indata.adjfactor.values
        f.close()

    def HDFcombine(self):
        pass

if __name__  ==  '__main__':
    path = 'C:\\Users\\user\\GitHub\\Project1\\out.hdf5'
    Data = HDFutility('CFE','IF','1d','20170101','20171231',path).HDFread()
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
        print('Creation Failed')
    else:
        print('Successfully Created')
