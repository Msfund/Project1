import numpy as np
import pandas as pd
import h5py
from getdata3 import *

'''
HDF
    /Raw
        /1d
            Attrs:
                Index_columns = Date Asset
                Matrix_columns = AdjFactor Open High Low Close
                From_date =
                To_date =
            Datasets:
                Index
                RawData
    /Stitch
        /CFE
            /IF
               /1d
                  /00
                      Attrs:
                        Index_columns = Date Asset
                        Matrix_columns = AdjFactor Open High Low Close
                        From_date =
                        To_date =
                      Datasets:
                        Index
                        StitchData
                  /01
    /Indicator
'''
class HDFutility():

    def __init__(self,path,excode,vt,period,startdate,enddate):
        self.path = path
        self.excode = excode
        self.vt = vt
        self.period = period
        self.startdate = startdate
        self.enddate = enddate
    # 读StitchData peroid为频率，kind为'raw','00'、'01'
    def HDFread(self,period='1d',kind='raw'):
        f = h5py.File(self.path,'r')
        if kind == 'raw':
            index  = f['Raw/'+period+'/Index']
            matrix = f['Raw/'+period+'/Rawdata']
            columns_name = f['Raw/'+period].Attrs('Index_columns') + f['Raw/'+period].Attrs('Matrix_columns')
            data = pd.DataFrame(np.hstack(index,matrix))
            data.columns = columns_name
            data = data[data.date > self.startdate & data.date < self.startdate & self.vt == data.asset]
        else:
            index = f['Stitch/'+self.excode+'/'+self.vt+'/'+period+'/'+kind+'/Index']
            matrix = f['Stitch/'+self.excode+'/'+self.vt+'/'+period+'/'+kind+'/StitchData']
            columns_name = f['Stitch/'+self.excode+'/'+self.vt+'/'+period+'/'+kind].Attrs('Index_columns') + f['Stitch/'+self.excode+'/'+self.vt+'/'+period+'/'+kind].Attrs('Matrix_columns')
            data = pd.DataFrame(np.hstack(index,matrix))
            data.columns = columns_name
            data = data[data.date > self.startdate & data.date < self.startdate & self.vt == data.asset]
        f.close()
        return data
    # 写StitchData peroid为频率，kind为'raw','00'、'01'
    def HDFwrite(self,indata,period='1d',kind='raw'):
        f = h5py.File(self.path,'w')
        if kind == 'raw':
            f['Raw/'+period+'/Index'] = dom_data[['TRADE_DT','S_INFO_WINDCODE']].values.astype(np.dtype("S10"))
            f['Raw/'+period+'/Rawdata'] = dom_data[['S_DQ_OPEN','S_DQ_HIGH','S_DQ_LOW','S_DQ_CLOSE']].values
            f['Raw/'+period].attrs.create('Index_columns', ["Date", "Asset"], dtype=h5py.special_dtype(vlen=str))
            f['Raw/'+period].attrs.create('RawData_columns', ["Open", "High", "Low", "Close"], dtype=h5py.special_dtype(vlen=str))
        else:
            f['Stitch/'+self.excode+'/'+self.vt+'/'+period+'/'+kind+'/Index'] = dom_data[['TRADE_DT','S_INFO_WINDCODE']].values.astype(np.dtype("S10"))
            f['Stitch/'+self.excode+'/'+self.vt+'/'+period+'/'+kind+'/StitchData'] = dom_data[['S_DQ_OPEN','S_DQ_HIGH','S_DQ_LOW','S_DQ_CLOSE']].values
            f['Stitch/'+self.excode+'/'+self.vt+'/'+period+'/'+kind].attrs.create('Index_columns', ["Date", "Asset"], dtype=h5py.special_dtype(vlen=str))
            f['Stitch/'+self.excode+'/'+self.vt+'/'+period+'/'+kind].attrs.create('StitchData_columns', ["Open", "High", "Low", "Close"], dtype=h5py.special_dtype(vlen=str))
        f.close()

    def HDFcombine(self):
        pass

if __name__  ==  '__main__':
    path = 'C:\\Users\\user\\GitHub\\Project1\\out.hdf5'
    Data = HDFutility('CFE','IF','1d','20170101','20171231',path).HDFwrite(dom_data)
    try:
        f = h5py.File('C:\\Users\\user\\GitHub\\Project1\\out.hdf5','a')
        f.create_group('Raw')
        f.create_group('Stitch/CFE/IF/1d/00')
        f.create_group('Stitch/CFE/IF/1d/01')
        f.close()
        f = h5py.File(path,'a')
        len(f)
        f['Raw/Index'] = dom_data[['TRADE_DT','S_INFO_WINDCODE']].values.astype(np.dtype("S10"))
        f['Raw/Rawdata'] = dom_data[['S_DQ_OPEN','S_DQ_HIGH','S_DQ_LOW','S_DQ_CLOSE']].values
    except:
        print('Creation Failed')
    else:
        print('Successfully Created')
