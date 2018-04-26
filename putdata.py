import numpy as np
import pandas as pd
import h5py
import re
from getdata3 import *

'''
HDF
    /Stitch
        /CFE
            /IF
               /00
                  Attrs:
                    Index_columns = Date Asset
                    Matrix_columns = AdjFactor
                    From_date =
                    To_date =
                  Datasets:
                    Index
                    Matrix
               /1d
                  Attrs:
                    Index_columns = Date Asset
                    Matrix_columns = Open High Low Close
                    From_date =
                    To_date =
                  Datasets:
                    Index
                    Matrix

    /Indicator
'''
class HDFutility():

    def __init__(self,path,excode,vt,startdate,enddate):
        self.path = path
        self.excode = excode
        self.vt = vt
        self.startdate = startdate
        self.enddate = enddate
    # 读，kind为 '00'、'01'、'1d'
    def HDFread(self,kind='00'):
        f = h5py.File(self.path,'r')
        subg = f['Stitch/'+self.excode+'/'+self.vt+'/'+kind]
        index = subg['Index'][:].astype(np.dtype("str"))
        matrix = subg['Matrix'][:].astype(np.dtype("float32"))
        columns_name = np.hstack((subg.attrs['Index_columns'],subg.attrs['Matrix_columns']))
        data = pd.DataFrame(np.hstack((index,matrix)))
        data.columns = columns_name
        f.close()
        return data
    # 写, kind为 '00'、'01'、'1d'
    def HDFwrite(self,indata,kind='00'):
        f = h5py.File(self.path,'w')
        try:
            subg = f['Stitch/'+self.excode+'/'+self.vt+'/'+kind]
        except:
            subg = f.create_group('Stitch/'+self.excode+'/'+self.vt+'/'+kind)
        columns_name = indata.columns
        subg['Index'] = indata.ix[:,0:2].values.astype(np.dtype("S10"))
        subg['Matrix'] = indata.ix[:,2:].values
        subg.attrs['Index_columns'] = columns_name[0:2].astype(h5py.special_dtype(vlen=str))
        subg.attrs['Matrix_columns'] = columns_name[2:].astype(h5py.special_dtype(vlen=str))
        f.close()

    def HDFcombine(self):
        pass

if __name__  ==  '__main__':
    path = 'C:\\Users\\user\\GitHub\\Project1\\out.hdf5'
    # Data = HDFutility('CFE','IF','1d','20170101','20171231',path).HDFwrite(dom_data)
