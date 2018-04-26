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
    def HDFread(self,kind):
        with h5py.File(self.path,'r') as f:
            subg = f['Stitch/'+self.excode+'/'+self.vt+'/'+kind]
            rawindex = subg['Index'][:].astype(np.dtype("str"))
            rawmatrix = subg['Matrix'][:]
            index = rawindex[np.vstack((rawindex[:,0]>self.startdate,rawindex[:,0]<self.enddate)).all(axis=0)]
            matrix = rawmatrix[np.vstack((rawindex[:,0]>self.startdate,rawindex[:,0]<self.enddate)).all(axis=0)]
            columns_name = np.hstack((subg.attrs['Index_columns'],subg.attrs['Matrix_columns']))
            data = pd.DataFrame(np.hstack((index,matrix)))
            data.columns = columns_name
        return data
    # 写, kind为 '00'、'01'、'1d'
    def HDFwrite(self,indata,kind):
        with h5py.File(self.path,'a') as f:
            try:
                subg = f['Stitch/'+self.excode+'/'+self.vt+'/'+kind]
                fromdate = subg.attrs['From_date']
                todate = subg.attrs['To_date']
                adddata = indata[pd.concat([indata.TRADE_DT < fromdate,indata.TRADE_DT > todate],axis=1).any(axis=1)]
                if adddata.shape[0] == 0:
                    print("No data added")
                else:
                    subg['Index'].resize((subg['Index'].shape[0]+adddata.shape[0], 2))
                    subg['Index'][-adddata.shape[0]:,:] = adddata.ix[:,0:2].values.astype(np.dtype("S10"))
                    subg['Matrix'].resize((subg['Index'].shape[0]+adddata.shape[0], 20))
                    subg['Matrix'][-adddata.shape[0]:,:] = adddat.ix[:,2:].valuess
                    subg.attrs['From_date'] = min(self.startdate,fromdate)
                    subg.attrs['To_date'] = max(self.enddate,todate)
            except:
                subg = f.create_group('Stitch/'+self.excode+'/'+self.vt+'/'+kind)
                columns_name = indata.columns
                subg.create_dataset('Index',data=indata.ix[:,0:2].values.astype(np.dtype("S10")), maxshape=(None, 2))
                subg.create_dataset('Matrix',data=indata.ix[:,2:].values, maxshape=(None, 20))
                subg.attrs['Index_columns'] = columns_name[0:2].astype(h5py.special_dtype(vlen=str))
                subg.attrs['Matrix_columns'] = columns_name[2:].astype(h5py.special_dtype(vlen=str))
                subg.attrs['From_date'] = self.startdate
                subg.attrs['To_date'] = self.enddate

    def HDFcombine(self):
        pass

if __name__  ==  '__main__':
    pass
