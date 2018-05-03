import numpy as np
import pandas as pd
from pandas.io.pytables import HDFStore
import re
from rawUlt import *
import h5py

# pd.set_option('io.hdf.default_format','table')
'''
HDF
    /Stitch
        /CFE
            /IF
               /Rule
                  /00
                  /01
               /Period
                  /1m
                  /5m
                  /15m
                  /60m
                  /1d

    /Indicator
'''
class HdfUtility:
    # 读，kind为 '00'、'01'、'1d'
    def hdfRead(self,path,excode,symbol,startdate,enddate,kind):
        store = HDFStore(path,mode = 'r')
        key = 'Stitch/'+excode+'/'+symbol+'/Rule/'+kind if kind in [EXT_Series_0,EXT_Series_1] else 'Stitch/'+excode+'/'+symbol+'/Period/'+kind
        data = store[key].ix[((store[key][EXT_Out_Date]>=startdate)&(store[key][EXT_Out_Date]<=enddate)),:]
        store.close()
        return data
    # 写, kind为 '00'、'01'、'1d'
    def hdfWrite(self,path,excode,symbol,startdate,enddate,indata,kind):
        store = HDFStore(path,mode='a')
        key = 'Stitch/'+excode+'/'+symbol+'/Rule/'+kind if kind in [EXT_Series_0,EXT_Series_1] else 'Stitch/'+excode+'/'+symbol+'/Period/'+kind
        try:
            store[key]
        except KeyError:
            store[key] = indata
            store.close()
            f = h5py.File(path,'a')
            f[key].attrs['From_date'] = startdate
            f[key].attrs['To_date'] = enddate
            f.close()
        else:
            f = h5py.File(path,'a')
            fromdate = f[key].attrs['From_date']
            todate = f[key].attrs['To_date']
            adddata = indata[pd.concat([indata[EXT_Out_Date] < fromdate,indata[EXT_Out_Date] > todate],axis=1).any(axis=1)]
            if adddata.shape[0] == 0:
                print("No data added")
            else:
                store.append(key,adddata)
                f[key].attrs['From_date'] = min(startdate,fromdate)
                f[key].attrs['To_date'] = max(enddate,todate)
                f.close()
        store.close()
