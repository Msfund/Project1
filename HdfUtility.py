import numpy as np
import pandas as pd
from pandas.io.pytables import HDFStore
import re
from dataUlt import *
import h5py
pd.set_option('io.hdf.default_format','table')
'''
HDF
    /Rawdata
        /CFE
            /IC
                /1m
                /5m
                /30m
                /1h
                /1d
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

    def hdfRead(self,path,excode,symbol,kind1,kind2,kind3,startdate=EXT_Start,enddate=EXT_End):
        # kind1为 'Rawdata',Stitch','Indicator'
        # kind2为 '00' '01'
        # kind3为 '1d' '60m' '30m' '15m' '5m' '1m'
        # 读各个频率的Rawdata: kind1='Rawdata',kind2=None,kind3='1d'
        # 读StitchRule:       kind1='Stitch', kind2='00',kind3=None
        # 读STitchData:       kind1='Stitch', kind2='00',kind3='1d'
        store = HDFStore(path,mode = 'r')
        if kind1 == EXT_Rawdata:
            key = kind1+'/'+excode+'/'+symbol+'/'+kind3
            data = store[key].ix[((store[key][EXT_Out_Date]>=pd.to_datetime(startdate))&(store[key][EXT_Out_Date]<=pd.to_datetime(enddate))),:]
        if kind1 == EXT_Stitch:
            key = kind1+'/'+excode+'/'+symbol+'/'+EXT_Rule+'/'+kind2 if kind3 == None else kind1+'/'+excode+'/'+symbol+'/'+EXT_Period+'/'+kind3+'/'+kind2
            data = store[key].ix[((store[key][EXT_Out_Date]>=pd.to_datetime(startdate))&(store[key][EXT_Out_Date]<=pd.to_datetime(enddate))),:]
        store.close()
        return data

    def hdfWrite(self,path,excode,symbol,indata,kind1,kind2,kind3):
        # kind1为 'Rawdata'、'Stitch'、'Indicator'
        # kind2为 '00' '01'
        # kind3为 '1d' '60m' '30m' '15m' '5m' '1m'
        # 写各个频率的Rawdata: kind1='Rawdata',kind2=None,kind3='1d'
        # 写StitchRule:       kind1='Stitch', kind2='00',kind3=None
        # 写StitchData:       kind1='Stitch', kind2='00',kind3='1d'
        store = HDFStore(path,mode='a')
        if kind1 == EXT_Rawdata:
            key = kind1+'/'+excode+'/'+symbol+'/'+kind3
        if kind1 == EXT_Stitch:
            key=kind1+'/'+excode+'/'+symbol+'/'+EXT_Rule+'/'+kind2 if kind3 == None else kind1+'/'+excode+'/'+symbol+'/'+EXT_Period+'/'+kind3+'/'+kind2
        adddata = indata[~indata.index.isin(store[key].index)]
        store.append(key,indata)
        store.close()
