import numpy as np
import pandas as pd
from pandas.io.pytables import HDFStore
import re
from rawUlt import *
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
    def dataInfo(self,path,excode,symbol,kind,startdate,enddate):
        # 本地有数据返回True
        # kind = 'raw' or 'rule'
        f = h5py.File(path,'a')
        if kind == 'raw':
            try:
                key = EXT_Rawdata+'/'+excode+'/'+symbol+'/'+EXT_Period+'/'+EXT_Period_1
                from_date = f[key].attrs['From_date']
                to_date = f[key].attrs['To_date']
                if from_date< pd.to_datetime(startdate) and pd.to_datetime(enddate)<to_date:
                    return True
                else:
                    return False
            except:
                return False
        elif kind == 'rule':
            try:
                key = EXT_Stitch+'/'+excode+'/'+symbol+'/'+EXT_Rule+'/'+EXT_Series_0
                from_date = f[key].attrs['From_date']
                to_date = f[key].attrs['To_date']
                if from_date< pd.to_datetime(startdate) and pd.to_datetime(enddate)<to_date:
                    return True
                else:
                    return False
            except:
                return False
        else:
            print("Wrong kind")
        f.close()



    def hdfRead(self,path,excode,symbol,startdate,enddate,kind1,kind2,kind3):
        # kind1为 'Rawdata'、'Stitch'、'Indicator'
        # kind2为 '00' '01'
        # kind3为 '1d' '60m' '30m' '15m' '5m' '1m'
        store = HDFStore(path,mode = 'r')
        if kind1 == EXT_Rawdata:
            key = kind1+'/'+excode+'/'+symbol+'/'+kind3
            data = store[key].ix[((store[key][EXT_Out_Date]>=pd.to_datetime(startdate))&(store[key][EXT_Out_Date]<=pd.to_datetime(enddate))),:]
        if kind1 == EXT_Stitch:
            key=kind1+'/'+excode+'/'+symbol+'/'+EXT_Rule+'/'+kind2 if kind3=None else kind1+'/'+excode+'/'+symbol+'/'+EXT_Period+'/'+kind3+'/'+kind2
            data = store[key].ix[((store[key][EXT_Out_Date]>=pd.to_datetime(startdate))&(store[key][EXT_Out_Date]<=pd.to_datetime(enddate))),:]
        # if kind1 == EXT_Indicator:
        store.close()
        return data

    def hdfWrite(self,path,excode,symbol,startdate,enddate,indata,kind1,kind2,kind3):
        # kind1为 'Rawdata'、'Stitch'、'Indicator'
        # kind2为 '00' '01'
        # kind3为 '1d' '60m' '30m' '15m' '5m' '1m'
        store = HDFStore(path,mode='a')
        if kind1 == EXT_Rawdata:
            key = kind1+'/'+excode+'/'+symbol+'/'+kind3
        if kind1 == EXT_Stitch:
            key=kind1+'/'+excode+'/'+symbol+'/'+EXT_Rule+'/'+kind2 if kind3=None else kind1+'/'+excode+'/'+symbol+'/'+EXT_Period+'/'+kind3+'/'+kind2
        # if kind1 == EXT_Indicator:
        try:
            #尝试是否存在该地址
            store[key]
        except:
            #不存在，则创建新的地址
            store[key] = indata
            store.close()
            f = h5py.File(path,'a')
            f[key].attrs['From_date'] = startdate
            f[key].attrs['To_date'] = enddate
            f.close()
        else:
            #已存在，则需要判断是否添加新数据
            ##读取已有数据的from_date和to_date值
            f = h5py.File(path,'a')
            fromdate = f[key].attrs['From_date']
            todate = f[key].attrs['To_date']
            adddata = indata[pd.concat([indata[EXT_Out_Date] < fromdate,indata[EXT_Out_Date] > todate],axis=1).any(axis=1)]
            if adddata.shape[0] == 0:
                ##如果已有数据，则不需要添加
                print("No data added")
            else:
                ##如果没有数据，则不需要添加
                if kind in [EXT_Series_0,EXT_Series_1]:
                    ###如果类型是新增00 or 01，则调整复权因子，保证数据的连贯性
                    if (adddata[EXT_Out_Date] < pd.to_datetime(fromdate)).shape[0] > 0:
                        #前增
                        adddata_befor = adddata.ix[adddata[EXT_Out_Date] < pd.to_datetime(fromdate)]
                        adddata_befor[EXT_Out_AdjFactor] = adddata_befor[EXT_Out_AdjFactor]/store[key][EXT_Out_AdjFactor].iloc[0]*adddata_befor[EXT_Out_AdjFactor].iloc[-1]
                        store.append(key,adddata_befor)
                    elif (adddata[EXT_Out_Date] < pd.to_datetime(todate)).shape[0] > 0:
                        #后增
                        adddata_after = adddata[EXT_Out_Date] < pd.to_datetime(todate)
                        adddata_after[EXT_Out_AdjFactor] = adddata_after[EXT_Out_AdjFactor]*store[key][EXT_Out_AdjFactor].iloc[-1]/adddata_after[EXT_Out_AdjFactor].iloc[0]
                        store.append(key,adddata_after)
                f[key].attrs['From_date'] = min(startdate,fromdate)
                f[key].attrs['To_date'] = max(enddate,todate)
                f.close()
            store.close()
