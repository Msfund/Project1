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

    # 读
    # kind1为 '00'、'01'、'1d'
    # 当kind1 为 '1d' '1m' 即在Stitch-Period下面，kind2为00 或 01
    # datatype为 'RawData' 'Stitch' 'Indicator'
    # loca为'Rule' 'Period'
    def hdfRead(self,path,excode,symbol,startdate,enddate,kind1,loca,datatype,kind2 = None):
        store = HDFStore(path,mode = 'r')
        if kind2 == None:
            #例如Stitch-Rule-00或者RawData-Period-1d执行下面语句
            key = datatype+'/'+excode+'/'+symbol+'/'+loca+'/'+kind1
            data = store[key].ix[((store[key][EXT_Out_Date]>=pd.to_datetime(startdate))&(store[key][EXT_Out_Date]<=pd.to_datetime(enddate))),:]
        else:
            #例如Stitch-Period-1d-00执行下面语句
            try:
                #尝试是否有缝合后的数据
                key = datatype+'/'+excode+'/'+symbol+'/'+loca+'/'+kind1+'/'+kind2
                data = store[key].ix[((store[key][EXT_Out_Date]>=pd.to_datetime(startdate))&(store[key][EXT_Out_Date]<=pd.to_datetime(enddate))),:]
                if data.shape[0] == 0:
                    #Stitch--Period-1d-00 or 01可能会出现创建了该地址，但没有任何数据,则data返回False
                    data = False
            except:
                #无该地址，即无缝合数据，返回空值
                data = False
        #key = 'Stitch/'+excode+'/'+symbol+'/Rule/'+kind if kind in [EXT_Series_0,EXT_Series_1] else 'Stitch/'+excode+'/'+symbol+'/Period/'+kind
        #data = store[key].ix[((store[key][EXT_Out_Date]>=pd.to_datetime(startdate))&(store[key][EXT_Out_Date]<=pd.to_datetime(enddate))),:]
        store.close()
        return data
    # 写
    # kind为 '00'、'01'、'1d'
    # datatype为 'RawData' 'Stitch' 'Indicator'
    # kind和datatype默认为空值时
    # kind2为Stitch-Period-1d-00备用参数
    def hdfWrite(self,path,excode,symbol,startdate,enddate,indata,kind,loca,datatype,kind2 = None):
        store = HDFStore(path,mode='a')
        ###########################################
        #以下是新增部分
        if kind2 == None:
            #例如Stitch-Rule-00执行下面语句
            key = datatype+'/'+excode+'/'+symbol+'/'+loca+'/'+kind
        else:
            key = datatype+'/'+excode+'/'+symbol+'/'+loca+'/'+kind+'/'+kind2
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
