# -*- coding: utf-8 -*-
"""
Created on Mon May 28 14:30:32 2018

@author: Mimi
"""
from dataUlt import *
from HdfUtility import *
from FutureTickData import *
from HisDayData import *
import talib

asset_list = {}
asset_list[EXT_EXCHANGE_CFE] = EXT_CFE_ALL
asset_list[EXT_EXCHANGE_SHFE] = EXT_SHFE_ALL
asset_list[EXT_EXCHANGE_DCE] = EXT_DCE_ALL
# asset_list[EXT_EXCHANGE_CZCE] = EXT_CZCE_ALL
hdf = HdfUtility()
compari_all = {}
compari_all[EXT_EXCHANGE_CFE] = {}
compari_all[EXT_EXCHANGE_SHFE] = {}
compari_all[EXT_EXCHANGE_DCE] = {}
for excode,symbol in asset_list.items():
    for i in range(len(symbol)):
        rawdata = hdf.hdfRead(EXT_Hdf_Path,excode,symbol[i],'Rawdata',None,'1d',startdate=EXT_Start,enddate=EXT_End)
        #rawdata = x.copy()
        rawdata = rawdata.reset_index().sort_values(by = EXT_Out_Date, ascending = 1)
        rawdata = rawdata.groupby(EXT_Out_Date).sum()
        rawdata[EXT_Bar_Volume] = talib.MA(rawdata[EXT_Bar_Volume].values,timeperiod=20)
        compari_all[symbol[i]] = rawdata[EXT_Bar_Volume].mean()

def sort_by_value(d): 
    items=d.items() 
    backitems=[[v[1],v[0]] for v in items] 
    backitems.sort() 
    return [ backitems[i][1] for i in range(0,len(backitems))]

active_asset = list(reversed(sort_by_value(compari_all))) #交易量由大到小排列


def find_active_asset():
    compari_all = {}
    for excode,symbol in asset_list.items():
        for i in range(len(symbol)):
            rawdata = hdf.hdfRead(EXT_Hdf_Path,excode,symbol[i],'Rawdata',None,'1d',startdate=EXT_Start,enddate=EXT_End)
            #rawdata = x.copy()
            rawdata = rawdata.reset_index().sort_values(by = EXT_Out_Date, ascending = 1)
            rawdata = rawdata.groupby(EXT_Out_Date).sum()
            rawdata[EXT_Bar_Volume] = talib.MA(rawdata[EXT_Bar_Volume].values,timeperiod=20)
            compari_all[symbol[i]] = rawdata[EXT_Bar_Volume].mean()
    # sort_by_value
    items=compari_all.items() 
    backitems=[[v[1],v[0]] for v in items] 
    backitems.sort() 
    # 交易量由大到小排列
    return list(reversed([ backitems[i][1] for i in range(0,len(backitems))]))
