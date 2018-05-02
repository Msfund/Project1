import numpy as np
import pandas as pd
from pandas.io.pytables import HDFStore
import re
from rawUlt import *
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
        store = HDFStore(path,mode = 'r+')
        if kind in ['00','01']:
            all_data = store['Stitch/'+excode+'/'+symbol+'/Rule/'+kind]
            data = all_data.ix[((all_data[EXT_Out_Date]>=startdate)&(all_data[EXT_Out_Date]<=enddate)),:]
        else:
            all_data = store['Stitch/'+excode+'/'+symbol+'/Period/'+kind]
            data = all_data.ix[((all_data[EXT_Out_Date]>=startdate)&(all_data[EXT_Out_Date]<=enddate)),:]
        store.close()
        return data
    # 写, kind为 '00'、'01'、'1d'
    def hdfWrite(self,path,excode,symbol,startdate,enddate,indata,kind):
        store = HDFStore(path,mode = 'a')
        if kind in ['00','01']:
            #store.append(['Stitch/'+excode+'/'+symbol+'/Rule/'+kind][0],indata,append = True)
            try:
            #如果该地址已存在已有数据，以下代码正常运行
                temp_rule = store['Stitch/'+excode+'/'+symbol+'/Rule/'+kind]
                temp_rule.sort_values(by = [EXT_Out_Date,EXT_Out_Asset])
                if temp_rule[EXT_Out_Date].iloc[-1] > startdate:
                    #如果[startdate,enddate]中已有对应于数据库中的数据
                    #找到数据重合位置,并提取该调整因子数值
                    temp_adj1 = indata.adj_factor.ix[indata[EXT_Out_Date] == temp_rule.temp_rule[EXT_Out_Date].iloc[-1],:]
                    temp_adj2 = temp_rule[EXT_Out_AdjFactor].iloc[-1]
                    temp_adj = temp_adj2/temp_adj1
                elif temp_rule[EXT_Out_Date].iloc[-1] < startdate:
                    #时间差大于4时，则有可能数据出现截断，每次录数据尽可能从每月1日开始
                    temp_adj = temp_rule[EXT_Out_AdjFactor].iloc[-1]
                else:
                    print('写入数据与现有数据可能出现不连贯性')
                    temp_adj = 1
                adddata = indata.ix[(temp_rule[EXT_Out_Date]>=temp_rule[EXT_Out_Date].iloc[-1] & temp_rule[EXT_Out_Date]<enddate),:]
                #保证调整因子的连贯性
                adddata.iloc[:,2] = adddata.iloc[:,2] * temp_adj
                store.append(['Stitch/'+excode+'/'+symbol+'/Rule/'+kind][0],adddata,append = True)
            except:
                #第一次创建
                store.append(['Stitch/'+excode+'/'+symbol+'/Rule/'+kind][0],indata,append = True)
        else:
            try:
                #如果该地址已存在已有数据，以下代码正常运行
                temp_rule = store['Stitch/'+excode+'/'+symbol+'/Period/'+kind]
                temp_rule.sort_values(by = [EXT_Out_Date,EXT_Out_Asset])
                adddata = indata.ix[(temp_rule[EXT_Out_Date]>=temp_rule[EXT_Out_Date].iloc[-1] & temp_rule[EXT_Out_Date]<enddate),:]
                #保证调整因子的连贯性
                adddata.iloc[:,2] = adddata.iloc[:,2] * temp_adj
                store.append(['Stitch/'+excode+'/'+symbol+'/Period/'+kind][0],adddata,append = True)
            except:
                #第一次创建
                store.append(['Stitch/'+excode+'/'+symbol+'/Period/'+kind][0],indata,append = True)
        store.close()
