import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import talib
import ffn
from HdfUtility import HdfUtility
from dataUlt import *
from statsmodels.tsa.stattools import adfuller

# 读入数据
hdf = HdfUtility()
data = hdf.hdfRead(EXT_Hdf_Path,'CFE','IC','Stitch','00','1d',startdate='20110101',enddate='20171231')

# 因子的参数文件
indparams = dict([('ma',dict([('period',[5,10,20])])),('rsi',dict([('period',[5,10,20])]))])

def ma_ind(data):
    for i in indparams['ma']['period']:
        data['ma'+str(i)] = talib.MA(data[EXT_Bar_Close]*data[EXT_AdjFactor].values,timeperiod=i)
    indname = ["'ma"+str(i)+"'," for i in indparams['ma']['period']]
    return eval("data[["+''.join(indname)+"]]")
def rsi_ind(data):
    for i in indparams['rsi']['period']:
        data['rsi'+str(i)] = talib.RSI(data[EXT_Bar_Close]*data[EXT_AdjFactor].values,timeperiod=i)
    indname = ["'rsi"+str(i)+"'," for i in indparams['rsi']['period']]
    return eval("data[["+''.join(indname)+"]]")

def Ind_Stability(data,mode='prod'):
    '''
    Unit Root Test
    The null hypothesis of the Augmented Dickey-Fuller is that there is a unit
    root, with the alternative that there is no unit root. That is to say the
    bigger the p-value the more reason we assert that there is a unit root
    使用单位根检验
    ts只能输入一列
    输出值有 t统计量 p值 滞后阶数 观测值数量 1% 5% 10%分位点数 icbest
    '''
    dfOutputAll = {}
    temp = data.reset_index().dropna()
    if mode =='prod':
        temp['ret'] = (1+temp['ret']).cumprod()
    elif mode =='sum':
        temp['ret'] = (temp['ret']).cumsum()
    for i in data.columns:
        if 'ret' not in i:
            dftest = adfuller(temp[i])
            # 对上述函数输出值进行语义描述
            dfoutput = pd.Series(dftest[0:4], index = ['t_Statistic','p_value','lags_used','Obs_used'])
            for key,value in dftest[4].items():
                dfoutput['Critical Value (%s)' %key] = value
            dfoutput['icbest'] = dftest[5]
            dfOutputAll[i] = dfoutput
    return dfOutputAll
def Ind_Eff(data,mode = 'prod'):
    #mode为'sum'时累积收益率为累加
    #mode为'prod'时累积收益率为1+收益率的累乘
    #TimeSeries结构为：MultiIndex为时间和资产；有两列数据 ret 和对应的Ind
    temp = data.reset_index().dropna()
    if mode =='prod':
        temp['ret'] = (1+temp['ret']).cumprod()
    if mode =='sum':
        temp['ret'] = (temp['ret']).cumsum()
    plt.figure()
    k = (len(data.columns)-1) #排除收益率这一列，还有k个Indicator
    j = 0
    for i in data.columns:
        if 'ret' not in i:
            TimeSeries = temp[[EXT_Bar_Date,'ret',i]]
            TimeSeries = TimeSeries.sort_values(by = i, ascending = 1)
            df = TimeSeries[[i,'ret']].set_index([i])
            ##因子排序和收益率的
            j = j+1
            plt.subplot(k,2,j)
            plt.plot(df)
            #现在只画前两个图
            ##因子的时间序列
            ts = TimeSeries[[i,EXT_Bar_Date]]
            ts = ts.sort_values(by = EXT_Bar_Date).set_index([EXT_Bar_Date])
            j = j+1
            plt.subplot(k,2,j)
            plt.plot(ts)
    plt.show()
    return

if __name__ == '__main__':  
    # 计算因子
    df = rsi_ind(data)
    df['ret'] = ffn.to_returns(data['Close'])
    # 平稳性检验，有效性检验
    Ind_Stability(df)
    Ind_Eff(df)
