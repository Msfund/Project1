import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import talib
import ffn
import json
from HdfUtility import HdfUtility
from dataUlt import *
from statsmodels.tsa.stattools import adfuller

# 读入数据
hdf = HdfUtility()
data = hdf.hdfRead(EXT_Hdf_Path,'CFE','IC','Stitch','00','1d',startdate='20110101',enddate='20171231')

# 因子的参数文件
with open('Indicator_setting.json','r') as f:
    indparams = json.load(f)

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

def Ind_Stability(data,excode,Asset):
    '''
    Unit Root Test
    The null hypothesis of the Augmented Dickey-Fuller is that there is a unit
    root, with the alternative that there is no unit root. That is to say the
    bigger the p-value the more reason we assert that there is a unit root
    使用单位根检验
    ts只能输入一列
    输出值有 t统计量 p值 滞后阶数 观测值数量 1% 5% 10%分位点数 icbest
    '''
    dfOutputAll = pd.DataFrame([])
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
    dfOutputAll.to_csv(excode+'_'+Asset+'StabilityTest.csv')
    return dfOutputAll

def Ind_Eff(data,excode,Asset,mode = 'prod'):
    #mode为'sum'时累积收益率为累加
    #mode为'prod'时累积收益率为1+收益率的累乘
    #TimeSeries结构为：MultiIndex为时间和资产；有两列数据 ret 和对应的Ind
    temp = data.reset_index().dropna()
    if mode =='prod':
        temp['ret'] = (1+temp['ret']).cumprod()
    if mode =='sum':
        temp['ret'] = (temp['ret']).cumsum()
    figsize = 10,15
    f = plt.figure(dpi = 100,figsize = figsize)
    k = (len(data.columns)-1) #排除收益率这一列，还有k个Indicator
    j = 0
    for i in data.columns:
        if 'ret' not in i:
            TimeSeries = temp[[EXT_Bar_Date,'ret',i]]
            TimeSeries = TimeSeries.sort_values(by = i, ascending = 1)
            df = TimeSeries[[i,'ret']].set_index([i])
            ##因子排序和收益率的
            j = j+1
            ax = plt.subplot(k,2,j)
            ax.plot(df)
            ax.set_xlabel(i,fontsize = 10)
            ax.tick_params(labelsize=8)
            ax.set_title('Cumulative return orderd by Ind_'+i)
            xlabels = ax.get_xticklabels()
            #ax.suptitle()
            for xl in xlabels:
                xl.set_rotation(30) #把x轴上的label旋转15度,以免太密集时有重叠
            ax.set_ylabel('cum_ret',fontsize = 10)
            #现在只画前两个图
            ##因子的时间序列
            ts = TimeSeries[[i,EXT_Bar_Date]]
            ts = ts.sort_values(by = EXT_Bar_Date).set_index([EXT_Bar_Date])
            j = j+1
            ax = plt.subplot(k,2,j)
            ax.plot(ts)
            ax.set_xlabel('Date',fontsize = 10)
            ax.tick_params(labelsize=8)
            ax.set_title('TimeSeries of Ind_'+i)
            xlabels = ax.get_xticklabels()
            for xl in xlabels:
                xl.set_rotation(30) #把x轴上的label旋转15度,以免太密集时有重叠
            ax.set_ylabel(i,fontsize = 10)
    plt.suptitle(Asset+'  Plot',fontsize=16,x=0.52,y=1.03)#储存入pdf后不能正常显示
    f.tight_layout()
    with PdfPages(excode+'_'+Asset+'_Plot.pdf') as pdf:
        pdf.savefig()
        plt.close()
    return
if __name__ =='__main__':
    asset_list = {}
    asset_list[EXT_EXCHANGE_CFE] = EXT_CFE_ALL
    asset_list[EXT_EXCHANGE_SHFE] = EXT_SHFE_ALL
    asset_list[EXT_EXCHANGE_DCE] = EXT_DCE_ALL
    # asset_list[EXT_EXCHANGE_CZCE] = EXT_CZCE_ALL
    for excode,symbol in asset_list.items():
        for i in range(len(symbol)):
            print(symbol[i])
            df = hdf.hdfRead(EXT_Hdf_Path,excode,symbol[i],'Stitch','00','1d',startdate = '20120101',enddate = '20171231')
            df[EXT_Bar_Close] = df[EXT_Bar_Close] * df[EXT_AdjFactor]
            mode = 'prod'
            df = rsi_ind(df)
            df['ret'] = ffn.to_returns(data[EXT_Bar_Close])
            dfOutputAll = Ind_Stability(df,excode,symbol[i])
            Ind_Eff(data = df,excode = excode,Asset = symbol[i])
