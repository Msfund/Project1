# -*- coding: utf-8 -*-
"""
Created on Thu May 24 11:56:59 2018

@author: Mimi
"""
import dataUlt
from statsmodels.tsa.stattools import adfuller
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
import matplotlib.pyplot as plt

class Indicator_testing(self,data):
    #Ind_name为指标名称
    TimeSeries = data['Ind_name'].copy()
    def __init__(self):
        self.Ind_name = Ind_name
        self.Window_period = 2
        self.Hold_period = 2
# =============================================================================
#     def Ind_Std(self,data):
#         mean = data[self.Ind_name].mean()
#         std = data[self.Ind_name].std()
#         data[self.Ind_name] = (data[self.Ind_name]-mean)/std
#         return data
# =============================================================================
    def Ind_Stability(self, ts):
        '''
        Unit Root Test
        The null hypothesis of the Augmented Dickey-Fuller is that there is a unit
        root, with the alternative that there is no unit root. That is to say the
        bigger the p-value the more reason we assert that there is a unit root
        使用单位根检验
        TimeSeries索引为时间，只有一列Indicator数据
        输出值有 t统计量 p值 滞后阶数 观测值数量 1% 5% 10%分位点数 
        '''
        dftest = adfuller(ts)
        # 对上述函数输出值进行语义描述
        dfoutput = pd.Series(dftest[0:4], index = ['t_Statistic','p_value','lags_used','Obs_used'])
        for key,value in dftest[4].items():
            dfoutput['Critical Value (%s)' %key] = value
        return dfoutput
    def Ind_Eff(self,TimeSeries,mode = 'sum'):
        #mode为'sum'时累积收益率为累加
        #mode为'prod'时累积收益率为1+收益率的累乘
        #TimeSeries结构为：index为时间；有两列数据 ret 和对应的Ind
        ##因子排序和收益率的
        TimeSeries = TimeSeries.reset_index()
        TimeSeries = TimeSeries.sort_values(by = 'Ind', ascending = 1)
        df = TimeSeries[['Ind','ret']].set_index(['Ind'])
        df['ret'] = (1+df['ret']).cumprod()
        plt.figure()
        df.plot()
        
        ##因子的时间序列
        ts = TimeSeries[['Ind','index']]
        ts = ts.sort_values(by = 'index').set_index(['index'])
        dfoutput = self.Ind_Stability(ts)
        ts.plot()   
        return dfoutput

