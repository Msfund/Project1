# -*- coding: utf-8 -*-
"""
Created on Mon Jun 11 15:58:49 2018

@author: Mimi
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import argparse
import datetime

import backtrader as bt

from HdfUtility import *
from dataUlt import *
import json
import pandas as pd
import ffn
from dataUlt import *
from statsmodels.tsa.stattools import adfuller
import matplotlib.pyplot as plt
import numpy as np
import talib
from HdfUtility import HdfUtility
from matplotlib.backends.backend_pdf import PdfPages


class TALibStrategy(bt.Strategy):

     params = (
                 ('ind', 'sma'),
                 ('doji', True)
             )
     with open('Indicator_setting.json','r') as f:
         Indicator_setting = json.load(f)
     INDS = Indicator_setting['INDS']
     
     def __init__(self):
        self.ind_data = {}
        with open('Indicator_setting.json','r') as f:
            Indicator_setting = json.load(f)
        if self.p.doji:
            self.ind_data['DOJI'] = bt.talib.CDLDOJI(self.data.open, self.data.high,
                             self.data.low, self.data.close)

        if 'SMA' in self.p.ind:
            for i in Indicator_setting['SMA']['timeperiod']:
                self.ind_data['SMA_'+str(i)] = bt.talib.SMA(self.data.close, timeperiod=i, plotname='TA_SMA')

        if 'EMA'  in self.p.ind:
            for i in Indicator_setting['EMA']['timeperiod']:
                self.ind_data['EMA_'+str(i)] = bt.talib.EMA(timeperiod=i, plotname='TA_SMA')

        if 'STOC' in self.p.ind:
            for i in Indicator_setting['STOCH']['fastk_period']:
                for j in Indicator_setting['STOCH']['slowk_period']:
                    for k in Indicator_setting['STOCH']['slowd_period']:
                        self.ind_data['STOCH_'+str(i)+'_'+str(j)+'_'+str(k)] = bt.talib.STOCH(self.data.high, self.data.low, self.data.close,
                                     fastk_period=i, slowk_period=j, slowd_period=k,
                                     plotname='TA_STOCH')


        if 'MACD' in self.p.ind:
            for i in Indicator_setting['MACD']['fastperiod']:
                for j in Indicator_setting['MACD']['slowperiod']:
                    for k in Indicator_setting['MACD']['signalperiod']:
                        if i<j:
                            self.ind_data['MACD_'+str(i)+'_'+str(j)+'_'+str(k)] = bt.talib.MACD(self.data,
                                          fastperiod=i, slowperiod=j, signalperiod=k, plotname='TA_MACD')

        if 'BBANDS' in self.p.ind:
            for i in Indicator_setting['BBANDS']['timeperiod']:
                self.ind_data['BBANDS_'+str(i)] = bt.talib.BBANDS(self.data, timeperiod=i,
                             plotname='TA_BBANDS')

        if 'RSI' in self.p.ind:
            for i in Indicator_setting['RSI']['timeperiod']:
                self.ind_data['RSI_'+str(i)] = bt.talib.RSI(self.data, timeperiod=i,
                             plotname='TA_RSI')

        if 'AROON'  in self.p.ind:
            for i in Indicator_setting['AROON']['timeperiod']:
                self.ind_data['AROON_'+str(i)] = bt.talib.AROON(self.data.high, self.data.low, timeperiod=i,
                             plotname='TA_AROON')

        if 'ULTOSC' in self.p.ind:
            for i in Indicator_setting['ULTOSC']['timeperiod1']:
                for j in Indicator_setting['ULTOSC']['timeperiod2']:
                    for k in Indicator_setting['ULTOSC']['timeperiod3']:
                        self.ind_data['ULTOSC_'+str(i)+'_'+str(j)+'_'+str(k)] = bt.talib.ULTOSC(self.data.high, self.data.low, 
                                      self.data.close, timeperiod1 = i, timeperiod2 = j, timeperiod3=k, plotname='TA_ULTOSC')

        if 'TRIX' in self.p.ind:
            for i in Indicator_setting['TRIX']['timeperiod']:
                self.ind_data['TRIX_'+str(i)] = bt.talib.TRIX(self.data, timeperiod=i,  plotname='TA_TRIX')


        if 'ADXR' in self.p.ind:
            for i in Indicator_setting['ADXR']['timeperiod']:
                self.ind_data['ADXR_'+str(i)] = bt.talib.ADXR(self.data.high, self.data.low,
                             self.data.close, timeperiod = i, plotname='TA_ADXR')


        if 'KAMA' in self.p.ind:
            for i in Indicator_setting['KAMA']['timeperiod']:
                self.ind_data['KAMA_'+str(i)] = bt.talib.KAMA(self.data, timeperiod=i, plotname='TA_KAMA')


        if 'DEMA' in self.p.ind:
            for i in Indicator_setting['DEMA']['timeperiod']:
                self.ind_data['DEMA_'+str(i)] = bt.talib.DEMA(self.data, timeperiod=i, plotname='TA_DEMA')


        if 'PPO' in self.p.ind:
            for i in Indicator_setting['PPO']['fastperiod']:
                for j in Indicator_setting['PPO']['slowperiod']:
                    self.ind_data['PPO_'+str(i)+'_'+str(j)] = bt.talib.PPO(self.data, fastperiod = i, slowperiod = j, plotname='TA_PPO')


        if 'TEMA' in self.p.ind:
            for i in Indicator_setting['TEMA']['timeperiod']:
                self.ind_data['TEMA_'+str(i)] = bt.talib.TEMA(self.data, timeperiod=i, plotname='TA_TEMA')


        if 'ROC' in self.p.ind:
            for i in Indicator_setting['ROC']['timeperiod']:
                self.ind_data['ROC_'+str(i)] = bt.talib.ROC(self.data, timeperiod=i, plotname='TA_ROC')
        if 'ROCP' in self.p.ind:
            for i in Indicator_setting['ROCP']['timeperiod']:
                self.ind_data['ROCP_'+str(i)] = bt.talib.ROCP(self.data, timeperiod=i, plotname='TA_ROCP')
        if 'ROCR' in self.p.ind:
            for i in Indicator_setting['ROCR']['timeperiod']:
                self.ind_data['ROCR_'+str(i)] = bt.talib.ROCR(self.data, timeperiod=i, plotname='TA_ROCR')
        if 'ROCR100' in self.p.ind:
            for i in Indicator_setting['ROCR100']['timeperiod']:
                self.ind_data['ROCR100_'+str(i)] = bt.talib.ROCR100(self.data, timeperiod=i, plotname='TA_ROCR100')

        if 'WILLR' in self.p.ind:
            for i in Indicator_setting['WILLR']['timeperiod']:
                self.ind_data['WILLR_'+str(i)] = bt.talib.WILLR(self.data.high, self.data.low, 
                              self.data.close, timeperiod = i,  plotname='TA_WILLR')
        if 'HT_TRENDLINE' in self.p.ind:   
            self.ind_data['HT_TRENDLINE'] =   bt.talib.HT_TRENDLINE(self.data.close,plotname = 'TA_HT_TRENDLINE')
        
        if 'MA' in self.p.ind:
            for i in Indicator_setting['MA']['timeperiod']:
                self.ind_data['MA_'+str(i)] = bt.talib.MA(self.data.close,timeperiod = i,
                              plotname = 'MA_'+str(i))
        
        if 'MAMA' in self.p.ind:
            self.ind_data['MAMA'] = bt.talib.MAMA(self.data.close,plotname = 'MAMA')
        
        if 'MIDPOINT' in self.p.ind:
            for i in Indicator_setting['MIDPOINT']['timeperiod']:
                self.ind_data['MIDPOINT_'+str(i)] = bt.talib.MIDPOINT(self.data.close,timeperiod = i,
                              plotname = 'MIDPOINT_'+str(i))
                
        if 'MIDPRICE' in self.p.ind:
            for i in Indicator_setting['MIDPRICE']['timeperiod']:
                self.ind_data['MIDPRICE_'+str(i)] = bt.talib.MIDPOINT(self.data.close,timeperiod = i,
                              plotname = 'MIDPRICE_'+str(i))
                
        if 'SAR' in self.p.ind:
            self.ind_data['SAR_'+str(i)] = bt.talib.SAR(self.data.high,self.data.low,plotname = 'SAR')
        
        if 'T3' in self.p.ind:
            for i in Indicator_setting['T3']['timeperiod']:
                self.ind_data['T3_'+str(i)] = bt.talib.T3(self.data.close,timeperiod = i,
                              plotname = 'T3_'+str(i))
        
        if 'TRIMA' in self.p.ind:
            for i in Indicator_setting['TRIMA']['timeperiod']:
                self.ind_data['TRIMA_'+str(i)] = bt.talib.TRIMA(self.data.close,timeperiod = i,
                              plotname = 'TRIMA_'+str(i))
                
        if 'WMA' in self.p.ind:
            for i in Indicator_setting['WMA']['timeperiod']:
                self.ind_data['WMA_'+str(i)] = bt.talib.WMA(self.data.close,timeperiod = i,
                              plotname = 'WMA_'+str(i))
                
        if 'ADX' in self.p.ind:
            for i in Indicator_setting['ADX']['timeperiod']:
                self.ind_data['ADX_'+str(i)] = bt.talib.ADX(self.data.high,self.data.low,
                              self.data.close,plotname = 'ADX_'+str(i))
        
        if 'APO' in self.p.ind:
            for i in Indicator_setting['APO']['fastperiod']:
                for j in Indicator_setting['APO']['slowperiod']:
                    self.ind_data['APO_'+str(i)+'_'+str(j)] = bt.talib.APO(self.data.close,
                                  fastperiod = i, slowperiod = j,plotname = 'APO_'+str(i)+'_'+str(j))
        
        if 'AROONOSC' in self.p.ind:
            for i in Indicator_setting['AROONOSC']['timeperiod']:
                self.ind_data['AROONOSC_'+str(i)] = bt.talib.AROONOSC(self.data.high,
                              self.data.low,timeperiod = i, plotname = 'AROONOSC_'+str(i))
        if 'BOP' in self.p.ind:
            self.ind_data['BOP'] = bt.talib.BOP(self.data.open,self.data.high,
                         self.data.low,self.data.close,plotname = 'BOP')
        if 'CCI' in self.p.ind:
            for i in Indicator_setting['CCI']['timeperiod']:
                self.ind_data['CCI_'+str(i)] = bt.talib.CCI(self.data.high,
                         self.data.low,self.data.close,plotname = 'CCI_'+str(i))
        
        if 'DX' in self.p.ind:
            for i in Indicator_setting['DX']['timeperiod']:
                self.ind_data['DX'] = bt.talib.DX(self.data.high,
                         self.data.low,self.data.close,plotname = 'DX_'+str(i))
        
        if 'MACDEXT' in self.p.ind:
            for i in Indicator_setting['MACDEXT']['fastperiod']:
                for j in Indicator_setting['MACDEXT']['slowperiod']:
                    for k in Indicator_setting['MACDEXT']['signalperiod']:
                        self.ind_data['MACDEXT_'+str(i)+'_'+str(j)+'_'+str(k)] = bt.talib.MACDEXT(self.data.close,
                                  fastperiod = i, slowperiod = j,signalperiod = k,plotname = 'MACDEXT_'+str(i)+'_'+str(j)+'_'+str(k))
                
        if 'MACDFIX' in self.p.ind:
            for i in Indicator_setting['MACDFIX']['signalperiod']:
                self.ind_data['MACDFIX_'+str(i)] = bt.talib.MACDFIX(self.data.close,plotname = 'MACDFIX_'+str(i))
                
        if 'MFI' in self.p.ind:
            for i in Indicator_setting['MFI']['timeperiod']:
                self.ind_data['MFI_'+str(i)] = bt.talib.MFI(self.data.high,
                         self.data.low,self.data.close,self.data.volume,timeperiod = i,plotname = 'MFI_'+str(i))
        
        if 'MINUS_DI' in self.p.ind:
            for i in Indicator_setting['MINUS_DI']['timeperiod']:
                self.ind_data['MINUS_DI_'+str(i)] = bt.talib.MINUS_DI(self.data.high,
                         self.data.low,self.data.close,timeperiod = i,plotname = 'MINUS_DI_'+str(i))
        
        if 'MINUS_DM' in self.p.ind:
            for i in Indicator_setting['MINUS_DM']['timeperiod']:
                self.ind_data['MINUS_DM_'+str(i)] = bt.talib.MINUS_DM(self.data.high,
                         self.data.low,timeperiod = i,plotname = 'MINUS_DM_'+str(i))

                
        if 'MOM' in self.p.ind:
            for i in Indicator_setting['MOM']['timeperiod']:
                self.ind_data['MOM_'+str(i)] = bt.talib.MOM(self.data.close,
                              timeperiod = i,plotname = 'MOM_'+str(i))
                
        if 'PLUS_DI' in self.p.ind:
            for i in Indicator_setting['PLUS_DI']['timeperiod']:
                self.ind_data['PLUS_DI_'+str(i)] = bt.talib.PLUS_DI(self.data.high,
                              self.data.low,self.data.close,timeperiod = i,
                              plotname = 'PLUS_DI_'+str(i))
        
        if 'PLUS_DM' in self.p.ind:
            for i in Indicator_setting['PLUS_DM']['timeperiod']:
                self.ind_data['PLUS_DM_'+str(i)] = bt.talib.PLUS_DM(self.data.high,
                         self.data.low,timeperiod = i, plotname = 'PLUS_DM_'+str(i))
        
        if 'AD' in self.p.ind:
            self.ind_data['AD'] = bt.talib.AD(self.data.high, self.data.low,
                         self.data.close,self.data.volume,plotname = 'AD')
                
        if 'ADOSC' in self.p.ind:
            for i in Indicator_setting['ADOSC']['fastperiod']:
                for j in Indicator_setting['ADOSC']['slowperiod']:
                    self.ind_data['ADOSC_'+str(i)+'_'+str(j)] = bt.talib.ADOSC(self.data.high, self.data.low,
                                 self.data.close,self.data.volume,fastperiod = i, slowperiod = j,
                                 plotname = 'ADOSC_'+str(i)+'_'+str(j))
        
        if 'OBV' in self.p.ind:
            self.ind_data['OBV'] = bt.talib.OBV(self.data.close,self.data.volume,plotname = 'OBV')
        
        if 'HT_DCPERIOD' in self.p.ind:
            self.ind_data['HT_DCPERIOD'] = bt.talib.HT_DCPERIOD(self.data.close,plotname = 'HT_DCPERIOD')
                
        if 'HT_DCPHASE' in self.p.ind:
            self.ind_data['HT_DCPHASE'] = bt.talib.HT_DCPHASE(self.data.close,plotname = 'HT_DCPHASE')
                
                
        if 'HT_PHASOR' in self.p.ind:
            self.ind_data['HT_PHASOR'] = bt.talib.HT_PHASOR(self.data.close,plotname = 'HT_PHASOR')
        
        if 'HT_SINE' in self.p.ind:
            self.ind_data['HT_SINE']= bt.talib.HT_SINE(self.data.close,plotname = 'HT_SINE')
        
        if 'AVGPRICE' in self.p.ind:
            self.ind_data['AVGPRICE'] = bt.talib.AVGPRICE(self.data.open,self.data.high,
                         self.data.low,self.data.close,plotname = 'AVGPRICE')
                
        if 'MEDPRICE' in self.p.ind:
            self.ind_data['MEDPRICE'] = bt.talib.MEDPRICE(self.data.high,
                         self.data.low,plotname = 'MEDPRICE')
                
                
        if 'TYPPRICE' in self.p.ind:
            self.ind_data['TYPPRICE'] = bt.talib.TYPPRICE(self.data.high,
                         self.data.low,self.data.close,plotname = 'TYPPRICE')
        
        if 'WCLPRICE' in self.p.ind:
            self.ind_data['WCLPRICE'] = bt.talib.WCLPRICE(self.data.high,
                         self.data.low,self.data.close,plotname = 'WCLPRICE')
                
        if 'ATR' in self.p.ind:
            for i in Indicator_setting['ATR']['timeperiod']:
                self.ind_data['ATR_'+str(i)] = bt.talib.ATR(self.data.high,
                         self.data.low,self.data.close,plotname = 'ATR_'+str(i))
                
                
        if 'NATR' in self.p.ind:
            for i in Indicator_setting['NATR']['timeperiod']:
                self.ind_data['NATR_'+str(i)] = bt.talib.NATR(self.data.high,
                         self.data.low,self.data.close,plotname = 'NATR_'+str(i))
        
        if 'TRANGE' in self.p.ind:
            self.ind_data['TRANGE'] = bt.talib.TRANGE(self.data.high,
                         self.data.low,self.data.close,plotname = 'TRANGE')
        
def hdf2bt(data):
    data = data.reset_index().set_index([EXT_Bar_Date])
    data[EXT_Bar_Close] = data[EXT_AdjFactor] * data[EXT_Bar_Close]
    data.drop([EXT_Out_Asset,EXT_AdjFactor,EXT_Bar_PreSettle,EXT_Bar_Settle],axis=1,inplace=True)
    return data

def runstrat(excode,asset,args=None):
    hdf = HdfUtility()
    args = parse_args(args)

    cerebro = bt.Cerebro()

    dkwargs = dict()
    if args.fromdate:
        fromdate = datetime.datetime.strptime(args.fromdate, '%Y-%m-%d')
        dkwargs['fromdate'] = fromdate
        start = ''.join(str(fromdate)[0:10].split('-'))
    if args.todate:
        todate = datetime.datetime.strptime(args.todate, '%Y-%m-%d')
        dkwargs['todate'] = todate
        end = ''.join(str(todate)[0:10].split('-'))
    
    data0 = hdf.hdfRead(EXT_Hdf_Path,excode,asset,'Stitch','00','1d',startdate=start,enddate=end)
    data0 = hdf2bt(data0)
    Indicator_All = pd.DataFrame([])
    Indicator_All[EXT_Bar_Close] = data0[EXT_Bar_Close]
    Indicator_All = Indicator_All.reset_index().copy()
    # Feed data
    data0 = bt.feeds.PandasData(dataname=data0.copy(),
                        fromdate = datetime.datetime(2012, 1, 1),
                        todate = datetime.datetime(2017, 12, 31)
                        )
    cerebro.adddata(data0)
    cerebro.addstrategy(TALibStrategy, ind=args.ind, doji=not args.no_doji, plot = args.plot)

    cerebro.run(runcone=not args.use_next, stdstats=False)
    stat = cerebro.runstrats[0][0]
    #print(dir(stat))
    #print(stat.INDS)
    indicator_byvt = stat.ind_data
    #print(dir(indicator_byvt))
    #params = setting['data_setting']
    
    for indi, indicator in indicator_byvt.items():
        Indicator_All[indi] = pd.DataFrame({indi:indicator.array})
    #args.plot = True
    if args.plot:
        pkwargs = dict(style='candle')
        if args.plot is not True:  # evals to True but is not True
            npkwargs = eval('dict(' + args.plot + ')')  # args were passed
            pkwargs.update(npkwargs)
        cerebro.plot(**pkwargs)
    Indicator_All['ret'] = ffn.to_returns(Indicator_All[EXT_Bar_Close])
    # set 'Date' as only index to test its Stability
    Indicator_All.set_index(Indicator_All[EXT_Bar_Date])
    Ind_Stability(Indicator_All,excode,asset)
    # curret indicator corresponding to next period of return
    Indicator_All['ret'] = Indicator_All['ret'].shift(-1)
    Ind_Eff(Indicator_All,excode,asset)
    return

def Ind_Stability(data,excode,Asset,mode = 'prod'):
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
    temp = data.reset_index()
    result_path = 'E:\\work\\test_backtra\\Stability'
    if mode =='prod':
        temp['ret'] = (1+temp['ret']).cumprod()
    elif mode =='sum':
        temp['ret'] = (temp['ret']).cumsum()
    for i in data.columns:
        if i not in ('Date','Close','ret'):
            dftest = adfuller(temp[i].dropna())
            # 对上述函数输出值进行语义描述
            dfoutput = pd.Series(dftest[0:4], index = ['t_Statistic','p_value','lags_used','Obs_used'])
            for key,value in dftest[4].items():
                dfoutput['Critical Value (%s)' %key] = value
            dfoutput['icbest'] = dftest[5]
            dfOutputAll[i] = dfoutput
    dfOutputAll.T.to_csv(result_path+'\\'+excode+'_'+Asset+'_StabilityTest.csv')
    return dfOutputAll.T

def Ind_Eff(data,excode,Asset, mode = 'prod'):
    #mode为'sum'时累积收益率为累加
    #mode为'prod'时累积收益率为1+收益率的累乘
    #TimeSeries结构为：MultiIndex为时间和资产；有两列数据 ret 和对应所有的Ind
    #最大每页行数为MaxPlotNum
    MaxPlotNum = 6
    temp = data.reset_index()
    j = 0
    figsize = 10,15
    f = plt.figure(figsize = figsize)
    result_path = 'E:\\work\\test_backtra\\Stability'
    with PdfPages(result_path+'\\'+excode+'_'+Asset+'_Plot.pdf') as pdf:
        for i in data.columns:
            if i not in ('Date','Close','ret'):
                ##因子排序和收益率的
                TimeSeries = temp[[EXT_Bar_Date,'ret',i]].dropna()
                TimeSeries = TimeSeries.sort_values(by = i, ascending = 1)
                if mode =='prod':
                    TimeSeries['ret'] = (1+TimeSeries['ret']).cumprod()
                elif mode =='sum':
                    TimeSeries['ret'] = (TimeSeries['ret']).cumsum()
                df = TimeSeries[[i,'ret']].set_index([i])
                j = j+1
                ax = plt.subplot(MaxPlotNum,2,j)
                ax.plot(df,marker='o', markersize = 1.3,linewidth = 1)
                ax.set_xlabel(i,fontsize = 10/6.0*MaxPlotNum)
                ax.tick_params(labelsize=8/6.0*MaxPlotNum)
                ax.set_title('Cum_return orderd by Ind_'+i)
                xlabels = ax.get_xticklabels()
                #ax.suptitle()
                for xl in xlabels:
                    xl.set_rotation(30) #把x轴上的label旋转30度,以免太密集时有重叠
                ax.set_ylabel('cum_ret',fontsize = 10/6.0*MaxPlotNum)
                #现在只画前两个图
                ##因子的时间序列
                ts = TimeSeries[[i,EXT_Bar_Date]].dropna()
                ts = ts.sort_values(by = EXT_Bar_Date).set_index([EXT_Bar_Date])
                j = j+1
                ax = plt.subplot(MaxPlotNum,2,j)
                ax.plot(ts,marker='o', markersize = 1.3,linewidth = 1)
                ax.set_xlabel('Date',fontsize = 10/6.0*MaxPlotNum)
                ax.tick_params(labelsize=8/6.0*MaxPlotNum)
                ax.set_title('TimeSeries of Ind_'+i)
                xlabels = ax.get_xticklabels()
                for xl in xlabels:
                    xl.set_rotation(30) #把x轴上的label旋转30度,以免太密集时有重叠
            f.tight_layout()
            if (j % (MaxPlotNum*2) == 0 and j !=0):
                f.tight_layout()
                #plt.subplots_adjust(top=1,bottom=0,left=0,right=1,hspace=0,wspace=0)
                plt.suptitle(Asset+'  Plot',fontsize=16/6.0*MaxPlotNum,x=0.52,y=1.03)#储存入pdf后不能正常显示
                pdf.savefig()
                plt.close()
                figsize = 10,15
                f = plt.figure(figsize = figsize)
                j = 0
            if i == data.columns[-1]:
                #plt.subplots_adjust(top=1,bottom=0,left=0,right=1,hspace=0,wspace=0)
                pdf.savefig()
                plt.close()
    return
def parse_args(pargs=None):

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Sample for sizer')

# =============================================================================
#     parser.add_argument('--data0', required=False,
#                         default='E:\\work\\vnpy-1.7\\getdata3\\out.hdf5',
#                         help='Data to be read in')
# =============================================================================

    parser.add_argument('--fromdate', required=False,
                        default='2012-01-01',
                        help='Starting date in YYYY-MM-DD format')

    parser.add_argument('--todate', required=False,
                        default='2017-12-31',
                        help='Ending date in YYYY-MM-DD format')

    parser.add_argument('--ind', required=False, action='store',
                        default=TALibStrategy.INDS[:22],
                        choices=TALibStrategy.INDS,
                        help=('Which indicator pair to show together'))

    parser.add_argument('--no-doji', required=False, action='store_true',
                        help=('Remove Doji CandleStick pattern checker'))

    parser.add_argument('--use-next', required=False, action='store_true',
                        help=('Use next (step by step) '
                              'instead of once (batch)'))
    #parser.add_argument('--')
    # Plot options
    parser.add_argument('--plot', '-p', nargs='?', required=False,
                        metavar='kwargs', const=True,
                        help=('Plot the read data applying any kwargs passed\n'
                              '\n'
                              'For example (escape the quotes if needed):\n'
                              '\n'
                              '  --plot style="candle" (to plot candles)\n'))
    if pargs is not None:
        return parser.parse_args(pargs)

    return parser.parse_args()


if __name__ == '__main__':
    runstrat(excode = 'CFE',asset = 'IF')
    
