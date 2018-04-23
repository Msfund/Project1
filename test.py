import cx_Oracle
import pandas as pd
import numpy as np
import h5py
import re

CFEcode = ('IF','IC','IH','TF','T')
SHFcode = ('CU','AL','ZN','RU','FU','AU','AG','RB','WR','PB','BU','HC','NI','SN')
DCEcode = ('A','B','M','C','Y','P','L','V','J','JM','I','JD','FB','BB','PP','CS')
CZCcode = ('PM','WH','CF','SR','OI','TA','RI','LR','MA','FG','RS','RM','TC','ZC','JR','SF','SM')
Allcode = CFEcode + SHFcode + DCEcode + CZCcode
# CCommodityFuturesEODPrices：date vt preSettle open high low close volumn openinterest

vt = 'CU'
startdate = '20170101'
enddate = '20171231'
db = cx_Oracle.connect('fe','fe','192.168.100.22:1521/winddb')
cursor = db.cursor()

if vt in CFEcode:
    exchmarkt = 'filesync.CIndexFuturesEODPrices'
else:
    exchmarkt = 'filesync.CCommodityFuturesEODPrices'
if vt in CZCcode:
    sql = '''select s_info_windcode,trade_dt,
    s_dq_presettle,s_dq_open,s_dq_high,s_dq_low,s_dq_close,s_dq_volume,s_dq_oi from '''+exchmarkt+'''
    where trade_dt>= '''+startdate+''' and trade_dt<= '''+enddate+''' and regexp_like(s_info_windcode,'^PM[0-9]{3}%')
    order by trade_dt'''
else:
    sql = '''select s_info_windcode,trade_dt,
    s_dq_presettle,s_dq_open,s_dq_high,s_dq_low,s_dq_close,s_dq_volume,s_dq_oi from '''+exchmarkt+'''
    where trade_dt>= '''+startdate+''' and trade_dt<= '''+enddate+''' and regexp_like(s_info_windcode,'^'''+vt+'''[0-9]{4}____')
    order by trade_dt'''
cursor.execute(sql)
trade_data = cursor.fetchall()
trade_data = pd.DataFrame(trade_data)
trade_data.columns = [i[0] for i in cursor.description]
trade_data = trade_data.sort_values(by = ['TRADE_DT','S_INFO_WINDCODE'])



trade_sort = trade_data.sort_values(by = ['TRADE_DT','S_DQ_OI','S_INFO_WINDCODE'], ascending = [1,0,1])
maxOI = trade_sort.groupby('TRADE_DT').nth(0).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]
subOI = trade_sort.groupby('TRADE_DT').nth(1).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]
thiOI = trade_sort.groupby('TRADE_DT').nth(2).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]
dom_code = maxOI.copy()
sub_code = subOI.copy()
if vt in CFEcode:
    # 金融期货直接按maxOI，subOI，且不回滚 ps:windcode&code
    sub_code[maxOI[['S_INFO_WINDCODE']] > subOI[['S_INFO_WINDCODE']]] = thiOI
else:
    # 商品期货满足最大持仓量满3天
    FSlag1check = subOI.shift(1) == maxOI
    FSlag2check = subOI.shift(2) == maxOI
    FScheck = pd.concat([FSlag1check['S_INFO_WINDCODE'],FSlag2check['S_INFO_WINDCODE']], axis=1).any(axis=1)
    dom_code[FScheck] = None
    dom_code = dom_code.fillna(method = 'ffill')
    STlag1check = thiOI.shift(1) == subOI
    STlag2check = thiOI.shift(1) == subOI
    STcheck = pd.concat([STlag1check['S_INFO_WINDCODE'],STlag2check['S_INFO_WINDCODE']], axis=1).any(axis=1) ###
    sub_code[STcheck] = None
    sub_code = sub_code.fillna(method = 'ffill')


lead = code['S_INFO_WINDCODE'].shift(-1) != code['S_INFO_WINDCODE']
lag = code['S_INFO_WINDCODE'].shift(1) != code['S_INFO_WINDCODE']
lead.iloc[-1] = False
lag.iloc[0] = False
temp1 = pd.concat([code[lead].reset_index().drop(columns='index'),code[lag].reset_index().drop(columns=['index','TRADE_DT'])],axis=1)
temp1.columns = ['TRADE_DT','old','new']
temp2 = temp1.merge(trade_data[['TRADE_DT','S_INFO_WINDCODE','S_DQ_CLOSE']],left_on=['TRADE_DT','old'],right_on=['TRADE_DT','S_INFO_WINDCODE'])
del temp2['S_INFO_WINDCODE']
temp2 = temp2.rename(columns={'S_DQ_CLOSE':'oldclose'})
temp3 = temp2.merge(trade_data[['TRADE_DT','S_INFO_WINDCODE','S_DQ_CLOSE']],left_on=['TRADE_DT','new'],right_on=['TRADE_DT','S_INFO_WINDCODE'])
del temp3['S_INFO_WINDCODE']
temp3 = temp3.rename(columns={'S_DQ_CLOSE':'newclose'})
temp3['adj_factor'] = temp3['oldclose'] / temp3['newclose']
code['adj_factor'] = None
temp3.index = code[lag][['adj_factor']].index
code[['adj_factor']] = temp3[['adj_factor']]
code = code.fillna(method = 'ffill')
code = code.fillna(value = 1) # 第一个调整因子为1
