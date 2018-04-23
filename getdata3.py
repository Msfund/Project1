import cx_Oracle
import pandas as pd
import numpy as np
import h5py
import re

CFEcode = ('IF','IC','IH','TF','T')
SHFcode = ('CU','AL','ZN','RU','FU','AU','AG','RB','WR','PB','BU','HC','NI','SN')
DCEcode = ('A','B','M','C','Y','P','L','V','J','I','JM','JD','FB','BB','PP','CS')
CZCcode = ('PM','WH','CF','SR','OI','TA','RI','LR','MA','FG','RS','RM','TC','ZC','JR','SF','SM')
Allcode = CFEcode + SHFcode + DCEcode + CZCcode
# CCommodityFuturesEODPrices：date vt preSettle open high low close volumn openinterest
class GetFutureData:

    def __init__(self,vt,startdate,enddate):
        self.vt = vt
        self.startdate = startdate
        self.enddate = enddate
        db = cx_Oracle.connect('fe','fe','192.168.100.22:1521/winddb')
        self.cursor = db.cursor()
        self.trade_data = self.get_trade_data()

    def get_trade_data(self):
        if self.vt in CFEcode:
            exchmarkt = 'filesync.CIndexFuturesEODPrices'
        else:
            exchmarkt = 'filesync.CCommodityFuturesEODPrices'
        if self.vt in DCEcode[0:10]:
            sql = '''select s_info_windcode,trade_dt,
            s_dq_presettle,s_dq_open,s_dq_high,s_dq_low,s_dq_close,s_dq_volume,s_dq_oi from '''+exchmarkt+'''
            where trade_dt>= '''+self.startdate+''' and trade_dt<= '''+self.enddate+" and s_info_windcode LIKE '"+self.vt+'''%'
            and fs_info_type = '2' and LENGTH(s_info_windcode)==9
            order by trade_dt'''
        else:
            sql = '''select s_info_windcode,trade_dt,
            s_dq_presettle,s_dq_open,s_dq_high,s_dq_low,s_dq_close,s_dq_volume,s_dq_oi from '''+exchmarkt+'''
            where trade_dt>= '''+self.startdate+''' and trade_dt<= '''+self.enddate+" and s_info_windcode LIKE '"+self.vt+'''%'
            and fs_info_type = '2'
            order by trade_dt'''
        self.cursor.execute(sql)
        trade_data = self.cursor.fetchall()
        trade_data = pd.DataFrame(trade_data)
        trade_data.columns = [i[0] for i in self.cursor.description]
        trade_data = trade_data.sort_values(by = ['TRADE_DT','S_INFO_WINDCODE'])
        return trade_data

    def get_code(self):
        trade_data = self.get_trade_data()
        trade_sort = trade_data.sort_values(by = ['TRADE_DT','S_DQ_OI','S_INFO_WINDCODE'], ascending = [1,0,1])
        # 取持仓量前三合约的时间、代码 maxOI subOI thiOI
        maxOI = trade_sort.groupby('TRADE_DT').nth(0).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]
        subOI = trade_sort.groupby('TRADE_DT').nth(1).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]
        thiOI = trade_sort.groupby('TRADE_DT').nth(2).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]
        # 初始化主力合约、次主力合约代码，默认为持仓量最大，次大的合约
        dom_code = maxOI.copy()
        sub_code = subOI.copy()
        if self.vt in CFEcode:
            pass
            # 金融期货直接按 maxOI，subOI，且不回滚
            FScheck = subOI.shift(1) == maxOI
            dom_code[FScheck] = None
            sub_code[FScheck] = None
            dom_code = dom_code.fillna(method = 'ffill')
            sub_code = sub_code.fillna(method = 'ffill')
            # sub_code平滑处理
            smoothcheck = pd.concat([sub_code['S_INFO_WINDCODE'].shift(1) != sub_code['S_INFO_WINDCODE'],sub_code['S_INFO_WINDCODE'].shift(-1) != sub_code['S_INFO_WINDCODE']], axis=1).all(axis=1)
            sub_code[smoothcheck] = None
            sub_code = sub_code.fillna(method = 'bfill')
        else:
            # 商品期货满足最大持仓量满 3天
            FSlag1check = subOI.shift(1) == maxOI
            FSlag2check = subOI.shift(2) == maxOI
            FScheck = pd.concat([FSlag1check['S_INFO_WINDCODE'],FSlag2check['S_INFO_WINDCODE']], axis=1).any(axis=1)
            dom_code[FScheck] = None
            sub_code[FScheck] = None
            dom_code = dom_code.fillna(method = 'ffill')
            sub_code = sub_code.fillna(method = 'ffill')
            STlag1check = thiOI.shift(1) == subOI
            STlag2check = thiOI.shift(2) == subOI
            STcheck = pd.concat([STlag1check['S_INFO_WINDCODE'],STlag2check['S_INFO_WINDCODE']], axis=1).any(axis=1)
            sub_code[STcheck] = None
            sub_code = sub_code.fillna(method = 'ffill')
        # 获取调整因子的数据
        dom_code = self.get_adj_factor(dom_code)
        sub_code = self.get_adj_factor(sub_code)
        return dom_code,sub_code

    def get_adj_factor(self,code):
        # 找到切换点 lead lag
        lead = code['S_INFO_WINDCODE'].shift(-1) != code['S_INFO_WINDCODE']
        lag = code['S_INFO_WINDCODE'].shift(1) != code['S_INFO_WINDCODE']
        lead.iloc[-1] = False
        lag.iloc[0] = False
        temp1 = pd.concat([code[lead].reset_index().drop(columns='index'),code[lag].reset_index().drop(columns=['index','TRADE_DT'])],axis=1)
        temp1.columns = ['TRADE_DT','old','new']
        temp2 = temp1.merge(self.trade_data[['TRADE_DT','S_INFO_WINDCODE','S_DQ_CLOSE']],left_on=['TRADE_DT','old'],right_on=['TRADE_DT','S_INFO_WINDCODE'])
        del temp2['S_INFO_WINDCODE']
        temp2 = temp2.rename(columns={'S_DQ_CLOSE':'oldclose'})
        temp3 = temp2.merge(self.trade_data[['TRADE_DT','S_INFO_WINDCODE','S_DQ_CLOSE']],left_on=['TRADE_DT','new'],right_on=['TRADE_DT','S_INFO_WINDCODE'])
        del temp3['S_INFO_WINDCODE']
        temp3 = temp3.rename(columns={'S_DQ_CLOSE':'newclose'})
        # t时主力合约从C1切换成C2，adj_factor = C1_Close(t-1)/C2_Close(t-1)
        temp3['adj_factor'] = temp3['oldclose'] / temp3['newclose']
        code['adj_factor'] = None
        temp3.index = code[lag][['adj_factor']].index
        code[['adj_factor']] = temp3[['adj_factor']]
        code = code.fillna(method = 'ffill')
        code = code.fillna(value = 1) # 第一个调整因子为1
        return code

    def save_code(self,path):
        dom_code,sub_code = self.get_code()
        dom_code.to_hdf(path,'dom_code')
        sub_code.to_hdf(path,'sub_code')

if __name__  ==  '__main__':
    a = GetFutureData('IF','20150101','20171231')
    trade_data = a.get_trade_data()
    dom_code, sub_code = a.get_code()
    # a.save_code('C:\\users\\user\\Desktop\\out1.h5')
