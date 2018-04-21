import cx_Oracle
import pandas as pd
import numpy as np
import h5py
import re

CFEcode = ('IF','IC','IH','TF','T')
SHFcode = ('cu','al','zn','ru','fu','au','ag','rb','wr','pb','bu','hc','NI','SN')
DCEcode = ('a','b','m','c','y','p','l','v','j','jm','i','jd','fb','bb','pp','cs')
CZCcode = ('PM','WH','CF','SR','OI','TA','RI','LR','MA','FG','RS','RM','TC','ZC','JR','SF','SM')
Allcode = CFEcode + SHFcode + DCEcode + CZCcode
# CCommodityFuturesEODPrices：date vt preSettle open high low close volumn openintereST
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
        sql = '''select s_info_windcode,trade_dt,
        s_dq_presettle,s_dq_open,s_dq_high,s_dq_low,s_dq_close,s_dq_volume,s_dq_oi from '''+exchmarkt+'''
        where trade_dt>= '''+self.startdate+' and trade_dt<= '+self.enddate+"and s_info_windcode LIKE '"+self.vt+'''%'
        and LENGTH(s_info_windcode)>=9
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
        maxOI = trade_sort.groupby('TRADE_DT').nth(0).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]
        subOI = trade_sort.groupby('TRADE_DT').nth(1).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]
        thiOI = trade_sort.groupby('TRADE_DT').nth(2).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]
        dom_code = maxOI
        sub_code = subOI
        if self.vt in CFEcode:
            # 金融期货直接按maxOI，subOI，且不回滚 ps:windcode&code
            sub_code[maxOI > subOI] = thiOI
        else:
            # 商品期货满足最大持仓量满3天
            FSlag1check = subOI.shift(1) == maxOI
            FSlag2check = subOI.shift(2) == maxOI
            FScheck = [FSlag1check,FSlag2check].any(axis=1)
            dom_code[FScheck] = None
            dom_code = dom_code.fillna(method = 'ffill')
            STlag1check = thiOI.shift(1) == subOI
            STlag2check = thiOI.shift(1) == subOI
            STcheck = [STlag1check,STlag2check].any(axis=1)
            dom_code[STcheck] = None
            sub_code = sub_code.fillna(method = 'ffill')
        return dom_code sub_code

    def get_adj_factor(self,code):
        lead = code['S_INFO_WINDCODE'].shift(-1) != code['S_INFO_WINDCODE']
        lag = code['S_INFO_WINDCODE'].shift(1) != code['S_INFO_WINDCODE']
        temp = code[lead].merge(code['S_INFO_WINDCODE'][lag])
        ### rename 'S_INFO_WINDCODE_x'=old 'S_INFO_WINDCODE_y'=new
        temp.merge(self.trade_data['S_DQ_CLOSE'],left_on='old',right_on='S_INFO_WINDCODE',inplace=True)
        temp.merge(self.trade_data['S_DQ_CLOSE'],left_on='new',right_on='S_INFO_WINDCODE',inplace=True)
        ### rename 'S_DQ_CLOSE_x'=oldclose 'S_DQ_CLOSE_y'=newclose
        temp['adj_factor'] = temp['oldclose'] / temp['newclose']
        code['adj_factor'] = None
        code[lag]['adj_factor'] = temp['adj_factor']
        code = code.fillna(methond = 'ffill')
        code = code.fillna(value = 1) # 第一个调整因子为1
        return code

if __name__  ==  '__main__':
    a = GetFutureData('SR','20170101','20171231')
    trade_data = a.get_trade_data()
    dom_code, sub_code = a.get_code()
    # a.save_code('C:\\users\\user\\Desktop\\out1.h5')
