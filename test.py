import cx_Oracle
import pandas as pd
import numpy as np
import h5py
import re

# CCommodityFuturesEODPrices：date vt preSettle open high low close volumn openinterest
class GetFutureData:

    def __init__(self,vt,startdate,enddate):
        self.vt = vt
        self.startdate = startdate
        self.enddate = enddate
        db = cx_Oracle.connect('fe','fe','192.168.100.22:1521/winddb')
        self.cursor = db.cursor()

    def define_ExchMarktList(self):
        '''定义不同品种所属列表'''
        exchmarkt = {}
        exchmarkt['CFE'] = 'filesync.CIndexFuturesEODPrices'         # 中金所
        exchmarkt['SHF'] = 'filesync.CCommodityFuturesEODPrices'     # 上期所
        exchmarkt['CZC'] = 'filesync.CCommodityFuturesEODPrices'     # 郑商所
        exchmarkt['DCE'] = 'filesync.CCommodityFuturesEODPrices'     # 大商所
        sql = '''select  s_info_windcode
        from filesync.CFuturesDescription '''"where s_info_windcode LIKE'"+self.vt+'''%' and rownum=1'''
        self.cursor.execute(sql)
        t = self.cursor.fetchall()
        return exchmarkt[t[0][0][-3:]]  #例如'IF1003.CFE'返回的是exchmarkt['CFE']的值

    def get_trade_data(self):
        '''读取日交易数据'''
        sql = '''select s_info_windcode,
        trade_dt,
        s_dq_presettle,
        s_dq_open,
        s_dq_high,
        s_dq_low,
        s_dq_close,
        s_dq_volume,
        s_dq_oi from '''+self.define_ExchMarktList()+'''
        where trade_dt>= '''+self.startdate+' and trade_dt<= '+self.enddate+"and s_info_windcode LIKE'"+self.vt+'''%'
        and LENGTH(s_info_windcode)>=10
        order by trade_dt'''
        self.cursor.execute(sql)
        trade_data = self.cursor.fetchall()
        trade_data = pd.DataFrame(trade_data)
        trade_data.columns = [i[0] for i in self.cursor.description]
        trade_data = trade_data.sort_values(by = ['TRADE_DT','S_INFO_WINDCODE'])
        return trade_data


a = GetFutureData('CS','20170101','20171231')
trade_data = a.get_trade_data()

trade_sort = trade_data.sort_values(by = ['TRADE_DT','S_DQ_OI','S_INFO_WINDCODE'], ascending = [1,0,1])
maxOI = trade_sort.groupby('TRADE_DT').nth(0).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]
subOI = trade_sort.groupby('TRADE_DT').nth(1).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]


    