import cx_Oracle
import pandas as pd
import numpy as np
import re
from putdata import *

path = 'C:\\Users\\user\\GitHub\\Project1\\out.hdf5'
CFEcode = ('IF','IC','IH','TF','T')
SHFcode = ('CU','AL','ZN','RU','FU','AU','AG','RB','WR','PB','BU','HC','NI','SN')
DCEcode = ('A','B','M','C','Y','P','L','V','J','I','JM','JD','FB','BB','PP','CS')
CZCcode = ('PM','WH','CF','SR','OI','TA','RI','LR','MA','FG','RS','RM','TC','ZC','JR','SF','SM')
Allcode = CFEcode + SHFcode + DCEcode + CZCcode

# CCommodityFuturesEODPrices：date vt preSettle open high low close volumn openinterest
class HisDayData:

    def __init__(self,excode,vt,startdate,enddate):
        self.excode = excode
        self.vt = vt
        self.startdate = startdate
        self.enddate = enddate
        db = cx_Oracle.connect('fe','fe','192.168.100.22:1521/winddb')
        self.cursor = db.cursor()
        self.trade_data = self.GetRawData()

    def GetRawData(self,save_hdf=False):
        l=4
        if self.vt in CFEcode:
            exchmarkt = 'filesync.CIndexFuturesEODPrices'
        else:
            exchmarkt = 'filesync.CCommodityFuturesEODPrices'
            if self.vt in DCEcode :
                l=3
        sql = '''select s_info_windcode,trade_dt,
        s_dq_presettle,s_dq_open,s_dq_high,s_dq_low,s_dq_close,s_dq_volume,s_dq_oi from '''+exchmarkt+'''
        where trade_dt>= '''+self.startdate+''' and trade_dt<= '''+self.enddate+" and regexp_like(s_info_windcode, '"+'^'+self.vt+str('[0-9]{')+str(l)+"}')"+'''
        and fs_info_type = '2'
        order by trade_dt'''
        self.cursor.execute(sql)
        trade_data = self.cursor.fetchall()
        trade_data = pd.DataFrame(trade_data)
        trade_data.columns = [i[0] for i in self.cursor.description]
        trade_data = trade_data.sort_values(by = ['TRADE_DT','S_INFO_WINDCODE'])
        if save_hdf == True:
            HDFutility(path,self.excode, self.vt, self.startdate, self.enddate).HDFwrite(trade_data,'1d')
        return trade_data

    def future_delistdate(self):
        # 获取合约退市日期
        sql = '''select fs_info_sccode,
        s_info_windcode,
        s_info_listdate,
        s_info_delistdate
        from filesync.CFuturesDescription
        where s_info_delistdate > '''+self.startdate+" and s_info_windcode LIKE'"+self.vt+'''%'order by s_info_windcode'''
        self.cursor.execute(sql)
        delistdate = pd.DataFrame(self.cursor.fetchall())
        delistdate.columns = [i[0] for i in self.cursor.description]
        return delistdate

    def GetStitchRule(self,save_hdf=False):
        trade_data = self.trade_data
        trade_sort = trade_data.sort_values(by = ['TRADE_DT','S_DQ_OI'], ascending = [1,0])
        delistdate = self.future_delistdate()
        # 取持仓量前三合约的时间、代码 maxOI subOI
        maxOI = trade_sort.groupby('TRADE_DT').nth(0).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]
        subOI = trade_sort.groupby('TRADE_DT').nth(1).reset_index()[['TRADE_DT','S_INFO_WINDCODE']]
        # 初始化主力合约、次主力合约代码，默认为持仓量最大，次大的合约
        dom_code = maxOI.copy()
        sub_code = subOI.copy()
        #----------------------------------------------------------------------
        #满足最大持仓量满 3天且不向当月切换
        ##先找到换仓点
        dom_loca = ~(dom_code['S_INFO_WINDCODE'] == dom_code['S_INFO_WINDCODE'].shift(1))
        sub_loca = ~(sub_code['S_INFO_WINDCODE'] ==sub_code['S_INFO_WINDCODE'].shift(1))
        ##再找到满足持仓三天的合约（loca&check2同时满足时保留）
        dom_check2 = (dom_code['S_INFO_WINDCODE'] == dom_code['S_INFO_WINDCODE'].shift(-1)) & (dom_code['S_INFO_WINDCODE'] == dom_code['S_INFO_WINDCODE'].shift(-2))
        sub_check2 = (sub_code['S_INFO_WINDCODE'] == sub_code['S_INFO_WINDCODE'].shift(-1)) & (sub_code['S_INFO_WINDCODE'] == sub_code['S_INFO_WINDCODE'].shift(-2))
        ##找到合约切换时为当月合约
        ###先转换合约名称(只需要数字部分),将只有三位数字的合约名称前添加数字1
        dcode = pd.Series([re.findall(r"\d*",dom_code['S_INFO_WINDCODE'][i])[2] for i in range(len(dom_code['S_INFO_WINDCODE']))])
        scode = pd.Series([re.findall(r"\d*",sub_code['S_INFO_WINDCODE'][i])[2] for i in range(len(sub_code['S_INFO_WINDCODE']))])
        dlen = pd.Series([len(dcode[i]) for i in range(len(dcode))])
        slen = pd.Series([len(scode[i]) for i in range(len(scode))])
        dcode.ix[dlen == 3] = '1'+dcode.ix[dlen == 3]
        scode.ix[dlen == 3] = '1'+scode.ix[slen == 3]
        ###找到dcode等于合约月份的位置（这里比较年份数+月份数），且第一个合约不切换
        dom_check3 = (dcode == dom_code['TRADE_DT'].str[2:6])
        dom_check3[0] = False
        sub_check3 = (scode == sub_code['TRADE_DT'].str[2:6])
        sub_check3[0] = False
        ##找到是当月合约且换仓的位置,设置为None 由于主力合约持仓量退市期间将会下降到次主力合约，有3天的判断期
        for i in range(3):
            dom_code['S_INFO_WINDCODE'].ix[dom_loca.shift(i) & dom_check3] = None
            sub_code['S_INFO_WINDCODE'].ix[sub_loca.shift(i) & sub_check3] = None
        dom_code['S_INFO_WINDCODE'] = dom_code['S_INFO_WINDCODE'].fillna(method = 'ffill')
        sub_code['S_INFO_WINDCODE'] = sub_code['S_INFO_WINDCODE'].fillna(method = 'ffill')
        ##找到满足3天条件合约同时换仓的位置，除满足两个条件的位置外，其他设为None,接着从前向后填充ffill
        dom_loca = ~(dom_code['S_INFO_WINDCODE'] == dom_code['S_INFO_WINDCODE'].shift(1))
        sub_loca = ~(sub_code['S_INFO_WINDCODE'] ==sub_code['S_INFO_WINDCODE'].shift(1))
        dom_code['S_INFO_WINDCODE'].ix[~(dom_loca & dom_check2)] = None
        sub_code['S_INFO_WINDCODE'].ix[~(sub_loca & sub_check2)] = None
        dom_code['S_INFO_WINDCODE'] = dom_code['S_INFO_WINDCODE'].fillna(method = 'ffill')
        sub_code['S_INFO_WINDCODE'] = sub_code['S_INFO_WINDCODE'].fillna(method = 'ffill')
        #--------------------------------------------------------------------------------
        if self.vt in CFEcode:
            pass
            # 处理主力合约和次主力合约，金融期货不回滚
            dom_check4 = []
            for i in range(len(dom_code['S_INFO_WINDCODE'])):
                dom_check4.append(dom_code['S_INFO_WINDCODE'][i]<dom_code['S_INFO_WINDCODE'][:i+1].max())
            dom_check4 = pd.Series(dom_check4)
            dom_code['S_INFO_WINDCODE'].ix[dom_check4] = None
            dom_code = dom_code.fillna(method = 'ffill')
            sub_check4 = []
            for i in range(len(sub_code['S_INFO_WINDCODE'])):
                sub_check4.append(sub_code['S_INFO_WINDCODE'][i]<sub_code['S_INFO_WINDCODE'][:i+1].max())
            sub_check4 = pd.Series(sub_check4)
            sub_code['S_INFO_WINDCODE'].ix[sub_check4] = None
            sub_code = sub_code.fillna(method = 'ffill')
        #--------------------------------------------------------------------------------
         #由于判断持仓量需要3天，在第4天才能换仓，所以数据往后移3个交易日
        dom_code['S_INFO_WINDCODE'] = dom_code['S_INFO_WINDCODE'].shift(3)
        sub_code['S_INFO_WINDCODE'] = sub_code['S_INFO_WINDCODE'].shift(3)
        dom_code['S_INFO_WINDCODE'] = dom_code['S_INFO_WINDCODE'].fillna(method = 'bfill')
        sub_code['S_INFO_WINDCODE'] = sub_code['S_INFO_WINDCODE'].fillna(method = 'bfill')
        #----------------------------------------------------------------------
        #合并退市数据
        dom_code = pd.merge(dom_code,delistdate, how = 'left', on = 'S_INFO_WINDCODE')
        sub_code = pd.merge(sub_code,delistdate, how = 'left', on = 'S_INFO_WINDCODE')
        #这里check1为判断移动三天后是否退市
        dom_check1 = (dom_code['TRADE_DT']>=dom_code['S_INFO_DELISTDATE'])
        sub_check1 = (sub_code['TRADE_DT']>=sub_code['S_INFO_DELISTDATE'])
        dom_code['S_INFO_WINDCODE'].ix[dom_check1]=None
        sub_code['S_INFO_WINDCODE'].ix[sub_check1]=None
        #向前填充，主力合约向后递延
        dom_code['S_INFO_WINDCODE'] = dom_code['S_INFO_WINDCODE'].fillna(method = 'bfill')
        sub_code['S_INFO_WINDCODE'] = sub_code['S_INFO_WINDCODE'].fillna(method = 'bfill')
        #----------------------------------------------------------------------
        #主力合约如果和次力合约相同，从后向前填充
        check = (dom_code['S_INFO_WINDCODE'] == sub_code['S_INFO_WINDCODE'])
        sub_code['S_INFO_WINDCODE'].ix[check] = None
        sub_code['S_INFO_WINDCODE'] = sub_code['S_INFO_WINDCODE'].fillna(method = 'bfill')
        #----------------------------------------------------------------------
        #删除
        dom_code.drop(['FS_INFO_SCCODE','S_INFO_LISTDATE','S_INFO_DELISTDATE'],axis=1,inplace = True)
        sub_code.drop(['FS_INFO_SCCODE','S_INFO_LISTDATE','S_INFO_DELISTDATE'],axis=1,inplace = True)
        #----------------------------------------------------------------------
        # 获取调整因子的数据
        dom_code = self.get_adj_factor(dom_code)
        sub_code = self.get_adj_factor(sub_code)
        if save_hdf == True:
            HDFutility(path,self.excode, self.vt, self.startdate, self.enddate).HDFwrite(dom_code,'00')
            HDFutility(path,self.excode, self.vt, self.startdate, self.enddate).HDFwrite(sub_code,'01')
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

    def GetStitchData(self,save_hdf=False):
        # 读RawData
        RawData = HDFutility(path,self.excode, self.vt, self.startdate, self.enddate).HDFread('1d')
        # 读Rule
        DomRule = HDFutility(path,self.excode, self.vt, self.startdate, self.enddate).HDFread('00')
        SubRule = HDFutility(path,self.excode, self.vt, self.startdate, self.enddate).HDFread('01')
        # stitch
        dom_data = dom_code.merge(trade_data,how='left',on=['TRADE_DT','S_INFO_WINDCODE']) ###
        sub_data = sub_code.merge(trade_data,how='left',on=['TRADE_DT','S_INFO_WINDCODE']) ###

        return dom_data, sub_data


if __name__  ==  '__main__':
    a = HisDayData('CFE','IF','20160101','20171231')
    trade_data = a.GetRawData(True)
    out = HDFutility(path,'CFE','IF','20160101','20171231').HDFread('1d')
#    dom_code, sub_code = a.GetStitchRule(True)
#    dom_data, sub_data = a.GetStitchData()