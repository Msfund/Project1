import cx_Oracle
import pandas as pd
import numpy as np
import re
from HdfUtility import *
from rawUlt import *

class HisDayData:

    def __init__(self):
        db = cx_Oracle.connect(EXT_Wind_User,EXT_Wind_User,EXT_Wind_Link)
        self.cursor = db.cursor()

    def getData(self,startdate,enddate,is_save=False):
        asset_list = {}
        asset_list[EXT_EXCHANGE_CFE] = EXT_CFE_ALL
        asset_list[EXT_EXCHANGE_SHFE] = EXT_SHFE_ALL
        asset_list[EXT_EXCHANGE_DCE] = EXT_DCE_ALL
        # asset_list[EXT_EXCHANGE_CZCE] = EXT_CZCE_ALL
        raw_data = {}
        dom_code = {}
        sub_code = {}
        for excode,symbol in asset_list.items():
            for i in range(len(symbol)):
                print(symbol[i])
                raw_data[symbol[i]] = self.getQuoteWind(excode,symbol[i],startdate,enddate)
                if raw_data[symbol[i]] is None:
                    continue
                else:
                    dom_code[symbol[i]],sub_code[symbol[i]] = self.getStitchRule(excode,symbol[i],startdate,enddate,raw_data[symbol[i]])
                if is_save == True:
                    hdf = HdfUtility()
                    hdf.hdfWrite(EXT_Path,excode,symbol[i],startdate,enddate,dom_code[symbol[i]],EXT_Series_0)
                    hdf.hdfWrite(EXT_Path,excode,symbol[i],startdate,enddate,sub_code[symbol[i]],EXT_Series_1)
                    hdf.hdfWrite(EXT_Path,excode,symbol[i],startdate,enddate,raw_data[symbol[i]],EXT_Period_1)
        return raw_data,dom_code,sub_code

    def getQuoteWind(self,excode,symbol,startdate,enddate):
        if symbol in EXT_CFE_STOCK:
            exchmarkt = EXT_CFE_STOCK_FILE
        elif symbol in EXT_CFE_BOND:
            exchmarkt = EXT_CFE_BOND_FILE
        elif symbol in EXT_SHFE_ALL:
            exchmarkt = EXT_SHFE_DATA_FILE
        elif symbol in EXT_DCE_ALL:
            exchmarkt = EXT_DCE_DATA_FILE
        elif symbol in EXT_CZCE_ALL:
            exchmarkt = EXT_CZCE_DATA_FILE
        else:
            print("Wrong Symbol")
            return
        l = 3 if symbol in EXT_CZCE_ALL else 4

        sql = ''' select '''+EXT_In_Header+''' from '''+exchmarkt+'''
        where '''+EXT_In_Date+''' >= '''+startdate+''' and '''+EXT_In_Date+''' <= '''+enddate+" and regexp_like("+EXT_In_Asset+", '"+'^'+symbol+str('[0-9]{')+str(l)+"}')"+'''
        order by trade_dt'''
        self.cursor.execute(sql)
        raw_data = pd.DataFrame(self.cursor.fetchall())
        try:
            raw_data.columns = EXT_Out_Header.split(',')
        except ValueError:
            print("No raw_data found")
            return

        sql = ''' select '''+EXT_In_Header2+''' from '''+EXT_Delistdate_File+'''
        where '''+EXT_In_Delistdate+''' > '''+startdate+'''
        and '''+EXT_In_Asset+" LIKE'"+symbol+'''%' order by '''+EXT_In_Asset
        self.cursor.execute(sql)
        delistdate = pd.DataFrame(self.cursor.fetchall())
        try:
            delistdate.columns = EXT_Out_Header2.split(',')
        except ValueError:
            print("No delistdate found")
            return
        raw_data = raw_data.merge(delistdate,on=[EXT_Out_Asset],how='left')
        raw_data = raw_data.sort_values(by = [EXT_Out_Date,EXT_Out_Asset])
        
        return raw_data

    def futureDelistdate(self,symbol,startdate):
        # 获取合约退市日期
        sql = ''' select '''+EXT_In_Header2+''' from '''+EXT_Delistdate_File+'''
        where '''+EXT_In_Delistdate+''' > '''+startdate+'''
        and '''+EXT_In_Asset+" LIKE'"+symbol+'''%' order by '''+EXT_In_Asset
        self.cursor.execute(sql)
        delistdate = pd.DataFrame(self.cursor.fetchall())
        delistdate.columns = EXT_In_Header2.split(',')
        if symbol in EXT_CZCE_ALL:
            #下面把所有郑州商品交易所原始数据三位数合约代码改为四位数
            code_num = pd.Series([re.findall(r"\d*",delistdate[EXT_In_Asset][i])[2] for i in range(len(delistdate[EXT_In_Asset]))])
            delist_len = pd.Series([len(code_num[i]) for i in range(len(code_num))])
            delistdate[EXT_In_Asset].ix[delist_len == 3] = symbol+'1'+code_num.ix[delist_len == 3]+'.CZC'
            delistdate[EXT_In_Asset].ix[delist_len == 4] = symbol+code_num.ix[delist_len == 4]+'.CZC'
        return delistdate

    def getStitchRule(self,excode,symbol,startdate,enddate,raw_data):
        trade_sort = raw_data.sort_values(by = [EXT_Out_Date,EXT_Out_OpenInterest], ascending = [1,0])
        delistdate = self.futureDelistdate(symbol,startdate)
        delistdate.columns = EXT_Out_Header2.split(',')
        # 取持仓量前三合约的时间、代码 maxOI subOI
        maxOI = trade_sort.groupby(EXT_Out_Date).nth(0).reset_index()[[EXT_Out_Date,EXT_Out_Asset]]
        subOI = trade_sort.groupby(EXT_Out_Date).nth(1).reset_index()[[EXT_Out_Date,EXT_Out_Asset]]
        # 初始化主力合约、次主力合约代码，默认为持仓量最大，次大的合约
        dom_code = maxOI.copy()
        sub_code = subOI.copy()
        #----------------------------------------------------------------------
        #满足最大持仓量满 3天且不向当月切换
        ##先找到换仓点
        dom_loca = ~(dom_code[EXT_Out_Asset] == dom_code[EXT_Out_Asset].shift(1))
        sub_loca = ~(sub_code[EXT_Out_Asset] ==sub_code[EXT_Out_Asset].shift(1))
        ##再找到满足持仓三天的合约（loca&check2同时满足时保留）
        dom_check2 = (dom_code[EXT_Out_Asset] == dom_code[EXT_Out_Asset].shift(-1)) & (dom_code[EXT_Out_Asset] == dom_code[EXT_Out_Asset].shift(-2))
        sub_check2 = (sub_code[EXT_Out_Asset] == sub_code[EXT_Out_Asset].shift(-1)) & (sub_code[EXT_Out_Asset] == sub_code[EXT_Out_Asset].shift(-2))
        ##找到合约切换时为当月合约
        ###先转换合约名称(只需要数字部分),将只有三位数字的合约名称前添加数字1
        dcode = pd.Series([re.findall(r"\d*",dom_code[EXT_Out_Asset][i])[2] for i in range(len(dom_code[EXT_Out_Asset]))])
        scode = pd.Series([re.findall(r"\d*",sub_code[EXT_Out_Asset][i])[2] for i in range(len(sub_code[EXT_Out_Asset]))])
        ###找到dcode等于合约月份的位置（这里比较年份数+月份数），且第一个合约不切换
        dom_check3 = (dcode == dom_code[EXT_Out_Date].str[2:6])
        dom_check3[0] = False
        sub_check3 = (scode == sub_code[EXT_Out_Date].str[2:6])
        sub_check3[0] = False
        ##找到是当月合约且换仓的位置,设置为None 由于主力合约持仓量退市期间将会下降到次主力合约，有3天的判断期
        for i in range(3):
            dom_code[EXT_Out_Asset].ix[dom_loca.shift(i) & dom_check3] = None
            sub_code[EXT_Out_Asset].ix[sub_loca.shift(i) & sub_check3] = None
        dom_code[EXT_Out_Asset] = dom_code[EXT_Out_Asset].fillna(method = 'ffill')
        sub_code[EXT_Out_Asset] = sub_code[EXT_Out_Asset].fillna(method = 'ffill')
        ##找到满足3天条件合约同时换仓的位置，除满足两个条件的位置外，其他设为None,接着从前向后填充ffill
        dom_loca = ~(dom_code[EXT_Out_Asset] == dom_code[EXT_Out_Asset].shift(1))
        sub_loca = ~(sub_code[EXT_Out_Asset] ==sub_code[EXT_Out_Asset].shift(1))
        dom_code[EXT_Out_Asset].ix[~(dom_loca & dom_check2)] = None
        sub_code[EXT_Out_Asset].ix[~(sub_loca & sub_check2)] = None
        dom_code[EXT_Out_Asset] = dom_code[EXT_Out_Asset].fillna(method = 'ffill')
        sub_code[EXT_Out_Asset] = sub_code[EXT_Out_Asset].fillna(method = 'ffill')
        #--------------------------------------------------------------------------------
        if symbol in EXT_CFE_ALL:
            pass
            # 处理主力合约和次主力合约，金融期货不回滚
            dom_check4 = []
            for i in range(len(dom_code[EXT_Out_Asset])):
                dom_check4.append(dom_code[EXT_Out_Asset][i]<dom_code[EXT_Out_Asset][:i+1].max())
            dom_check4 = pd.Series(dom_check4)
            dom_code[EXT_Out_Asset].ix[dom_check4] = None
            dom_code = dom_code.fillna(method = 'ffill')
            sub_check4 = []
            for i in range(len(sub_code[EXT_Out_Asset])):
                sub_check4.append(sub_code[EXT_Out_Asset][i]<sub_code[EXT_Out_Asset][:i+1].max())
            sub_check4 = pd.Series(sub_check4)
            sub_code[EXT_Out_Asset].ix[sub_check4] = None
            sub_code = sub_code.fillna(method = 'ffill')
        #--------------------------------------------------------------------------------
         #由于判断持仓量需要3天，在第4天才能换仓，所以数据往后移3个交易日
        dom_code[EXT_Out_Asset] = dom_code[EXT_Out_Asset].shift(3)
        sub_code[EXT_Out_Asset] = sub_code[EXT_Out_Asset].shift(3)
        dom_code[EXT_Out_Asset] = dom_code[EXT_Out_Asset].fillna(method = 'bfill')
        sub_code[EXT_Out_Asset] = sub_code[EXT_Out_Asset].fillna(method = 'bfill')
        #----------------------------------------------------------------------
        #合并退市数据
        dom_code = pd.merge(dom_code,delistdate, how = 'left', on = EXT_Out_Asset)
        sub_code = pd.merge(sub_code,delistdate, how = 'left', on = EXT_Out_Asset)
        #这里check1为判断移动三天后是否退市
        dom_check1 = (dom_code[EXT_Out_Date]>=dom_code[EXT_Out_Delistdate])
        sub_check1 = (sub_code[EXT_Out_Date]>=sub_code[EXT_Out_Delistdate])
        dom_code[EXT_Out_Asset].ix[dom_check1]=None
        sub_code[EXT_Out_Asset].ix[sub_check1]=None
        #向前填充，主力合约向后递延
        dom_code[EXT_Out_Asset] = dom_code[EXT_Out_Asset].fillna(method = 'bfill')
        sub_code[EXT_Out_Asset] = sub_code[EXT_Out_Asset].fillna(method = 'bfill')
        #----------------------------------------------------------------------
        #主力合约如果和次力合约相同，从后向前填充
        check = (dom_code[EXT_Out_Asset] == sub_code[EXT_Out_Asset])
        sub_code[EXT_Out_Asset].ix[check] = None
        sub_code[EXT_Out_Asset] = sub_code[EXT_Out_Asset].fillna(method = 'bfill')
        #----------------------------------------------------------------------
        #删除
        dom_code.drop([EXT_Out_Delistdate],axis=1,inplace = True)
        sub_code.drop([EXT_Out_Delistdate],axis=1,inplace = True)
        #----------------------------------------------------------------------
        # 获取调整因子的数据
        dom_code = self.getAdjFactor(raw_data,dom_code)
        sub_code = self.getAdjFactor(raw_data,sub_code)
        return dom_code,sub_code

    def getAdjFactor(self,raw_data,code):
        # 找到切换点 lead lag
        lead = code[EXT_Out_Asset].shift(-1) != code[EXT_Out_Asset]
        lag = code[EXT_Out_Asset].shift(1) != code[EXT_Out_Asset]
        lead.iloc[-1] = False
        lag.iloc[0] = False
        temp1 = pd.concat([code[lead].reset_index().drop(columns='index'),code[lag].reset_index().drop(columns=['index',EXT_Out_Date])],axis=1)
        temp1.columns = [EXT_Out_Date,'OldAsset','NewAsset']
        temp2 = temp1.merge(raw_data[[EXT_Out_Date,EXT_Out_Asset,EXT_Out_Close]],left_on=[EXT_Out_Date,'OldAsset'],right_on=[EXT_Out_Date,EXT_Out_Asset])
        del temp2[EXT_Out_Asset]
        temp2 = temp2.rename(columns={EXT_Out_Close:'OldClose'})
        temp3 = temp2.merge(raw_data[[EXT_Out_Date,EXT_Out_Asset,EXT_Out_Close]],left_on=[EXT_Out_Date,'NewAsset'],right_on=[EXT_Out_Date,EXT_Out_Asset])
        del temp3[EXT_Out_Asset]
        temp3 = temp3.rename(columns={EXT_Out_Close:'NewClose'})
        # t时主力合约从C1切换成C2，adj_factor = C1_Close(t-1)/C2_Close(t-1)
        temp3[EXT_Out_AdjFactor] = temp3['OldClose'] / temp3['NewClose']
        code[EXT_Out_AdjFactor] = None
        temp3.index = code[lag][[EXT_Out_AdjFactor]].index
        code[[EXT_Out_AdjFactor]] = temp3[[EXT_Out_AdjFactor]]
        code = code.fillna(method = 'ffill')
        code = code.fillna(value = 1) # 第一个调整因子为1
        return code

    def getStitchData(self,excode,symbol,startdate,enddate):
        hdf = HdfUtility()
        raw_data = hdf.hdfRead(EXT_Path,excode,symbol,startdate,enddate,EXT_Period_1)
        dom_rule = hdf.hdfRead(EXT_Path,excode,symbol,startdate,enddate,EXT_Series_0)
        sub_rule = hdf.hdfRead(EXT_Path,excode,symbol,startdate,enddate,EXT_Series_1)
        dom_data = dom_rule.merge(raw_data,on=[EXT_Out_Date,EXT_Out_Asset],how='left')
        sub_data = sub_rule.merge(raw_data,on=[EXT_Out_Date,EXT_Out_Asset],how='left')
        dom_data.sort_values(by=[EXT_Out_Date,EXT_Out_Asset],inplace=True)
        sub_data.sort_values(by=[EXT_Out_Date,EXT_Out_Asset],inplace=True)
        return dom_data, sub_data

if __name__  ==  '__main__':
    a = HisDayData()
    raw_data = a.getQuoteWind('CFE','IF','20170101','20171231')
