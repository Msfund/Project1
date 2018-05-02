import cx_Oracle
import pandas as pd
import numpy as np
import re
from putdata import HdfUtility
from rawUlt import *

class HisDayData:

    def __init__(self):
        db = cx_Oracle.connect(EXT_Wind_User,EXT_Wind_User,EXT_Wind_Link)
        self.cursor = db.cursor()

    def getRawData(self,startdate,enddate,is_save=False):
        AssetList = {}
        AssetList[EXT_EXCHANGE_CFE] = EXT_CFE_ALL
        AssetList[EXT_EXCHANGE_SHFE] = EXT_SHFE_ALL
        AssetList[EXT_EXCHANGE_DCE] = EXT_DCE_ALL
        AssetList[EXT_EXCHANGE_CZCE] = EXT_CZCE_ALL
        AssetData = {}
        for exch,asset in AssetList.items():
            for i in range(len(asset)):
                print(asset[i])
                AssetData[asset[i]] = self.getQuoteWind(exch,asset[i],startdate,enddate)
                if is_save == True:
                    hdf = HdfUtility()
                    hdf.hdfWrite(path,exch,asset[i],startdate,enddate,AssetData[asset[i]],EXT_Period_1)

    def getQuoteWind(self,excode,symbol,startdate,enddate):
        if symbol in EXT_CFE_ALL:
            exchmarkt = EXT_CFE_DATA_FILE
        elif symbol in EXT_SHFE_ALL:
            exchmarkt = EXT_SHFE_DATA_FILE
        elif symbol in EXT_DCE_ALL:
            exchmarkt = EXT_DCE_DATA_FILE
        elif symbol in EXT_CZCE_ALL:
            exchmarkt = EXT_CZCE_DATA_FILE
        else:
            print("Wrong Symbol")
            return
        l = 3 if symbol in EXT_DCE_ALL else 4
        sql = ''' select '''+EXT_In_Header+''' from '''+exchmarkt+'''
        where '''+EXT_In_Date+''' >= '''+startdate+''' and '''+EXT_In_Date+''' <= '''+enddate+" and regexp_like("+EXT_In_Asset+", '"+'^'+symbol+str('[0-9]{')+str(l)+"}')"+'''
        order by trade_dt'''
        self.cursor.execute(sql)
        raw_data = self.cursor.fetchall()
        raw_data = pd.DataFrame(raw_data)
        try:
            raw_data.columns = EXT_Out_Header.split(',')
        except ValueError:
            print("No rawdata found")
            return
        raw_data = raw_data.sort_values(by = [EXT_Out_Date,EXT_Out_Asset])
        
        return raw_data

    def futureDelistdate(self):
        # 获取合约退市日期
        sql = ''' select '''+EXT_In_Header2+''' from '''+EXT_Delistdate_File+'''
        where '''+EXT_In_delistdate+''' > '''+startdate+'''
        and '''+EXT_In_Asset+" LIKE'"+symbol+'''%' order by'''+EXT_In_Asset
        self.cursor.execute(sql)
        delistdate = pd.DataFrame(self.cursor.fetchall())
        delistdate.columns = EXT_In_Header2.split(',')
        return delistdate

    def getStitchRule(self,raw_data,save_hdf=False):
        trade_sort = raw_data.sort_values(by = [EXT_Out_Date,'S_DQ_OI'], ascending = [1,0])
        delistdate = self.futureDelistdate()
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
        dlen = pd.Series([len(dcode[i]) for i in range(len(dcode))])
        slen = pd.Series([len(scode[i]) for i in range(len(scode))])
        dcode.ix[dlen == 3] = '1'+dcode.ix[dlen == 3]
        scode.ix[dlen == 3] = '1'+scode.ix[slen == 3]
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
        if self.vt in CFEcode:
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
        if save_hdf == True:
            hdf = HdfUtility()
            hdf.hdfWrite(path,self.excode, self.vt, self.startdate, self.enddate,dom_code,'00')
            hdf.hdfWrite(path,self.excode, self.vt, self.startdate, self.enddate,sub_code,'01')
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

    def getStitchData(self):
        hdf = HdfUtility()
        # 读RawData
        RawData = hdf.hdfRead(path,self.excode, self.vt, self.startdate, self.enddate,'1d')
        # 读Rule
        DomRule = hdf.hdfRead(path,self.excode, self.vt, self.startdate, self.enddate,'00')
        SubRule = hdf.hdfRead(path,self.excode, self.vt, self.startdate, self.enddate,'01')
        # stitch
        dom_data = DomRule.merge(RawData,how='left',on=[EXT_Out_Date,EXT_Out_Asset])
        sub_data = SubRule.merge(RawData,how='left',on=[EXT_Out_Date,EXT_Out_Asset])

        return dom_data, sub_data


if __name__  ==  '__main__':

    a = HisDayData()
    a.getRawData('20170101','20171231')
