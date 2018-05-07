# encoding: UTF-8
import pandas as pd
import numpy as np
import os 
import json
import pymongo  
from datetime import *
import time
from dateutil.parser import parse

#the function is to read the raw tick data from the txt and form the bar data and save the data to mongodb
def save_rawdata(dir, db_name, client):
    #the list of the collection name 
    data_name = [db_name+'_'+'tick',db_name+'_'+'1min',db_name+'_'+'3min',db_name+'_'+'5min',db_name+'_'+'10min',db_name+'_'+'1day']
    #get the database
    db = client[db_name]
    #if save the IF/TF data, execute eval1
    #if save the T data, execute eval2
    eval1 = ("db_name in root")
    eval2 = ("db_name in root and root[root.find('T')+1] != 'F'")
    if len(db_name) == 1:
        eval_temp = eval2
    else:
        eval_temp = eval1
    #save the TF/IF/T data 
    #save the high frequency raw data to the mongodb 
    #tick, 1min, 3 min, 5min, 10min 
    for root, dirs, files in os.walk(dir):
        if files:
            for f in files:
                if 'Survey' not in f:
                    if eval(eval_temp):
                        #get the year, month ,day from the dir
                        year =  root[(-12-len(db_name)):(-8-len(db_name))]
                        month = root[-4:-2]
                        day = root[-2:]
                        name = f[0:4+len(db_name)]
                        file_add =  root +'\\'+ f
                        #read txt accoridng to the dir
                        data = pd.read_table(file_add,sep =',')
                        data['name'] = name
                        #some txt's data format is different, so drop some columns 
                        if 'TradingDay' in data.columns:
                            del data['TradingDay']
                        if 'InstrumentID' in data.columns:
                            del data['InstrumentID']
                        if 'UpdateTime' in data.columns:
                            data['Time'] = data['UpdateTime']
                            del data['UpdateTime']
                        if 'Volume' in data.columns:
                            data['TradeVolume'] = data['Volume']
                            del data['Volume']
                        #add the date and datetime
                        data['date'] = year+month+day
                        data['datetime'] = year + '-' + month + '-' + day +' ' + data['Time']
                        data['datetime'] = pd.to_datetime(data['datetime'])
                        #use the datetime as index 
                        data.index = data['datetime']
                        del data['Time']
                        #drop the duplicates 
                        #the dict is used to form the min bar 
                        ohlc_dict = { 'open':'first','high':'max', 'low':'min','close': 'last','volume': 'sum'}
                        #drop the duplicates of the data 
                        data = data.drop_duplicates('datetime',keep = 'first')
                        #get the open close high low, form the 1 min bar data 
                        data_1min =data['LastPrice'].resample('1T', closed='left', label='left').apply(ohlc_dict)
                        #sum the volume, some data is missing, so the data may not precise
                        data_1min['volume'] = data['TradeVolume'].resample('1T',closed='left', label='left').sum()
                        #add the name and date column
                        #need to save the data to mongodb
                        #the data format need to be the same 
                        data_1min['name'] = name
                        data_1min['date'] = year+month+day
                        #drop the missing open data 
                        data_1min = data_1min[data_1min['open'].notnull()]
                        #form the 3 min bar 
                        data_3min = data_1min.resample('3T', closed='left', label='left').apply(ohlc_dict)
                        data_3min['name'] = name
                        data_3min['date'] = year+month+day
                        data_3min = data_3min[data_3min['open'].notnull()]
                        #form the 5 min bar 
                        data_5min = data_1min.resample('5T', closed='left', label='left').apply(ohlc_dict)
                        data_5min['name'] = name
                        data_5min['date'] = year+month+day
                        data_5min = data_5min[data_5min['open'].notnull()]
                        #form the 10min bar 
                        data_10min = data_1min.resample('10T', closed='left', label='left').apply(ohlc_dict)
                        data_10min['name'] = name
                        data_10min['date'] = year + month +day
                        data_10min = data_10min[data_10min['open'].notnull()]
                        #form the 1 day bar 
                        data_1day = data_10min.resample('1D', closed='left', label='left').apply(ohlc_dict)
                        data_1day['name'] = name
                        data_1day['date'] = year+month+day
                        data_1day = data_1day[data_1day['open'].notnull()]
                        data = data.reset_index(drop=True)
                        #insert the tick data to the mongodb
                        db[data_name[0]].insert_many(data.T.to_dict().values())
                        #add the datetime column and drop the index 
                        data_1min['datetime'] = data_1min.index
                        data_1min = data_1min.reset_index(drop=True)
                        #insert the 1min data column
                        db[data_name[1]].insert_many(data_1min.T.to_dict().values())
                        #add the datetime column and drop the index 
                        data_3min['datetime'] = data_3min.index
                        data_3min = data_3min.reset_index(drop=True)
                        #insert the 3 min data 
                        db[data_name[2]].insert_many(data_3min.T.to_dict().values())
                        #add the datetime column and drop the index 
                        data_5min['datetime'] = data_5min.index
                        data_5min = data_5min.reset_index(drop=True)
                        #insert the 5 min bar data 
                        db[data_name[3]].insert_many(data_5min.T.to_dict().values())
                        #add the datetime column and drop the index 
                        data_10min['datetime'] = data_10min.index
                        data_10min = data_10min.reset_index(drop=True)
                        #insert  the 10min bar data to mongodb
                        db[data_name[4]].insert_many(data_10min.T.to_dict().values())
                        #add the datetime column and drop the index 
                        data_1day['datetime'] = data_1day.index
                        data_1day = data_1day.reset_index(drop=True)
                        #insert the day bar to mongodb
                        db[data_name[5]].insert_many(data_1day.T.to_dict().values())


#get the dominant future according to the volume
def get_dom_future(client,dbname,collection_name):
    #get the dominant future constract of T
    #read the daily bar from the mongodb
    #read daily bar data 
    db = client[dbname]
    collection = db[collection_name]
    data_1day = collection.find()
    data_1day = pd.DataFrame(list(data_1day))
    #sort the daily bar data according to the datetime and volume 
    data_1day = data_1day.sort_values(by = ['datetime','volume'])
    #the index is the datetime 
    data_1day.index = data_1day.datetime
    #the last's volume is the largest, so only keep the last 
    data_1day = data_1day.resample('1D',closed ='left',label='left').apply('last')
    #drop the missing data 
    data_1day = data_1day.dropna()
    #only keep the datetime and the name 
    dom_future = data_1day[['name','datetime']]
    #sort the dom_future_temp according to the name and date 
    #因为主力合约是根据成交量来算，认为一旦切换主力合约，那么就不再切换回之前的主力合约
    dom_future_temp = dom_future.sort_values(by=['name','datetime'])
    dom_future_temp = dom_future_temp.groupby(by = ['name']).head(1)
    #get the day bar that only have dominant future 
    dom_future = pd.merge(dom_future[['datetime']],dom_future_temp,on ='datetime',how='outer')
    dom_future  = dom_future.fillna(method = 'ffill')
    db.dom_future.insert_many(dom_future.T.to_dict().values())
    #calculate the adj_factor(olhc)
    #merge the dom_future and the olhc
    dom_future_temp2 = pd.merge(dom_future,data_1day[['close','high','low','open','datetime','name']],on = ['datetime','name'],how = 'outer')
    #calculate the adj factor, only keep the adj factor that we change the domimant future 
    dom_future_temp3 = dom_future_temp2[['close','open','low','high']]/dom_future_temp2[['close','open','low','high']].shift(1)
    dom_future_temp3[['datetime','name']]= dom_future_temp2[['datetime','name']]
    dom_future_temp3 = pd.merge(dom_future_temp3,dom_future_temp,how='inner')
    #the first adj factor is 1 
    dom_future_temp3 = dom_future_temp3.fillna(1) 
    #merge the fdajfactor and the domfuture 
    adj_factor = pd.merge(dom_future_temp3,dom_future,on=['datetime','name'],how = 'outer')
    adj_factor = adj_factor.sort_values(by='datetime').fillna(method='ffill')
    adj_factor = adj_factor.reset_index(drop=True)
    #save the adjfactor in mongodb
    db.adj_factor.insert_many(adj_factor.T.to_dict().values())


#the fucntion is used to save the dominant future 
#dbname: the database name to put the domfuture data in 
#dom_db_name: the database name that keep the date and name of dominant future 
def get_dom_data(db_name,dom_db_name,client,vtsymbol,dir):
    #db: the database that keep the date and name of the dominant future 
    db = client[dom_db_name]
    #the collection where the dominant future data save 
    collection = client[db_name]['dom_future']
    #data_1day old is the date and name of the dominant future 
    data_1day_old = collection.find()
    #transfer the data to data frame
    data_1day_old = pd.DataFrame(list(data_1day_old))
    #the index of the data_1day_old is the datetime 
    data_1day_old.index = data_1day_old.datetime
    #form the list of the data name 
    data_name = [db_name+'_'+'tick',db_name+'_'+'1min',db_name+'_'+'3min',db_name+'_'+'5min',db_name+'_'+'10min',db_name+'_'+'1day']
    #if the db_name is TF/IF, execute eval1
    eval1 = ("db_name in root")
    #if the dbname is T, execute eval2
    eval2 = ("db_name in root and root[root.find('T')+1] != 'F'")
    #judge execute eval1/ eval2 
    if len(db_name) == 1:
        eval_temp = eval2
    else:
        eval_temp = eval1
    #read the txt and save the dominant data 
    for root, dirs, files in os.walk(dir):
        if files:
            for f in files:
                if 'Survey' not in f:
                    #execute eval1 or eval2  
                    if eval(eval_temp):
                        #get the year month day from the root 
                        year =  root[(-12-len(db_name)):(-8-len(db_name))]
                        month = root[-4:-2]
                        day = root[-2:]
                        #get the name from the f 
                        name = f[0:4+len(db_name)]
                        #form the date from year month and day
                        date = datetime.strptime(year+month+day,'%Y%m%d')
                        #get the dom_name of the date 
                        dom_name = data_1day_old.ix[date,'name']
                        #judge whether the name is the same as the dom_name 
                        #if the same, read the txt
                        if name == dom_name:
                            #get the dir
                            file_add =  root +'\\'+ f
                            #read the txt 
                            data = pd.read_table(file_add,sep =',')
                            #add the name and the vtsymbol column
                            data['symbol'] = name
                            data['vtSymbol'] = vtsymbol
                            #del the tradingday instrumentID updatetime Volume columns
                            if 'TradingDay' in data.columns:
                                del data['TradingDay']
                            if 'InstrumentID' in data.columns:
                                del data['InstrumentID']
                            if 'UpdateTime' in data.columns:
                                data['Time'] = data['UpdateTime']
                                del data['UpdateTime']
                            if 'Volume' in data.columns:
                                data['TradeVolume'] = data['Volume']
                                del data['Volume']
                            #add the datetime date time columns 
                            data['datetime'] = year + '-' + month + '-' + day +' ' + data['Time']
                            data['datetime'] = pd.to_datetime(data['datetime'])
                            data['date'] = year+month+day 
                            data['time'] = data['Time']
                            data.index = data['datetime']
                            #delete the Time oolumns
                            del data['Time']
                            #form the olhc dict 
                            ohlc_dict = { 'open':'first','high':'max', 'low':'min','close': 'last','volume': 'sum'} 
                            #form the 1 min bar data 
                            data_1min =data['LastPrice'].resample('1T', closed='left', label='left').apply(ohlc_dict)
                            data_1min['volume'] = data['TradeVolume'].resample('1T',closed='left', label='left').sum()
                            data_1min['symbol'] = name
                            data_1min['vtSymbol'] = vtsymbol
                            data_1min['date'] = year+month+day
                            data_1min['openInterest'] =''
                            data_1min['exchange'] = ''
                            #delete the missing data
                            data_1min = data_1min[data_1min['open'].notnull()]
                            #form the 3 min bar 
                            data_3min = data_1min.resample('3T', closed='left', label='left').apply(ohlc_dict)
                            data_3min['symbol'] = name
                            data_3min['vtSymbol'] = vtsymbol
                            data_3min['date'] = year + month + day
                            data_3min['openInterest'] =''
                            data_3min['exchange'] = ''
                            data_3min = data_3min[data_3min['open'].notnull()]
                            #form the 5 min bar 
                            data_5min = data_1min.resample('5T', closed='left', label='left').apply(ohlc_dict)
                            data_5min['symbol'] = name
                            data_5min['vtSymbol'] = vtsymbol
                            data_5min['date'] = year + month + day
                            data_5min['openInterest'] = ''
                            data_5min['exchange'] = ''
                            data_5min = data_5min[data_5min['open'].notnull()]
                            #form the 10 min bar 
                            data_10min = data_1min.resample('10T', closed='left', label='left').apply(ohlc_dict)
                            data_10min['name'] = name
                            data_10min['vtSymbol'] = vtsymbol
                            data_10min['date'] = year + month + day
                            data_10min['exchange'] = ''
                            data_10min['openInterest'] = ''
                            data_10min = data_10min[data_10min['open'].notnull()]
                            #form the 1 day bar 
                            data_1day = data_10min.resample('1D', closed='left', label='left').apply(ohlc_dict)
                            data_1day['symbol'] = name
                            data_1day['vtSymbol'] =vtsymbol
                            data_1day['date'] = year + month + day 
                            data_1day['openInterest'] = ''
                            data_1day['exchange']  = ''
                            data_1day = data_1day[data_1day['open'].notnull()]
                            data = data.reset_index(drop=True)
                            #save the bar data in the mongodb
                            db[data_name[0]].insert_many(data.T.to_dict().values())
                            #add the datetime column and drop the index 
                            data_1min['datetime'] = data_1min.index
                            data_1min = data_1min.reset_index(drop=True)
                            #save the 1 min bar data in the mongodb
                            db[data_name[1]].insert_many(data_1min.T.to_dict().values())
                            #add the datetime column and drop the index 
                            data_3min['datetime'] = data_3min.index
                            data_3min = data_3min.reset_index(drop=True)
                            #save the 3 min bar data in the mongodb
                            db[data_name[2]].insert_many(data_3min.T.to_dict().values())
                            #add the datetime column and drop the index 
                            data_5min['datetime'] = data_5min.index
                            data_5min = data_5min.reset_index(drop=True)
                            #save the 5 min bar data in the mongo db
                            db[data_name[3]].insert_many(data_5min.T.to_dict().values())
                            #add the datetime column and drop the index 
                            data_10min['datetime'] = data_10min.index
                            data_10min = data_10min.reset_index(drop=True)
                            #save the 10 min bar data in the mongodb
                            db[data_name[4]].insert_many(data_10min.T.to_dict().values())
                            #add the datetime column and drop the index 
                            data_1day['datetime'] = data_1day.index
                            data_1day = data_1day.reset_index(drop=True)
                            #save the day bar data in the mongodb
                            db[data_name[5]].insert_many(data_1day.T.to_dict().values())



#the function is to align two dominant future data
#如果策略在两个合约上进行，必须传入对齐的两个合约的数据
#startdate 
#enddate 
#dbname: 
#vtsymbol1: dbname of one future
#vtsymbol2: dbname of another future 
def align_data(startdate,endate, dbname, vtsymbol1, vtsymbol2):
    #transfer the string to datetime 
    datastartdate = datetime.strptime(startdate, '%Y%m%d')
    dataenddate = datetime.strptime(enddate,'%Y%m%d')
    #flt is used to get data from collection
    flt = {'datetime':{'$gte':datastartdate,
                        '$lt':dataenddate}}
    #get the mongodb client 
    client = pymongo.MongoClient('localhost', 27017)
    #get two collection that keep the dominant future data 
    collection1  = client[dbname][vtsymbol1]
    collection2 = client[dbname][vtsymbol2]
    #get data from the collection
    data1 = collection1.find(flt)
    data2 = collection2.find(flt)
    #transfer to dataframe 
    data1 = pd.DataFrame(list(data1))
    data2 = pd.DataFrame(list(data2))
    #get intersection of the datetime of the two dataframe 
    datetime_all = set(data2['datetime']) & set(data1['datetime'])
    #delete the row, if the datetime not in the intersection 
    for i in range(0,len(data1)):
        if data1.ix[i,'datetime'] not  in datetime_all:
            data1 = data1.drop(i)
    #drop the duplicates of data1
    data1 = data1.drop_duplicates('datetime',keep = 'first')
    #delete the row if the datetime not in the intersection
    for i in range(0,len(data2)):
        if data2.ix[i,'datetime'] not in datetime_all:
            data2 = data2.drop(i)
    #drop the duplicates of data2 
    data2 = data2.drop_duplicates('datetime',keep = 'first')
    #the 'dom_future2' is to save the aligned data 
    dbname1 = 'dom_future2'
    db = client[dbname1]
    #del the _id column
    del data1['_id'] 
    del data2['_id']
    #sort data1 and data2 according to datetime 
    data1 = data1.sort_values(by = 'datetime')
    data2 = data2.sort_values(by = 'datetime')
    #drop the index 
    data1 =  data1.reset_index(drop = True)
    data2 = data2.reset_index(drop = True)
    #save data1 and data2 in the mongodb----dom_future2 db, collection namme is vtsymbol1 and vtsymbol2
    db[vtsymbol1].insert_many(data1.T.to_dict().values())
    db[vtsymbol2].insert_many(data2.T.to_dict().values()) 



#test save the raw data 
if __name__ == "__main__":
    client = pymongo.MongoClient('localhost', 27017)
    dir = r'F:\tickdata\ctadata'
    db_name = 'TF'
    save_rawdata(dir, db_name, client)
    db_name = 'T'
    save_rawdata(dir, db_name, client)
    
    db_name = 'IF'
    save_rawdata(dir, db_name, client)
    
    
    #test get the dominant future and the adj factor
    from time import time
    start=time()
    client = pymongo.MongoClient('localhost', 27017)
    get_dom_future(client,'TF','TF_1day')
    get_dom_future(client,'IF','IF_1day')
    get_dom_future(client,'T','T_1day')
    # print 'total time is %s分钟'%(time()-start)
    
    #test only save the dominant future and add the vtsymbol
    from time import time
    start=time()
    client = pymongo.MongoClient('localhost', 27017)
    dir = r'F:\tickdata\ctadata'
    dom_db_name = 'dom_future'
    db_name ='TF'
    get_dom_data(db_name,dom_db_name,client,'TF0000',dir)
    dom_db_name = 'dom_future'
    db_name ='T'
    get_dom_data(db_name,dom_db_name,client,'T0000',dir)
    # print 'total time is %s秒'%(time()-start)
    dom_db_name = 'dom_future'
    db_name ='IF'
    get_dom_data(db_name,dom_db_name,client,'IF0000',dir)
    
    #get the dominant future aligned
    startdate = '20160101'
    enddate = '20161230'
    dbname = 'dom_future'
    vtsymbol1 = 'TF_10min'
    vtsymbol2 = 'T_10min'
    align_data(startdate,enddate, dbname, vtsymbol1, vtsymbol2)
                            
