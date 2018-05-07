
EXT_Wind_User = 'fe'
EXT_Wind_Password = 'fe'
EXT_Wind_Link = '192.168.100.22:1521/winddb'
EXT_Path = 'C:\\Users\\user\\GitHub\\Project1\\out.hdf5'
##Saving Structure: datatype
EXT_Rawdata = 'RawData'
EXT_Stitch = 'Stitch'
EXT_Indicator = 'Indicator'
##Saving location:loca
EXT_Period = 'Period'
EXT_Rule = 'Rule'
## future series Num
EXT_Series_0 = '00'
EXT_Series_1 = '01'
EXT_Period_1 = '1d'
# code of exchange
EXT_EXCHANGE_CFE  = 'CFE'  #Zhong Jin Suo
EXT_EXCHANGE_SHFE = 'SHF' #shang hai Suo
EXT_EXCHANGE_DCE  = 'DCE'  #Da Lian Suo
EXT_EXCHANGE_CZCE = 'CZC' #ZhengZhou Suo
# code for Commodity/finance Future in CFE
EXT_CFE_IF = 'IF'
EXT_CFE_IH = 'IH'
EXT_CFE_IC = 'IC'
EXT_CFE_T  = 'T'
EXT_CFE_TF = 'TF'
EXT_CFE_STOCK = ['IF','IC','IH']
EXT_CFE_BOND = ['TF','T']
EXT_CFE_ALL = EXT_CFE_STOCK + EXT_CFE_BOND
# code for Commodity/finance Future in SHFE
EXT_SHFE_CU = 'CU'
EXT_SHFE_AL = 'AL'
EXT_SHFE_ZN = 'ZN'
EXT_SHFE_RU = 'RU'
EXT_SHFE_FU = 'FU'
EXT_SHFE_AU = 'AU'
EXT_SHFE_AG = 'AG'
EXT_SHFE_RB = 'RB'
EXT_SHFE_WR = 'WR'
EXT_SHFE_PB = 'PB'
EXT_SHFE_BU = 'BU'
EXT_SHFE_HC = 'HC'
EXT_SHFE_NI = 'NI'
EXT_SHFE_SN = 'SN'
EXT_SHFE_ALL = ['CU','AL','ZN','RU','AU','AG','RB','WR','PB','BU','HC','NI','SN'] #'FU'
# code for Commodity/finance Future in DCE
EXT_DCE_A = 'A'
EXT_DCE_B = 'B'
EXT_DCE_M = 'M'
EXT_DCE_C = 'C'
EXT_DCE_Y = 'Y'
EXT_DCE_P = 'P'
EXT_DCE_L = 'L'
EXT_DCE_V = 'V'
EXT_DCE_J = 'J'
EXT_DCE_I = 'I'
EXT_DCE_JM = 'JM'
EXT_DCE_JD = 'JD'
EXT_DCE_FB = 'FB'
EXT_DCE_BB = 'BB'
EXT_DCE_PP = 'PP'
EXT_DCE_CS = 'CS'
EXT_DCE_ALL = ['A','B','M','C','Y','P','L','V','J','I','JM','JD','FB','BB','PP','CS']
# code for Commodity/finance Future in CZCE
EXT_CZCE_PM = 'PM'
EXT_CZCE_WH = 'WH'
EXT_CZCE_CF = 'CF'
EXT_CZCE_SR = 'SR'
EXT_CZCE_OI = 'OI'
EXT_CZCE_TA = 'TA'
EXT_CZCE_RI = 'RI'
EXT_CZCE_LR = 'LR'
EXT_CZCE_MA = 'MA'
EXT_CZCE_FG = 'FG'
EXT_CZCE_RS = 'RS'
EXT_CZCE_RM = 'RM'
EXT_CZCE_TC = 'TC'
EXT_CZCE_ZC = 'ZC'
EXT_CZCE_JR = 'JR'
EXT_CZCE_SF = 'SF'
EXT_CZCE_SM = 'SM'
EXT_CZCE_ALL = ['PM','WH','CF','SR','OI','TA','RI','LR','MA','FG','RS','RM','TC','ZC','JR','SF','SM']

# Wind filename
EXT_CFE_STOCK_FILE = 'filesync.CIndexFuturesEODPrices'
EXT_CFE_BOND_FILE = 'filesync.CBondFuturesEODPrices'
EXT_SHFE_DATA_FILE = 'filesync.CCommodityFuturesEODPrices'
EXT_DCE_DATA_FILE = 'filesync.CCommodityFuturesEODPrices'
EXT_CZCE_DATA_FILE = 'filesync.CCommodityFuturesEODPrices'
EXT_Delistdate_File = 'filesync.CFuturesDescription'

EXT_In_Header = 'trade_dt,s_info_windcode,s_dq_presettle,s_dq_open,s_dq_high,s_dq_low,s_dq_close,s_dq_settle,s_dq_volume,s_dq_oi'
EXT_In_Header2 = 's_info_windcode,s_info_delistdate'
EXT_In_Date = 'trade_dt'
EXT_In_Asset = 's_info_windcode'
EXT_In_Delistdate = 's_info_delistdate'

EXT_Out_Header = 'Date,Asset,PreSettle,Open,High,Low,Close,Settle,Volume,OpenInterest'
EXT_Out_Header2 = 'Asset,Delistdate'
EXT_Out_Date = 'Date'
EXT_Out_Asset = 'Asset'
EXT_Out_AdjFactor = 'AdjFactor'
EXT_Out_Close = 'Close'
EXT_Out_OpenInterest = 'OpenInterest'
EXT_Out_Delistdate = 'Delistdate'
