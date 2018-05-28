import talib
from dataUlt import *

# -----------------------TA_Lib documents Page 1------------------------------
# ----------------------------Overlap Studies --------------------------------

def bbands(data):
    for i in param['bbands']['timeperiod']:
        for j in param['bbands']['nbdevup']:
            for k in param['bbands']['nbdevdn']:
                for l in param['bbands']['matype']:
                    data['_'.join(['upperband',str(i),str(j),str(k),str(l)])], \
                    data['_'.join(['middleband',str(i),str(j),str(k),str(l)])], \
                    data['_'.join(['lowerband',str(i),str(j),str(k),str(l)])] \
                        = talib.BBANDS(data[EXT_Bar_Close].values, timeperiod = i,
                                       nbdevup = j, nbdevdn = k, matype = l)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def dema(data):
    for i in param['dema']['timeperiod']:
        data['_'.join(['dema',str(i)])] = talib.DEMA(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ema(data):
    for i in param['ema']['timeperiod']:
        data['_'.join(['ema',str(i)])] = talib.EMA(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ht_trendline(data):
    data['ht_trendline'] = talib.HT_TRENDLINE(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def kama(data):
    for i in param['kama']['timeperiod']:
        data['_'.join(['kama',str(i)])] = talib.KAMA(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ma(data):
    for i in param['ma']['timeperiod']:
        data['_'.join(['ma',str(i)])] = talib.MA(data[EXT_Bar_Close].values,
                                                 timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def mama(data):
    ''' fastLimit : Upper limit use in the adaptive algorithm.
        slowLimit : Lower limit use in the adaptive algorithm.
        Valid range from 0.01 to 0.99.'''
    for i in param['mama']['fastlimit']:
        for j in param['mama']['slowlimit']:
            data['_'.join(['mama',str(i),str(j)])], \
            data['_'.join(['fama',str(i),str(j)])] \
                = talib.MAMA(data[EXT_Bar_Close].values,
                             fastlimit = i, slowlimit = j)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# real = MAVP(close, periods, minperiod=2, maxperiod=30, matype=0)

def midpoint(data):
    for i in param['midpoint']['timeperiod']:
        data['_'.join(['midpoint',str(i)])] \
            = talib.MIDPOINT(data[EXT_Bar_Close].values,timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def midprice(data):
    for i in param['midprice']['timeperiod']:
        data['_'.join(['midprice',str(i)])] \
            = talib.MIDPRICE(data[EXT_Bar_High].values, data[EXT_Bar_Low].values,
                             timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def sar(data):
    for i in param['sar']['acceleration']:
        for j in param['sar']['maximum']:
            data['_'.join(['sar',str(i),str(j)])] \
                = talib.SAR(data[EXT_Bar_High].values, data[EXT_Bar_Low].values,
                            acceleration = i, maximum = j)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def sarext(data):
    for i in param['sarext']['startvalue']:
        for j in param['sarext']['offsetonreverse']:
            for k in param['sarext']['accelerationinitlong']:
                for l in param['sarext']['accelerationlong']:
                    for m in param['sarext']['accelerationmaxlong']:
                        for n in param['sarext']['accelerationinitshort']:
                            for o in param['sarext']['accelerationshort']:
                                for p in param['sarext']['accelerationmaxshort']:
                                    data['_'.join(['sarext',str(i),str(j),
                                                   str(k),str(l),str(m),
                                                   str(n),str(o),str(p)])] \
                                        = talib.SAR(data[EXT_Bar_High].values,
                                                    data[EXT_Bar_Low].values,
                                                    startvalue = i,
                                                    offsetonreverse = j,
                                                    accelerationinitlong = k,
                                                    accelerationlong = l,
                                                    accelerationmaxlong = m,
                                                    accelerationinitshort = n,
                                                    accelerationshort = o,
                                                    accelerationmaxshort = p)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def sma(data):
    for i in param['sma']['timeperiod']:
        data['_'.join(['sma',str(i)])] = talib.SMA(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def t3(data):
    for i in param['t3']['timeperiod']:
        for j in param['t3']['vfactor']:
            data['_'.join(['t3',str(i),str(j)])] \
                = talib.T3(data[EXT_Bar_Close].values,
                            timeperiod = i,
                            vfactor = j)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def tema(data):
    for i in param['tema']['timeperiod']:
        data['_'.join(['tema',str(i)])] = talib.TEMA(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def trima(data):
    for i in param['trima']['timeperiod']:
        data['_'.join(['trima',str(i)])] = talib.TRIMA(data[EXT_Bar_Close].values,
                                                       timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def wma(data):
    for i in param['wma']['timeperiod']:
        data['_'.join(['wma',str(i)])] = talib.WMA(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 2------------------------------
# --------------------------Momentum Indicators-------------------------------

def adx(data):
    for i in param['adx']['timeperiod']:
        data['_'.join(['adx',str(i)])] = talib.ADX(data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def adxr(data):
    for i in param['adxr']['timeperiod']:
        data['_'.join(['adxr',str(i)])] = talib.ADXR(data[EXT_Bar_High].values,
                                                     data[EXT_Bar_Low].values,
                                                     data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def apo(data):
    for i in param['apo']['fastperiod']:
        for j in param['apo']['slowperiod']:
            for k in param['apo']['matype']:
                data['_'.join(['apo',str(i),str(j),str(k)])] \
                    = talib.APO(data[EXT_Bar_Close].values,
                                fastperiod = i, slowperiod = j, matype = k)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def aroon(data):
    for i in param['aroon']['timeperiod']:
        data['_'.join(['aroondown',str(i)])], \
        data['_'.join(['aroonup',str(i)])] \
            = talib.AROON(data[EXT_Bar_High].values, data[EXT_Bar_Low].values,
                          timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def aroonosc(data):
    for i in param['aroonosc']['timeperiod']:
        data['_'.join(['aroonosc',str(i)])] \
            = talib.AROONOSC(data[EXT_Bar_High].values,
                             data[EXT_Bar_Low].values,
                             timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def bop(data):
    data['bop'] = talib.BOP(data[EXT_Bar_Open].values, data[EXT_Bar_High].values,
                            data[EXT_Bar_Low].values, data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def cci(data):
    for i in param['cci']['timeperiod']:
        data['_'.join(['cci',str(i)])] \
            = talib.CCI(data[EXT_Bar_High].values, data[EXT_Bar_Low].values,
                        data[EXT_Bar_Close].values, timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def cmo(data):
    for i in param['cmo']['timeperiod']:
        data['_'.join(['cmo',str(i)])] = talib.CMO(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def dx(data):
    for i in param['dx']['timeperiod']:
        data['_'.join(['dx',str(i)])] \
            = talib.CCI(data[EXT_Bar_High].values, data[EXT_Bar_Low].values,
                        data[EXT_Bar_Close].values, timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def macd(data):
    for i in param['macd']['fastperiod']:
        for j in param['macd']['slowperiod']:
            for k in param['macd']['signalperiod']:
                data['_'.join(['macd',str(i),str(j),str(k)])], \
                data['_'.join(['macdsignal',str(i),str(j),str(k)])], \
                data['_'.join(['macdhist',str(i),str(j),str(k)])] \
                    = talib.MACD(data[EXT_Bar_Close].values, fastperiod = i,
                                 slowperiod = j, signalperiod = k)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def macdext(data):
    for i in param['macdext']['fastperiod']:
        for j in param['macdext']['fastmatype']:
            for k in param['macdext']['slowperiod']:
                for l in param['macdext']['slowmatype']:
                    for m in param['macdext']['signalperiod']:
                        for n in param['macdext']['signalmatype']:
                            data['_'.join(['macd',str(i),str(j),str(k),
                                           str(l),str(m),str(n)])], \
                            data['_'.join(['macdsignal',str(i),str(j),str(k),
                                           str(l),str(m),str(n)])], \
                            data['_'.join(['macdhist',str(i),str(j),str(k),
                                           str(l),str(m),str(n)])] \
                                = talib.MACDEXT(data[EXT_Bar_Close].values,
                                                fastperiod = i, fastmatype = j,
                                                slowperiod = k, slowmatype = l,
                                                signalperiod = m, signalmatype = n)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def macdfix(data):
    for i in param['macdfix']['signalperiod']:
        data['_'.join(['macd',str(i)])], \
        data['_'.join(['macdsignal',str(i)])], \
        data['_'.join(['macdhist',str(i)])] \
            = talib.MACDFIX(data[EXT_Bar_Close].values, signalperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def MFI(data):
    for i in params['mfi']['timeperiod']:
        data['_'.join(['mfi',str(i)])] = talib.MFI(data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   data[EXT_Bar_Close].values,
                                                   data[EXT_Bar_Volume].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def minus_di(data):
    for i in params['minus_di']['timeperiod']:
        data['_'.join(['minus_di',str(i)])] = talib.MINUS_DI(
                                                   data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def minus_dm(data):
    for i in params['minus_dm']['timeperiod']:
        data['_'.join(['minus_dm',str(i)])] = talib.MINUS_DM(
                                                   data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def mom(data):
    for i in params['mom']['timeperiod']:
        data['_'.join(['mom',str(i)])] = talib.MOM(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def plus_di(data):
    for i in params['plus_di']['timeperiod']:
        data['_'.join(['plus_di',str(i)])] = talib.PLUS_DI(
                                                   data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def plus_dm(data):
    for i in params['plus_dm']['timeperiod']:
        data['_'.join(['plus_dm',str(i)])] = talib.PLUS_DM(
                                                   data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ppo(data):
    for i in param['ppo']['fastperiod']:
        for j in param['ppo']['slowperiod']:
            for k in param['ppo']['matype']:
                data['_'.join(['pop',str(i),str(j),str(k)])] \
                    = talib.POP(data[EXT_Bar_Close].values, fastperiod = i,
                                 slowperiod = j, matype = k)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def roc(data):
    for i in param['roc']['timeperiod']:
        data['_'.join(['roc',str(i)])] = talib.ROC(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def rocp(data):
    for i in param['rocp']['timeperiod']:
        data['_'.join(['rocp',str(i)])] = talib.ROCP(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def rocr(data):
    for i in param['rocr']['timeperiod']:
        data['_'.join(['rocr',str(i)])] = talib.ROCR(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def rocr100(data):
    for i in param['rocr100']['timeperiod']:
        data['_'.join(['rocr100',str(i)])] = talib.ROCR100(
                                                data[EXT_Bar_Close].values,
                                                timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def rsi(data):
    for i in param['rsi']['timeperiod']:
        data['_'.join(['rsi',str(i)])] = talib.RSI(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def stoch(data):
    for i in param['stoch']['fastk_period']:
        for j in param['stoch']['slowk_period']:
            for k in param['stoch']['slowk_matype']:
                for l in param['stoch']['slowd_period']:
                    for m in param['stoch']['slowd_matype']:
                        data['_'.join(['slowk',str(i),str(j),str(k),
                                       str(l),str(m)])], \
                        data['_'.join(['slowd',str(i),str(j),str(k),
                                       str(l),str(m)])] \
                            = talib.STOCH(data[EXT_Bar_High].values,
                                          data[EXT_Bar_Low].values,
                                          data[EXT_Bar_Close].values,
                                          fastk_period = i, slowk_period = j,
                                          slowk_matype = k, slowd_period = l,
                                          slowd_matype = m)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def stochf(data):
    for i in param['stochf']['fastk_period']:
        for j in param['stochf']['fastd_period']:
            for k in param['stochf']['fastd_matype']:
                data['_'.join(['fastk',str(i),str(j),str(k)])], \
                data['_'.join(['fastd',str(i),str(j),str(k)])] \
                    = talib.STOCHF(data[EXT_Bar_High].values,
                                   data[EXT_Bar_Low].values,
                                   data[EXT_Bar_Close].values,
                                   fastk_period = i, slowd_period = j,
                                   fastd_matype = k)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def stochrsi(data):
    for i in param['stochrsi']['timeperiod']:
        for j in param['stochrsi']['fastk_period']:
            for k in param['stochrsi']['fastd_period']:
                for l in param['stochrsi']['fastd_matype']:
                    data['_'.join(['fastk',str(i),str(j),str(k),str(l)])], \
                    data['_'.join(['fastd',str(i),str(j),str(k),str(l)])] \
                        = talib.STOCHRSI(data[EXT_Bar_Close].values,
                                         timeperiod = i, fastk_period = j,
                                         fastd_period = k, fastd_matype = l)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def trix(data):
    for i in param['trix']['timeperiod']:
        data['_'.join(['trix',str(i)])] = talib.TRIX(data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ultosc(data):
    for i in param['ultosc']['timeperiod1']:
        for j in param['ultosc']['timeperiod2']:
            for k in param['ultosc']['timeperiod3']:
                data['_'.join(['ultosc',str(i),str(j),str(k)])] \
                    = talib.ULTOSC(data[EXT_Bar_High].values,
                                   data[EXT_Bar_Low].values,
                                   data[EXT_Bar_Close].values,
                                   timeperiod1 = i,
                                   timeperiod2 = j,
                                   timeperiod3 = k)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def willr(data):
    for i in param['willr']['timeperiod']:
        data['_'.join(['willr',str(i)])] \
            = talib.WILLR(data[EXT_Bar_High].values,
                          data[EXT_Bar_Low].values,
                          data[EXT_Bar_Close].values,
                          timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 3------------------------------
# --------------------------Volume Indicators---------------------------------

def ad(data):
    data['ad'] = talib.AD(data[EXT_Bar_High].values, data[EXT_Bar_Low].values,
                          data[EXT_Bar_Close].values, data[EXT_Bar_Volume].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def adosc(data):
    for i in param['adosc']['fastperiod']:
        for j in param['adosc']['slowperiod']:
            data['_'.join(['ultosc',str(i),str(j)])] \
                = talib.ADOSC(data[EXT_Bar_High].values,
                              data[EXT_Bar_Low].values,
                              data[EXT_Bar_Close].values,
                              data[EXT_Bar_Volume].values,
                              fastperiod = i,
                              slowperiod = j)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def obv(data):
    data['obv'] = talib.OBV(data[EXT_Bar_Close].values,
                            data[EXT_Bar_Volume].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 4------------------------------
# ----------------------------Cycle Indicator---------------------------------

def ht_dcperiod(data):
    data['ht_dcperiod'] = talib.HT_DCPERIOD(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ht_dcphase(data):
    data['ht_dcphase'] = talib.HT_DCPHASE(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ht_phasor(data):
    data['inphase'], data['quadrature'] = talib.HT_PHASOR(
                                            data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ht_sine(data):
    data['sine'], data['leadsine'] = talib.HT_SINE(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ht_trendmode(data):
    data['ht_trendmode'] = talib.HT_TRENDMODE(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 5------------------------------
# ----------------------------Price Transform---------------------------------

def avgprice(data):
    data['avgprice'] = talib.AVGPRICE(
                            data[EXT_Bar_Open].values,
                            data[EXT_Bar_High].values,
                            data[EXT_Bar_Low].values,
                            data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def medprice(data):
    data['medprice'] = talib.MEDPRICE(
                            data[EXT_Bar_High].values,
                            data[EXT_Bar_Low].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def typprice(data):
    data['typprice'] = talib.TYPPRICE(
                            data[EXT_Bar_High].values,
                            data[EXT_Bar_Low].values,
                            data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def wclprice(data):
    data['wclprice'] = talib.WCLPRICE(
                            data[EXT_Bar_High].values,
                            data[EXT_Bar_Low].values,
                            data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 6------------------------------
# ------------------------Volatility Indicators-------------------------------
def atr(data):
    for i in param['atr']['timeperiod']:
        data['_'.join(['atr',str(i)])] = talib.ATR(data[EXT_Bar_High].values,
                                                   data[EXT_Bar_Low].values,
                                                   data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def natr(data):
    for i in param['natr']['timeperiod']:
        data['_'.join(['natr',str(i)])] = talib.NATR(data[EXT_Bar_High].values,
                                                     data[EXT_Bar_Low].values,
                                                     data[EXT_Bar_Close].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def trange(data):
    data['trange'] = talib.TRANGE(data[EXT_Bar_High].values,
                                  data[EXT_Bar_Low].values,
                                  data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 7------------------------------
# ------------------------Statistic Functions-------------------------------

def beta(data):
    for i in param['beta']['timeperiod']:
        data['_'.join(['beta',str(i)])] = talib.BETA(data[EXT_Bar_High].values,
                                                     data[EXT_Bar_Low].values,
                                                     timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def correl(data):
    for i in param['correl']['timeperiod']:
        data['_'.join(['correl',str(i)])] = talib.CORREL(
                                                data[EXT_Bar_High].values,
                                                data[EXT_Bar_Low].values,
                                                timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def linearreg(data):
    for i in param['linearreg']['timeperiod']:
        data['_'.join(['linearreg',str(i)])] = talib.LINEARREG(
                                                    data[EXT_Bar_Close].values,
                                                    timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def linearreg_angle(data):
    for i in param['linearreg_angle']['timeperiod']:
        data['_'.join(['linearreg_angle',str(i)])] \
            = talib.LINEARREG_ANGLE(data[EXT_Bar_Close].values,timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def linearreg_intercept(data):
    for i in param['linearreg_intercept']['timeperiod']:
        data['_'.join(['linearreg_intercept',str(i)])] \
            = talib.LINEARREG_INTERCEPT(data[EXT_Bar_Close].values,
                                        timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def linearreg_slope(data):
    for i in param['linearreg_slope']['timeperiod']:
        data['_'.join(['linearreg_slope',str(i)])] \
            = talib.LINEARREG_SLOPE(data[EXT_Bar_Close].values,timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def stddev(data):
    for i in param['stddev']['timeperiod']:
        for j in param['stddev']['nbdev']:
            data['_'.join(['stddev',str(i),str(j)])] \
                = talib.STDDEV(data[EXT_Bar_Close].values,
                               timeperiod = i, nbdev = j)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def tsf(data):
    for i in param['tsf']['timeperiod']:
        data['_'.join(['tsf',str(i)])] = talib.TSF(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def var(data):
    for i in param['var']['timeperiod']:
        for j in param['var']['nbdev']:
            data['_'.join(['stddev',str(i),str(j)])] \
                = talib.VAR(data[EXT_Bar_Close].values,
                            timeperiod = i, nbdev = j)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 7------------------------------
# ----------------------------Math Transform----------------------------------

def acos(data):
    data['acos'] = talib.ACOS(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def asin(data):
    data['asin'] = talib.ASIN(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def atan(data):
    data['atan'] = talib.ATAN(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ceil(data):
    data['ceil'] = talib.CEIL(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def cos(data):
    data['cos'] = talib.COS(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def cosh(data):
    data['cosh'] = talib.COSH(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def exp(data):
    data['exp'] = talib.EXP(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def floor(data):
    data['floor'] = talib.FLOOR(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def ln(data):
    data['ln'] = talib.LN(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def log10(data):
    data['log10'] = talib.LOG10(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def sin(data):
    data['sin'] = talib.SIN(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def sinh(data):
    data['sinh'] = talib.SINH(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def sqrt(data):
    data['sqrt'] = talib.SQRT(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def tan(data):
    data['tan'] = talib.TAN(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def tanh(data):
    data['tanh'] = talib.TANH(data[EXT_Bar_Close].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

# -----------------------TA_Lib documents Page 8------------------------------
# ----------------------------Math Operators----------------------------------

def add(data):
    data['add'] = talib.ADD(data[EXT_Bar_High].values, data[EXT_Bar_Low].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def div(data):
    data['div'] = talib.DIV(data[EXT_Bar_High].values, data[EXT_Bar_Low].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def max(data):
    for i in param['max']['timeperiod']:
        data['_'.join(['max',str(i)])] = talib.MAX(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def maxindex(data):
    for i in param['maxindex']['timeperiod']:
        data['_'.join(['maxindex',str(i)])] = talib.MAXINDEX(
                                                data[EXT_Bar_Close].values,
                                                timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def min(data):
    for i in param['min']['timeperiod']:
        data['_'.join(['min',str(i)])] = talib.MIN(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def minindex(data):
    for i in param['minindex']['timeperiod']:
        data['_'.join(['minindex',str(i)])] = talib.MININDEX(
                                                data[EXT_Bar_Close].values,
                                                timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def minmax(data):
    for i in param['minmax']['timeperiod']:
        data['_'.join(['min',str(i)])], data['_'.join(['max',str(i)])]  \
            = talib.MININDEX(data[EXT_Bar_Close].values, timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def minmaxindex(data):
    for i in param['minmaxindex']['timeperiod']:
        data['_'.join(['minidx',str(i)])], data['_'.join(['maxidx',str(i)])]  \
            = talib.MINMAXINDEX(data[EXT_Bar_Close].values, timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def mult(data):
    data['mult'] = talib.MULT(data[EXT_Bar_High].values, data[EXT_Bar_Low].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def sub(data):
    data['sub'] = talib.SUB(data[EXT_Bar_High].values, data[EXT_Bar_Low].values)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data

def sum(data):
    for i in param['sum']['timeperiod']:
        data['_'.join(['sum',str(i)])] = talib.SUM(data[EXT_Bar_Close].values,
                                                   timeperiod = i)
    data.drop(EXT_Del_Header.split(','),axis=1,inplace=True)
    return data
