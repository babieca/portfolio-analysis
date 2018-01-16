#!/usr/bin/python

# import modules

import sys
import argparse
from mysql_python import MysqlPython
from collections import OrderedDict
import datetime

VERSION = '0.0.1'
HOST = ''
USER = ''
PASSWORD = ''
DATABASE = ''

mysql_conn = MysqlPython(HOST, USER, PASSWORD, DATABASE)

def usage():
    print('Usage')

class Extender(argparse.Action):

    def __call__(self,parser,namespace,values,option_strings=None):

        # Need None here incase `argparse.SUPPRESS` was supplied for `dest`
        dest = getattr(namespace,self.dest,None)

        # print dest,self.default,values,option_strings
        if(not hasattr(dest,'extend') or dest == self.default):
            dest = []
            setattr(namespace,self.dest,dest)
            # if default isn't set to None, this method might be called
            # with the default as `values` for other arguements which
            # share this destination.
            parser.set_defaults(**{self.dest:None})

        try:
            dest.extend(values)
        except ValueError:
            dest.append(values)

        #another option:
        #if not isinstance(values,basestring):
        #    dest.extend(values)
        #else:
        #    dest.append(values) #It's a string.  Oops.

def eomonth(date0, n):
    from dateutil.relativedelta import relativedelta

    newDate = date0 + relativedelta(months=n+1)
    newDate = newDate.replace(day=1)
    newDate = newDate - datetime.timedelta (days = 1)

    return newDate

def dayNoWeekend(date0, n):
    newDate = date0
    while (newDate.isoweekday() == 6) or (newDate.isoweekday() == 7):        # Not Saturday or Sunday
        if n < 0:
            newDate = newDate - datetime.timedelta (days = 1)
        else:
            newDate = newDate + datetime.timedelta (days = 1)

    return newDate

def minimalist_xldate_as_datetime(xldate, datemode=0):
    # datemode: 0 for 1900-based, 1 for 1904-based
    return (
        datetime.datetime(1899, 12, 30)
        + datetime.timedelta(days=xldate + 1462 * datemode)
        )


def isExcelDate(s):
    try:
        float(s)
        return True
    except ValueError:
        return False
#############################################################################################################
def performance(args):

    if len(args) < 5:
        return "Error: Not enough parameters"

    ticker = args[0]   # Ticker
    field  = args[1]   # QTY, MVAL_BASE, PX_AVG_CCY, PX_AVG_BASE, RTN_BASE, RTN_BASE_CONST_CCY
    day0   = args[2]   # From
    day1   = args[3]   # To

    base_ccy = 'EUR'
    if len(args) >= 5:
        base_ccy = args[4]

    tblddbb = 'databasename.fundtrades'
    if len(args) >= 6:
        if args[5] == 'databasename.fundtrades' or args[5] == 'databasename.swaptrades':
            tblddbb = args[5]    # fundtrades/swaptrades table
        else:
            return "Error: please check table name (fundtrades / swaptrades)"

    v1p = 1             # Value of 1 point.
    px_avg_ccy = 0
    px_avg_base = 0
    perf_const_ccy = 0  # Performance constant currency
    perf_base = 0       # Performance base currency
    maxloops = 10       # To prevent an inf loop
    amnt = 0
    amnt0 = 0
    xrate = -1
    xrate1 = 1
    xrate2 = -1

    if isExcelDate(day0):
        day0 = minimalist_xldate_as_datetime(int(float(day0)))
    else:
        day0 = datetime.datetime.strptime(day0, '%Y-%m-%d')

    if isExcelDate(day1):
        day1 = minimalist_xldate_as_datetime(int(float(day1)))
    else:
        day1 = datetime.datetime.strptime(day1, '%Y-%m-%d')

    t_init = datetime.datetime.strptime("2015-07-31", "%Y-%m-%d")

    if dayNoWeekend(day0,-1).month==day0.month:
        firstDay=dayNoWeekend(day0,-1)
    else:
        firstDay=dayNoWeekend(day0,+1)

    firstPrevDay = dayNoWeekend(firstDay - datetime.timedelta (days = 1), -1)
    lastDay = dayNoWeekend(day1,-1)

    if lastDay < t_init:
        return "Error: lastDay < t_init"
    if (firstDay < datetime.datetime.strptime("2015-08-03", "%Y-%m-%d")) or \
        (firstPrevDay < datetime.datetime.strptime("2015-07-31", "%Y-%m-%d")):         # Monday Aug. 03, 2015 /  Friday July. 31, 2015
        firstDay = datetime.datetime.strptime("2015-08-03", "%Y-%m-%d")
        firstPrevDay = datetime.datetime.strptime("2015-07-31", "%Y-%m-%d")
    if lastDay > datetime.datetime.today():     # Today
        return "Error: lastDay"

    sql = "SELECT ccy FROM databasename.assets WHERE ticker = '" + ticker + "'"
    ccy = mysql_conn.selectone(sql, '#--')
    if ccy == '#--':
        return "Error: ccy"

    maxhist = mysql_conn.selectone("SELECT max(ha_date) FROM databasename.histassets WHERE ha_ticker = '" + ticker + "'", '#--')
    if maxhist == '#--':
        return "Error: max historical prices"

    maxhist = datetime.datetime.strptime(maxhist,"%Y-%m-%d")

    if firstDay > lastDay:
        return "Error: first day must be <= last day"

    # Check if the historical table for the asset and the currency if is not EUR are up to date.
    if maxhist < lastDay:
        if ccy != base_ccy:
            lastDayCcy = mysql_conn.selectone("SELECT max(ccy_date) FROM databasename.histccy WHERE ccy_name = '" + ccy + "'")
            lastDayCcy = datetime.datetime.strptime(lastDayCcy,'%Y-%m-%d')
            if lastDayCcy < lastDay:
                return "Error. Please update ccy exchange rates"
        return "Error. Please update historical prices"

    sectype = mysql_conn.selectone("SELECT sectype FROM databasename.assets WHERE ticker = '" + ticker + "'")
    if sectype == 'INDEX' and ticker != 'MCXP' and ticker != 'SCXP':
        v1p = float(mysql_conn.selectone("SELECT fut_valuepoint FROM databasename.assets WHERE ticker = '" + ticker + "'", '-1'))
        firstTradeDate = mysql_conn.selectone("SELECT fut_firsttradedate FROM databasename.assets WHERE ticker = '" + ticker + "'")
        if firstTradeDate == '#--':
            return "Error: firstTradeDate"
        firstTradeDate = datetime.datetime.strptime(firstTradeDate,'%Y-%m-%d')

        lastTradeDate = mysql_conn.selectone("SELECT fut_lasttradedate FROM databasename.assets WHERE ticker = '" + ticker + "'")
        if lastTradeDate == '#--':
            return "Error: lastTradeDate"
        lastTradeDate = datetime.datetime.strptime(lastTradeDate,'%Y-%m-%d')

        if firstTradeDate > firstDay:
            firstDay = firstTradeDate
            firstPrevDay = firstDay - datetime.timedelta (days = 1)
            firstPrevDay = dayNoWeekend(firstDay, -1)
        if lastTradeDate < lastDay:
            lastDay = lastTradeDate
    # End of index

    amnt0 = getNumShares([ticker, firstPrevDay.strftime("%Y-%m-%d"), tblddbb])

    sqlSTR = longsql(ticker, firstDay, lastDay, tblddbb)

    mysql_conn.select("SET @rownum := 0.0")
    mysql_conn.select("SET @val1ptn := 0.0")
    mysql_conn.select("SET @xrate := 0.0")
    mysql_conn.select("SET @prevtotal := 0.0")
    mysql_conn.select("SET @total := 0.0")
    mysql_conn.select("SET @pricegross := 0.0")
    mysql_conn.select("SET @pricenet := 0.0")
    mysql_conn.select("SET @prev_avgprice_ccy := 0.0")
    mysql_conn.select("SET @avgprice_ccy := 0.0")
    mysql_conn.select("SET @pricenet_base := 0.0")
    mysql_conn.select("SET @prev_avgprice_base := 0.0")
    mysql_conn.select("SET @avgprice_base := 0.0")
    mysql_conn.select("SET @prevperf_base := 0.0")
    mysql_conn.select("SET @perf_base := 0.0")
    mysql_conn.select("SET @prev_last_perf_base := 0.0")
    mysql_conn.select("SET @last_perf_base := 0.0")
    mysql_conn.select("SET @holding := '1970-01-01'")
    mysql_conn.select("SET @buynumshares := 0.0")
    mysql_conn.select("SET @sellnumshares := 0.0")

    # data[n x 21]
    # 0:  row_number
    # 1:  trd_filled
    # 2:  commission
    # 3:  val1point
    # 4:  xrate
    # 5:  prev_total_shares
    # 6:  total_shares
    # 7:  pricegross
    # 8:  pricenet
    # 9:  prev_avgprice_ccy
    # 10: avgprice_ccy
    # 11: pricenet_base
    # 12: prev_avgprice_base
    # 13: avgprice_base
    # 14: prevperf_base
    # 15: perf_base
    # 16: prev_last_perf_base
    # 17: last_perf_base
    # 18: Date of last entry of the position
    # 19: sum of all buy orders (pos. number)
    # 20: sum of all sell orders (neg. number)

    data = mysql_conn.select(sqlSTR)

    if type(data) != list:
        data = [data]

    entry_date= data[len(data)-1][18]



    def dvd(arg,x):
        #return 0
        date1=arg[0]
        if type(date1) != str:
            date1=date1.strftime("20%y-%m-%d")
        ticker=arg[1]
        #-p  "KWS LN"  "RTN_BASE" 42491 42521 "EUR"
        if len(arg)>2:
            date2=arg[2]
            if type(date2) != str:
                date2=date2.strftime("20%y-%m-%d")
        else:
            date2=datetime.date.today().strftime("20%y-%m-%d")

        dividend_table = mysql_conn.select("SELECT ticker, exdate, ccy, amount, comments FROM databasename.dividends where ccy != '#--' and amount != -1 and exdate >= '"+date1+"' and exdate <= '"+date2+"' and ticker = '"+ticker+"'")

        if type(dividend_table)!= list:
            dividend_table=[dividend_table]

        split=[]

        dividend_only=[]

        for all in dividend_table:
            if "STOCK SPLIT" in all:
                split.append(all)
            if "RETURN OF CAPITAL" in all or "REGULAR CASH" in all or "FINAL" in all or "2ND INTERIM" in all or "3RD INTERIM" in all or "1ST INTERIM" in all or "4TH INTERIM" in all or "ESTIMATED" in all or "SPECIAL CASH" in all or "INTERIM" in all:
                dividend_only.append(all)

        split.sort(key=lambda tup: tup[1])


        total_div=0
        total_div_ccy=0

        if dividend_only==[]:
            return 0

        for all in dividend_only:
            ccyxrate= mysql_conn.select("SELECT ccy_xrate FROM databasename.histccy where ccy_name= '"+ all[2] + "' and ccy_date= '"+all[1].strftime("20%y-%m-%d")+"'")
            #print("SELECT ccy_xrate FROM databasename.histccy where ccy_name= '"+ all[2] + "' and ccy_date= '"+all[1].strftime("20%y-%m-%d")+"'")
            if all[2]=="EUR":
                ccyxrate=1
            if ccyxrate==[]:
                continue
            if all[2]== "GBP" and (x==0 or x==2):
                ccyxrate*=100
            div_amount=all[3]
            for split in split:
                if all[1]<split[1]:
                    div_amount=float(all[3])/split[3]
            stock_currency=mysql_conn.select("SELECT ccy FROM databasename.assets where ticker='"+ticker+"'")
            if weight(ticker)>0:
                div_amount*=0.6



            amnt = getNumShares([ticker, (all[1]-datetime.timedelta(days=1)).strftime("20%y-%m-%d"), tblddbb])
            total_div+=amnt*float(div_amount)/ccyxrate



            if stock_currency != all[2]:
                xrate= mysql_conn.select("SELECT ccy_xrate FROM databasename.histccy where ccy_name= '"+ "GBP" + "' and ccy_date= '"+all[1].strftime("20%y-%m-%d")+"'")
                if stock_currency == "GBP":
                    div_amount=div_amount*xrate*100
                else:
                    div_amount=div_amount*xrate


            total_div_ccy+=amnt*float(div_amount)

        entry_date2=datetime.datetime.strptime(entry_date, "%Y-%m-%d")


        entry_date2=entry_date2-datetime.timedelta(days=1)


        sqlSTR_fordvd = longsql(ticker, firstDay, entry_date2, tblddbb)

        mysql_conn.select("SET @rownum := 0.0")
        mysql_conn.select("SET @val1ptn := 0.0")
        mysql_conn.select("SET @xrate := 0.0")
        mysql_conn.select("SET @prevtotal := 0.0")
        mysql_conn.select("SET @total := 0.0")
        mysql_conn.select("SET @pricegross := 0.0")
        mysql_conn.select("SET @pricenet := 0.0")
        mysql_conn.select("SET @prev_avgprice_ccy := 0.0")
        mysql_conn.select("SET @avgprice_ccy := 0.0")
        mysql_conn.select("SET @pricenet_base := 0.0")
        mysql_conn.select("SET @prev_avgprice_base := 0.0")
        mysql_conn.select("SET @avgprice_base := 0.0")
        mysql_conn.select("SET @prevperf_base := 0.0")
        mysql_conn.select("SET @perf_base := 0.0")
        mysql_conn.select("SET @prev_last_perf_base := 0.0")
        mysql_conn.select("SET @last_perf_base := 0.0")
        mysql_conn.select("SET @holding := '1970-01-01'")
        mysql_conn.select("SET @buynumshares := 0.0")
        mysql_conn.select("SET @sellnumshares := 0.0")

        # data[n x 21]
        # 0:  row_number
        # 1:  trd_filled
        # 2:  commission
        # 3:  val1point
        # 4:  xrate
        # 5:  prev_total_shares
        # 6:  total_shares
        # 7:  pricegross
        # 8:  pricenet
        # 9:  prev_avgprice_ccy
        # 10: avgprice_ccy
        # 11: pricenet_base
        # 12: prev_avgprice_base
        # 13: avgprice_base
        # 14: prevperf_base
        # 15: perf_base
        # 16: prev_last_perf_base
        # 17: last_perf_base
        # 18: Date of last entry of the position
        # 19: sum of all buy orders (pos. number)
        # 20: sum of all sell orders (neg. number)


        data_fordvd = mysql_conn.select(sqlSTR_fordvd)

        if type(data_fordvd) != list:
            data_fordvd = [data_fordvd]




        if data_fordvd==[]:
            beforeamount_long=0
            beforeamount_short=0
        else:
            beforeamount_long=data_fordvd[len(data_fordvd)-1][19]
            beforeamount_short=data_fordvd[len(data_fordvd)-1][20]




        if weight(ticker)>0:

            num_shares_to_divide= data[len(data)-1][19]- beforeamount_long
        else:

            num_shares_to_divide= data[len(data)-1][20]- beforeamount_short



        if x==0:
            return total_div
        if x==1:
            return total_div_ccy/num_shares_to_divide
        if x==2:
            return total_div/num_shares_to_divide

    #print(dvd((entry_date, ticker)))
    amnt            = float(data[len(data)-1][6])

    px_avg_ccy      = float(data[len(data)-1][10])-dvd((entry_date, ticker,day1),1)
    px_avg_base     = float(data[len(data)-1][13])-dvd((entry_date, ticker,day1),2)
    perf_base       = float(data[len(data)-1][15])+ dvd((firstDay,ticker,lastDay),0)        # Performance of a stock between two dates (d0, d1)
    last_perf_base  = float(data[len(data)-1][17])+ dvd((entry_date, ticker),0)      # Last performance since we enter in the company between two dates (d0,d1)

    # Get px and xrate for firstDay -1
    px_first_ccy = -1
    xrate1 = -1
    tmpdate = firstPrevDay
    counter = maxloops
    while (px_first_ccy == -1) or (xrate1 == -1):
        sql = "SELECT " + \
                "ha_price " + \
              "FROM " + \
                "databasename.histassets " + \
              "WHERE " + \
                "ha_ticker ='" + ticker + "' AND " +\
                "ha_date = '" + tmpdate.strftime("%Y-%m-%d") + "'"
        px_first_ccy = float(mysql_conn.selectone(sql, -1))

        xrate1 = 1
        if ccy != base_ccy:
            sql = "SELECT " + \
                "ccy_xrate " + \
              "FROM " + \
                "databasename.histccy " + \
              "WHERE " + \
                "ccy_name ='" + ccy + "' AND " +\
                "ccy_date = '" + tmpdate.strftime("%Y-%m-%d") + "'"
            xrate1 = float(mysql_conn.selectone(sql, -1))

        if px_first_ccy != -1 and xrate1 == -1:
            return "Error. No historical prices for ccy"

        counter -= 1
        if counter < 1:
            return "Error: No historical prices for PrevDay"

        firstDay = tmpdate
        tmpdate = dayNoWeekend(tmpdate - datetime.timedelta (days = 1),-1)


    # Get px and xrate for lastDay
    px_last_ccy = -1
    xrate2 = -1
    tmpdate = lastDay
    counter = maxloops
    while (px_last_ccy == -1) or (xrate2 == -1):
        sql = "SELECT " + \
                "ha_price " + \
              "FROM " + \
                "databasename.histassets " + \
              "WHERE " + \
                "ha_ticker ='" + ticker + "' AND " +\
                "ha_date = '" + tmpdate.strftime("%Y-%m-%d") + "'"
        px_last_ccy = float(mysql_conn.selectone(sql, -1))

        xrate2 = 1
        if ccy != base_ccy:
            sql = "SELECT " + \
                "ccy_xrate " + \
              "FROM " + \
                "databasename.histccy " + \
              "WHERE " + \
                "ccy_name ='" + ccy + "' AND " + \
                "ccy_date = '" + tmpdate.strftime("%Y-%m-%d") + "'"
            xrate2 = float(mysql_conn.selectone(sql, -1))

        if px_last_ccy != -1 and xrate2 == -1:
            return "Error. No historical prices for ccy"

        counter -= 1
        if counter < 1:
            return "Error: No historical prices for LastDay"

        lastDay = tmpdate
        tmpdate = dayNoWeekend(tmpdate - datetime.timedelta (days = 1),-1)

    px_first_ccy *= v1p
    px_last_ccy *= v1p

    if ccy == 'GBP':
        px_first_ccy = px_first_ccy / 100
        px_last_ccy = px_last_ccy / 100

    px_first_base = px_first_ccy / xrate1
    px_last_base = px_last_ccy / xrate2

    if perf_base != last_perf_base:         # We exited the position sometime in the period between (d0,d1) and
                                            # we do not need to take into account the number of shares at the beginning (amnt0)
        perf_base = -(amnt0 * px_first_base) + perf_base + (amnt * px_last_base)
        last_perf_base  = last_perf_base + (amnt * px_last_base)
    else:
        perf_base = -(amnt0 * px_first_base) + perf_base + (amnt * px_last_base)
        last_perf_base  = perf_base

    mval_ccy = amnt * px_last_ccy
    mval_base = amnt * px_last_base

    # QTY, MVAL_BASE, PX_AVG_CCY, PX_AVG_BASE, RTN_BASE, RTN_BASE_CONST_CCY
    if field == 'QTY':
        return amnt
    elif field == 'MVAL_CCY':
        return mval_ccy
    elif field == 'MVAL_BASE':
        return mval_base
    elif field == 'PX_AVG_CCY':
        return px_avg_ccy
    elif field == 'PX_AVG_BASE':
        return px_avg_base
    elif field == 'RTN_BASE':
        return perf_base
    elif field == 'RTN_LAST_BASE':
        return last_perf_base
    else:
        return "Error: Please check requested field"
# Performance function end




def position(args):

    if args[0] == 'mktValEur':
        toRtn = 'mktValEur'
    else:
        toRtn = 'mktValCcy'

    if len(args) >= 2:
        if args[1] == '':
            return 'Error: ticker missing'
        ticker = args[1]

    if len(args) >= 3:
        if args[2] == '':
            day1 = datetime.datetime.now()
        else:
            day1 = args[2]

    if isExcelDate(day1):
        day1 = minimalist_xldate_as_datetime(int(float(day1)))
    else:
        day1 = datetime.datetime.strptime(day1, '%Y-%m-%d')

    base_ccy='EUR'
    if len(args) >= 4:
        if args[3] != '':
            base_ccy = args[3]

    tblddbb='databasename.fundtrades'
    if len(args) >= 5:                      # fundtrades/swaptrades table
        if args[4] == 'databasename.fundtrades':
            tblddbb = 'databasename.fundtrades'
        elif args[4] == 'databasename.swaptrades':
            tblddbb = 'databasename.swaptrades'
        else:
            return "Error: please check table name (fundtrades / swaptrades)"

    maxloops = 10                           # To prevent an inf loop
    v1p = 1                                 # Value of 1 point.
    t_init = datetime.datetime.strptime("2015-07-31", "%Y-%m-%d")
    lastDay = dayNoWeekend(day1,-1)
    if lastDay < t_init:
        return "Error: lastDay < t_init"

    sql = "SELECT ccy FROM databasename.assets WHERE ticker = '" + ticker + "'"
    ccy = mysql_conn.selectone(sql, '#--')
    if ccy == '#--':
        return "Error: ccy"

    maxhist = mysql_conn.selectone("SELECT max(ha_date) FROM databasename.histassets WHERE ha_ticker = '" + ticker + "'", '#--')
    if maxhist == '#--':
        return "Error: max historical prices"

    maxhist = datetime.datetime.strptime(maxhist,"%Y-%m-%d")

    # Check if the historical table for the asset and the currency if is not EUR are up to date.
    if maxhist < lastDay:
        if ccy != base_ccy:
            lastDayCcy = mysql_conn.selectone("SELECT max(ccy_date) FROM databasename.histccy WHERE ccy_name = '" + ccy + "'")
            lastDayCcy = datetime.datetime.strptime(lastDayCcy,'%Y-%m-%d')
            if lastDayCcy < lastDay:
                return "Error. Please update ccy exchange rates"
        return "Error. Please update historical prices"

    sectype = mysql_conn.selectone("SELECT sectype FROM databasename.assets WHERE ticker = '" + ticker + "'")
    if sectype == 'INDEX' and ticker != 'MCXP' and ticker != 'SCXP':
        v1p = float(mysql_conn.selectone("SELECT fut_valuepoint FROM databasename.assets WHERE ticker = '" + ticker + "'", '-1'))

        lastTradeDate = mysql_conn.selectone("SELECT fut_lasttradedate FROM databasename.assets WHERE ticker = '" + ticker + "'")
        if lastTradeDate == '#--':
            return "Error: lastTradeDate"
        lastTradeDate = datetime.datetime.strptime(lastTradeDate,'%Y-%m-%d')

        if lastTradeDate < lastDay:
            lastDay = lastTradeDate

    amnt = getNumShares([ticker, lastDay.strftime("%Y-%m-%d"), tblddbb])

    # Get px and xrate for lastDay
    px_last_ccy = -1
    xrate = -1
    tmpdate = lastDay
    counter = maxloops
    while (px_last_ccy == -1) or (xrate == -1):
        sql = "SELECT " + \
                "ha_price " + \
              "FROM " + \
                "databasename.histassets " + \
              "WHERE " + \
                "ha_ticker ='" + ticker + "' AND " +\
                "ha_date = '" + tmpdate.strftime("%Y-%m-%d") + "'"
        px_last_ccy = float(mysql_conn.selectone(sql, -1))

        xrate = 1
        if ccy != base_ccy:
            sql = "SELECT " + \
                "ccy_xrate " + \
              "FROM " + \
                "databasename.histccy " + \
              "WHERE " + \
                "ccy_name ='" + ccy + "' AND " + \
                "ccy_date = '" + tmpdate.strftime("%Y-%m-%d") + "'"
            xrate = float(mysql_conn.selectone(sql, -1))

        if px_last_ccy != -1 and xrate == -1:
            return "Error. No historical prices for ccy"

        counter -= 1
        if counter < 1:
            return "Error: No historical prices for LastDay"

        lastDay = tmpdate
        tmpdate = dayNoWeekend(tmpdate - datetime.timedelta (days = 1),-1)

    px_last_ccy *= v1p

    if ccy == 'GBP':
        px_last_ccy = px_last_ccy / 100

    px_last_base = px_last_ccy / xrate

    mval_ccy = amnt * px_last_ccy
    mval_base = amnt * px_last_base

    if toRtn == 'mktValCcy':
        return mval_ccy
    else:
       return mval_base


def getMktValEur(args):
    args.insert(0,"mktValEur")
    return position(args)


def getMktValCcy(args):
    args.insert(0,"mktValCcy")
    return position(args)


def getNumShares(args):
    if len(args) >= 1:
        if args[0] == '':
            return 'Error: ticker missing'
        ticker = args[0]

    if len(args) >= 2:
        if args[1] == '':
            day1 = datetime.datetime.now()
        else:
            day1 = args[1]

    if isExcelDate(day1):
        day1 = minimalist_xldate_as_datetime(int(float(day1)))
    else:
        day1 = datetime.datetime.strptime(day1, '%Y-%m-%d')

    tblddbb='databasename.fundtrades'
    if len(args) >= 3:                      # fundtrades/swaptrades table
        if args[2] == 'databasename.fundtrades':
            tblddbb = 'databasename.fundtrades'
        elif args[2] == 'databasename.swaptrades':
            tblddbb = 'databasename.swaptrades'
        else:
            return "Error: please check table name (fundtrades / swaptrades)"

    sql = "SELECT " + \
            "sum(trd_filled) " + \
          "FROM " + \
            tblddbb + \
          " WHERE " + \
            "trd_ticker = '" + ticker + "' AND " + \
            "trd_date <= '" + day1.strftime("%Y-%m-%d") + "'"

    return float(mysql_conn.selectone(sql, 0))

######################################################################################################
def weight(ticker,date=datetime.date.today(), nav=17524206):

    if date==datetime.date.today():
        date-=datetime.timedelta(days=1)
    elif date=="":
        date=datetime.date.today()-datetime.timedelta(days=1)
    elif isExcelDate(date):
        date = minimalist_xldate_as_datetime(int(float(date)))
    else:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    date=date.strftime("20%y-%m-%d")


    #p_var=(str(ticker),"MVAL_BASE",str(date),str(date),"EUR")
    MVAL=getMktValEur([str(ticker), str(date)])

    #MVAL=performance(p_var)
    try:

        weight= float(MVAL)/nav
    except ValueError:
        weight=0
    return weight
##########################################################################################################
def exposure(arg):
    from decimal import Decimal


    date=arg[0]
    field=arg[1]



    if date=="":
        date=datetime.date.today()-datetime.timedelta(days=1)
    elif date==datetime.date.today():
        date-=datetime.timedelta(days=1)
    elif isExcelDate(date):
        date = minimalist_xldate_as_datetime(int(float(date)))
    else:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    date=date.strftime("20%y-%m-%d")



    long_weight_list=[]
    short_weight_list=[]

    gross_exp=0
    net_exp=0




    #get all the tickers that we have ever had on the funds
    if len(arg)==3:
        if arg[2]=="index":
            ticker= mysql_conn.select("SELECT DISTINCT(trd_ticker) FROM databasename.fundtrades A left join databasename.assets B on A.trd_ticker = B.ticker WHERE sectype = 'EQUITY' or (sectype = 'INDEX' and fut_lasttradedate > '"+date+"')or (sectype = 'INDEX' and fut_lasttradedate = '1970-01-01')")
    else:
        if arg[1]=="grossexp" or arg[1]=="netexp":
            ticker= mysql_conn.select("SELECT DISTINCT(trd_ticker) FROM databasename.fundtrades A left join databasename.assets B on A.trd_ticker = B.ticker WHERE sectype = 'EQUITY' or (sectype = 'INDEX' and fut_lasttradedate > '"+date+"')or (sectype = 'INDEX' and fut_lasttradedate = '1970-01-01')")
        else:
            ticker= mysql_conn.select("SELECT DISTINCT(trd_ticker) FROM databasename.fundtrades A left join databasename.assets B on A.trd_ticker = B.ticker WHERE sectype = 'EQUITY' ")




    #get all the countries in the fund
    country_list=  mysql_conn.select("select distinct(country) from databasename.assets where country != '#--' and country != '#N/A FIELD NOT APPLICABLE' ")
    Netcountry_dict={}
    Grosscountry_dict={}
    #store all the countries in a dictionary with each of their values being equal to zero
    for country in country_list:
        Netcountry_dict[country]=0
        Grosscountry_dict[country]=0
    del country_list
    Netcountry_list=[]
    Grosscountry_list=[]




    #get all the sectors in the fund
    sector_list=  mysql_conn.select("select distinct(eq_sector) from databasename.assets where eq_sector != '#--' and eq_sector != '#N/A FIELD NOT APPLICABLE' ")
    Netsector_dict={}
    Grosssector_dict={}
    #store all the countries in a dictionary with each of their values being equal to zero
    for sectors in sector_list:
        Netsector_dict[sectors]=0
        Grosssector_dict[sectors]=0
    del sector_list
    Netsector_list=[]
    Grosssector_list=[]


    amin=[]



    for all in ticker:

        #check if the ticker in the total ticker's list is still on the portfolio
        if weight(all,date) !=0 :


            #Gross exposure always adds the weights regardless of short or long


            if arg[1]=="grossexp":
                gross_exp=gross_exp+abs(weight(all, date))


            elif arg[1]=="netexp":
                net_exp=net_exp+weight(all,date)




            elif arg[1][:9]=="netsector":
                sector= mysql_conn.select("SELECT eq_sector FROM databasename.assets where ticker="+ "'"+ all+"'")
                if sector in Netsector_dict:
                    Netsector_dict[sector]+= weight(all, date)

            elif arg[1][:11]=="grosssector":
                sector= mysql_conn.select("SELECT eq_sector FROM databasename.assets where ticker="+ "'"+ all+"'")
                if sector in Grosssector_dict:
                    Grosssector_dict[sector]+= abs(weight(all,date))





            elif arg[1][:10]=="netcountry":
                country= mysql_conn.select("SELECT country FROM databasename.assets where ticker="+ "'"+ all+"'")
                if country in Netcountry_dict:
                    Netcountry_dict[country]+=weight(all,date)

            elif arg[1][:12]=="grosscountry":
                country= mysql_conn.select("SELECT country FROM databasename.assets where ticker="+ "'"+ all+"'")
                if country in Grosscountry_dict:
                    Grosscountry_dict[country]+=abs(weight(all,date))


#Find the Position-----------------

            #for net exposure the longs' weight is deducted by the shorts' weights
            #positionquery="SELECT trd_position FROM databasename.fundtrades WHERE trd_ticker = "+"'" + all + "'"

            #position=mysql_conn.select(positionquery)
            #if type(position)!= list:
             #   position=[position]

#------------------------------------



            if weight(all, date)>0:

                #for long, the gross nad net both add the weights (for all cases of sector, country and total exposures)

                long_weight_list.append((round(Decimal(weight(all, date))*100,2),all))


            else:            #for short, the gross still adds but net substract the weights (for all cases of sector, country and total exposures)

                short_weight_list.append((round(Decimal(weight(all, date))*100,2),all))




    #values of the sector and country exposure are stored in a list of tuples in order to provide the possibility of sorting and see the biggest positions

    for k, v in Netsector_dict.iteritems():
        Netsector_list.append((round(Decimal(v*100),2),k))
    for k, v in Grosssector_dict.iteritems():
        Grosssector_list.append((round(Decimal(v*100),2),k))
    Grosssector_list.sort()
    Netsector_list.sort( )


    for k, v in Netcountry_dict.iteritems():
        Netcountry_list.append((round(Decimal(v*100),2),k))
    for k, v in Grosscountry_dict.iteritems():
        Grosscountry_list.append((round(Decimal(v*100),2),k))
    Grosscountry_list.sort()
    Netcountry_list.sort()

    long_weight_list.sort()
    short_weight_list.sort()
    if field[:4]=="long":
        for i in range(1,6):
            if field=="long"+str(i):
                return long_weight_list[len(long_weight_list)-i]
            if field=="long"+str(-i):
                return long_weight_list[i-1]
    if field[:5]=="short":
        for i in range(1,6):
            if field=="short"+str(i):
                return short_weight_list[len(short_weight_list)-i]
            if field=="short"+str(-i):
                return short_weight_list[i-1]

    if field[:12]=="grosscountry":
        for i in range(1,len(Grosscountry_list)):
            if field=="grosscountry"+str(i):
                return Grosscountry_list[len(Grosscountry_list)-i]
            if field=="grosscountry"+str(-i):
                while Grosscountry_list[i-1][0]==0:
                    i+=1
                return Grosscountry_list[i-1]

    if field[:10]=="netcountry":
        for i in range(1,len(Netcountry_list)):
            if field=="netcountry"+str(i):
                return Netcountry_list[len(Netcountry_list)-i]
            if field=="netcountry"+str(-i):
                while Netcountry_list[i-1][0]==0:
                    i+=1
                return Netcountry_list[i-1]

    if field[:11]=="grosssector":
        for i in range(1,len(Grosssector_list)):
            if field=="grosssector"+str(i):
                return Grosssector_list[len(Grosssector_list)-i]
            if field=="grosssector"+str(-i):
                while Grosssector_list[i-1][0]==0:
                    i+=1
                return Grosssector_list[i-1]

    if field[:9]=="netsector":
        for i in range(1,len(Netsector_list)):
            if field=="netsector"+str(i):
                return Netsector_list[len(Netsector_list)-i]
            if field=="netsector"+str(-i):
                while Netsector_list[i-1][0]==0:
                    i+=1
                return Netsector_list[i-1]

    if field=="grossexp":
        return round(Decimal(gross_exp*100),2)

    if field=="netexp":
        return round(Decimal(net_exp*100),2)





###################################################################################################

#volatility of teh fund
def volatility(args):
    from decimal import Decimal
    import numpy as np
    from scipy.stats import norm

    date=args[0]
    p=int(args[1])     #NAV of portfolio
    c=float(args[2])     # for VaR 99% enter 0.99 etc
    field=args[3]       # var, vol or beta





    if date=="":
        date=datetime.date.today()-datetime.timedelta(days=1)

    elif isExcelDate(date):
        date = minimalist_xldate_as_datetime(int(float(date)))
    else:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    if date==datetime.date.today():
        date-=datetime.timedelta(days=1)
    date=date.strftime("20%y-%m-%d")


    return_weight_ticker=[]


    #adjust the fut_lasttrade date in ticker!!!!!

    ticker= mysql_conn.select("SELECT DISTINCT(trd_ticker) FROM databasename.fundtrades A left join databasename.assets B on A.trd_ticker = B.ticker WHERE sectype = 'EQUITY' or (sectype = 'INDEX' and fut_lasttradedate > '"+date+"')or (sectype = 'INDEX' and fut_lasttradedate = '1970-01-01')")


    for ticker in ticker:

        weight2=weight(ticker, date)
        if weight2!=0:

            price= mysql_conn.select("SELECT ha_price, ha_date FROM databasename.histassets where ha_ticker="+"'"+ticker+"'"+"AND (DAYOFWEEK(ha_date) != 1 and DAYOFWEEK(ha_date) != 7)")
            noinfocount=0
            while price[noinfocount][0]==-1:
                noinfocount+=1
            price=price[noinfocount:]
            if len(price)<10:
                continue

            for i in range (0,len(price)):
                if price[i][0]==-1:
                    price[i]=(price[i-1][0],price[i][1])
                i+=1
            returnlist=[]
            betareturn=[]



            #position=mysql_conn.select("SELECT trd_position FROM databasename.fundtrades WHERE trd_ticker="+ "'" +ticker+"'")
            #if type(position)!=list:
             #   position=[position]
            if weight2<0:
                position=["SHORT"]
            else:
                position=["LONG"]
            if position[len(position)-1]=="SHORT":
                for i in range(1,len(price)):
                    returnlist.append(-np.log(float(price[i][0])/price[i-1][0]))
                    betareturn.append(np.log(float(price[i][0])/price[i-1][0]))
                    i+=1
           #volatility=(statistics.stdev(returnlist))*(252**0.5)
                #vol_weight_ticker.append((volatility,weight2,ticker))




            elif position[len(position)-1]=="LONG":
                for i in range(1,len(price)):
                    betareturn.append(np.log(float(price[i][0])/price[i-1][0]))
                    returnlist.append(np.log(float(price[i][0])/price[i-1][0]))
                    i+=1
  #volatility=(statistics.stdev(returnlist))*(252**0.5)
                #vol_weight_ticker.append((volatility,weight2,ticker))
            return_weight_ticker.append((returnlist, weight2, ticker,betareturn))

    returnlist=[]
    weightlist=[]
    returnlist_beta=[]
    tickerlist=[]


    for all in return_weight_ticker:

        returnlist.append(all[0])
        weightlist.append(all[1])
        returnlist_beta.append(all[3])

        tickerlist.append(all[2])


    def cov(a,b):
        x=a
        y=b

        if len(x) != len(y):
            x=x[len(x)-min(len(x),len(y)):]
            y=y[len(y)-min(len(x),len(y)):]
        n = len(x)

        xy = [x[i]*y[i] for i in range(n)]
        mean_x = sum(x)/float(n)
        mean_y = sum(y)/float(n)

        var = 0
        for i in range(n):
            var = var + sum([(x[i]-mean_x)*(y[i]-mean_y)])
        return var/(n-1)



    covm=[]

    for i in range(0, len(returnlist)):
        covm.append([])
        if len(returnlist[i])>1:

            for j in range(0, len(returnlist)):
                if len(returnlist[j])>1 :

                    covm[i].append(cov(returnlist[i],returnlist[j]))
                    j+=1
            i+=1
    removecount=0
    removeindex=[]

    while [] in covm:
        removeindex.append(covm.index([])+removecount)
        covm.remove([])
        removecount+=1
    i=len(removeindex)-1
    while i>-1:
        del(weightlist[removeindex[i]])
        i-=1
    covm=np.array(covm)
    asset_mean_return_list=[]
    for all in returnlist:
        if len(all)>1:
            asset_mean_return_list.append(np.average(all))




    weightlist=np.array(weightlist)
    portfoliovol=((np.dot(abs(weightlist).T, np.dot(covm,abs(weightlist))))**0.5)*(252**0.5)
    if field=="vol":
        return round(Decimal(100*portfoliovol),2)
    def VaR(c,p,m,v):
        alpha = norm.ppf(1-c, m, v )
        return p - p*(alpha + 1)



    expectedval=np.average(asset_mean_return_list, weights=abs(weightlist))
    if field== "var":
        var=VaR(c,p,expectedval,portfoliovol/(252**0.5))
        return round(Decimal(100*var/p),2)


    def Beta():
        beta=[]

        indexprice= mysql_conn.select("SELECT ha_price FROM databasename.histassets where ha_ticker='SXXP' and ha_date > '2016-03-12' and (DAYOFWEEK(ha_date) != 1 and DAYOFWEEK(ha_date) != 7)" )
        for n,i in enumerate(indexprice):
            if i==-1:
                indexprice[n]=indexprice[n-1]



        indexreturnlist=[]
        for i in range(1,len(indexprice)):
            indexreturnlist.append(np.log(float(indexprice[i])/indexprice[i-1]))



        for all in returnlist_beta:
            beta.append((cov(all,indexreturnlist)/cov(indexreturnlist,indexreturnlist))*0.67+0.33)





        portfoliobeta=np.dot(beta,list(weightlist))
        return float(portfoliobeta)

        #return round(Decimal(portfoliobeta),2)


    if field=="beta":

        return Beta()


###############################################################################################
#Drawdown of the fund

def drawdown(arg):
    date=arg[0]
    import ast
    if date=="":
        date=datetime.date.today()-datetime.timedelta(days=1)

    elif isExcelDate(date):
        date = minimalist_xldate_as_datetime(int(float(date)))
    else:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    if date==datetime.date.today():
        date-=datetime.timedelta(days=1)
    date1=date.strftime("20%y-%m-%d")


    sql="select NULL AS f_date, NULL AS row_num, NULL AS drowdawn FROM dual WHERE @row_num := 0 OR @drowdawn := 1 UNION (SELECT"+\
    "fund_date AS f_date, @row_num := @row_num + 1 AS row_num, @drowdawn := if(@drowdawn * (1 + fund_performance_local)  <= 1, @drowdawn * (1 + fund_performance_local), 1) AS drowdawn"+\
    "from databasename.funds where fund_shares='A2' AND fund_date >= '2015-01-01' AND fund_date < '2016-03-31'and fund_ccy = 'EUR' ORDER BY fund_date ASC) ORDER BY row_num DESC LIMIT 1"


    last_sei_draw= mysql_conn.select("select NULL AS f_date, NULL AS row_num, NULL AS drowdawn FROM dual WHERE @row_num := 0 OR @drowdawn := 1 UNION (SELECT fund_date AS f_date, @row_num := @row_num + 1 AS row_num, @drowdawn := if(@drowdawn * (1 + fund_performance_local)  <= 1, @drowdawn * (1 + fund_performance_local), 1) AS drowdawn from databasename.funds where fund_shares='A2' AND fund_date >= '2015-01-01' AND fund_date < '2016-03-31'and fund_ccy = 'EUR' ORDER BY fund_date ASC) ORDER BY row_num DESC LIMIT 1")

    draw=last_sei_draw[2]

    '''
    last_sei_draw= mysql_conn.select("select	NULL AS f_date,	NULL AS row_num, NULL AS drowdawn FROM dual WHERE @row_num := 0 OR @drowdawn := 1 UNION (SELECT	fund_date AS f_date, @row_num := @row_num + 1 "+\
    "AS row_num, @drowdawn := @drowdawn * (1 + fund_performance_local) AS drowdawn from databasename.funds "+\
    "where fund_shares='A2' AND fund_date >= '2015-01-01' AND fund_date <= '"+date1 +"'and fund_ccy = 'EUR' ORDER BY fund_date ASC) ORDER BY row_num DESC LIMIT 1
    '''


    date2=last_sei_draw[0]
    year=date.year
    month=date.month
    while date>date2:
        date3=date2.strftime("20%y-%m-%d")
        print("SELECT sum(fund_ending_nav_base) as sum_fund FROM databasename.funds where year(fund_date) = '"+ str(year)+"' AND month(fund_date) ='"+ str(month)+"'")

        nav=mysql_conn.select("SELECT sum(fund_ending_nav_base) as sum_fund FROM databasename.funds where year(fund_date) = '"+ str(year)+"' AND month(fund_date) ='"+ str(month)+"'")
        #nav=mysql_conn.select("SELECT sum(fund_ending_nav_base) as sum_fund FROM databasename.funds where fund_date='"+date1+"'")
        if nav is None:
            month-=1
            nav=mysql_conn.select("SELECT sum(fund_ending_nav_base) as sum_fund FROM databasename.funds where year(fund_date) = '"+ str(year)+"' AND month(fund_date) ='"+ str(month)+"'")
        pandl=loss((date1 , "", "", nav))

        draw*=(1+(pandl/nav))*100
        if draw>100:
            draw=100
        date2+=datetime.timedelta(days=32)
        date2.replace(day=1)

    return draw

#############################################################################################

def loss(args):
    date=args[0]
    ticker=args[1]
    field=args[2]
    nav=args[3]

    if nav=="":
        nav=17756630
    else:
        nav=float(nav)



    if date=="":
        date=datetime.date.today()-datetime.timedelta(days=1)
    elif isExcelDate(date):
        date = minimalist_xldate_as_datetime(int(float(date)))
    else:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    if date==datetime.date.today():
        date-=datetime.timedelta(days=1)


    date1=date.strftime("20%y-%m-%d")




    loserslist=[]

    if ticker=="":
        pandltotal=0
        ticker= mysql_conn.select("SELECT DISTINCT(trd_ticker) FROM databasename.fundtrades A left join databasename.assets B on A.trd_ticker = B.ticker " + \
                                  "WHERE sectype = 'EQUITY' or (sectype = 'INDEX' and fut_lasttradedate > '"+date1+ "') or ticker = 'MCXP' or ticker = 'SCXP'")
        for ticker in ticker:
            weight2=abs(weight(ticker, date1))
            if weight2>0:
                sql_return=(str(ticker),"RTN_BASE",str(date.replace(day=1).strftime("20%y-%m-%d")),str(date.strftime("20%y-%m-%d")),"EUR")

                Return=performance(sql_return)
                try:
                    if Return[:31]=="Error: No historical prices for":
                        price=mysql_conn.select("SELECT  ha_price,ha_date FROM databasename.histassets where ha_ticker='"+str(ticker)+"' AND (DAYOFWEEK(ha_date) != 1 and DAYOFWEEK(ha_date) != 7)")
                        noinfocount=0
                        while int(price[noinfocount][0])==-1:
                            noinfocount+=1
                        price=price[noinfocount:]
                        begining_dt=price[1][1].strftime("20%y-%m-%d")
                        sql_return=(str(ticker),"RTN_BASE",begining_dt,str(date),"EUR")
                        Return=performance(sql_return)


                except TypeError:
                    pass


                pandltotal+=Return
                if Return/nav < -0.005:
                    loserslist.append(ticker)


        if field== "":
            return pandltotal
        else:
            return loserslist
    else:
        sql_return=(str(ticker),"RTN_BASE",str(date.replace(day=1).strftime("20%y-%m-%d")),str(date.strftime("20%y-%m-%d")),"EUR")

        weight2=abs(weight(ticker, date1))
        Return=0
        if weight2>0:

            Return=performance(sql_return)
            try:
                if Return[:31]=="Error: No historical prices for":
                    price=mysql_conn.select("SELECT  ha_price,ha_date FROM databasename.histassets where ha_ticker='"+str(ticker)+"' AND (DAYOFWEEK(ha_date) != 1 and DAYOFWEEK(ha_date) != 7)")
                    noinfocount=0
                    while int(price[noinfocount][0])==-1:
                        noinfocount+=1
                    price=price[noinfocount:]
                    begining_dt=price[1][1].strftime("20%y-%m-%d")
                    sql_return=(str(ticker),"RTN_BASE",begining_dt,str(date),"EUR")
                    Return=performance(sql_return)


            except TypeError:
                pass




        return Return

##############################################################################################
def liquidity(args):
    import statistics
    ticker=args[0]
    percentofvol=args[1]
    date=args[2]
    field=args[3]
    if field=="":
        field=1

    if percentofvol=="":
        percentofvol=1

    if date=="":
        date=datetime.date.today()-datetime.timedelta(days=1)
    elif isExcelDate(date):
        date = minimalist_xldate_as_datetime(int(float(date)))
    else:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    if date==datetime.date.today():
        date-=datetime.timedelta(days=1)
    date1=date.strftime("20%y-%m-%d")


    if ticker=="":
        ticker= mysql_conn.select("SELECT DISTINCT(trd_ticker) FROM databasename.fundtrades A left join databasename.assets B on A.trd_ticker = B.ticker WHERE sectype = 'EQUITY' or (sectype = 'INDEX' and fut_lasttradedate > '"+date1+ "')")
        Liquidity=[]
        for ticker in ticker:
            if abs(weight(ticker, date1))>0:
                date2=date-datetime.timedelta(days=120)
                date2=date2.strftime("20%y-%m-%d")
                volume= mysql_conn.select("SELECT ha_volume FROM databasename.histassets WHERE ha_ticker = '"+ticker+"' AND ha_volume!=-1 and ha_date> '"+date2+"' and ha_date<'"+date1+"'")
                meanvolume=statistics.mean(volume)
                percentofvolmean=meanvolume*percentofvol


                quantity=getNumShares([str(ticker), str(date1)])

                quantity=abs(float(quantity))
                Liquidity.append((quantity/percentofvolmean,ticker))
            Liquidity.sort(reverse=True)

        return Liquidity[field-1]

    else:
        date2=date-datetime.timedelta(days=120)
        volume= mysql_conn.select("SELECT ha_volume FROM databasename.histassets WHERE ha_ticker = '"+ticker+"' AND ha_volume!=-1 and ha_date> '"+date2+"' and ha_date<'"+date1+"'")
        meanvolume=statistics.mean(volume)
        percentofvolmean=meanvolume*percentofvol
        sql_value=(str(ticker),"QTY",str(date1),str(date1),"EUR")

        quantity=performance(sql_value)
        Liquidity=quantity/percentofvolmean
        return Liquidity

##############################################################################################
def exageration(arg):

    import math
    import numpy as np

    p=arg[0]
    date=arg[1]

    if date=="":
        date=datetime.date.today()-datetime.timedelta(days=1)

    elif isExcelDate(date):
        date = minimalist_xldate_as_datetime(int(float(date)))
    else:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    if date==datetime.date.today():
        date-=datetime.timedelta(days=1)
    date=date.strftime("20%y-%m-%d")


    if p=="":
        p=0.90
    else:
        p=float(p)
    highpercentile=p

    sector_indices=["SXAP","SX7P","SXBSCP","SXPP","SX4P","SXOP","SXCGSP","SXCSVP","SXFP","SXFINP", "SX3P","SXDP","SXNP","SXIP","SXMP","SXEP","SXQP","S8670P","SX86P","SXRP","SX8P","SXKP","SXTP","SX6P","SXIDUP"]
    highlowlist=[]
    count=0
    for all in sector_indices:
        mysqlquery="SELECT ha_price FROM databasename.histassets where ha_ticker='"+all+"' AND ha_date <='"+date+"' and (DAYOFWEEK(ha_date) != 1 and DAYOFWEEK(ha_date) != 7)"
        price= mysql_conn.select(mysqlquery)
        returnlist=[]


        while -1 in price:
            price.remove(-1)
        for i in range(1,len(price)):
            returnlist.append(np.log(float(price[i])/price[i-1]))
            i+=1


        L=highpercentile*(len(returnlist)+1)
        loc=math.trunc(L)
        last=returnlist[len(returnlist)-1]
        returnlist.sort()

        pth_percentile=returnlist[loc-1]+((L-loc)*(returnlist[loc]-returnlist[loc-1]))


        if last>pth_percentile:
            highlowlist.append(all+" is"+" too high")
            count+=1
        returnlist.sort(reverse=True)
        L=highpercentile*(len(returnlist)+1)
        loc=math.trunc(L)
        pth_percentile=returnlist[loc-1]+((L-loc)*(returnlist[loc]-returnlist[loc-1]))

        if last<pth_percentile:
            highlowlist.append(all+" is"+"too low")
            count+=1
    if count==0:
            highlowlist.append("Every sector is within limits")
    for i in range(0,len(highlowlist)):
        if highlowlist[i] in highlowlist[:i]:
            print(highlowlist[i])

    highlowlist.sort()
    return(str(highlowlist))


######################################################################################

def longsql(ticker, date0, date1, tbl):
    return " " + \
"           SELECT " + \
"            @rownum := @rownum + 1.0 as row_number, " + \
"            A.trd_filled AS trd_filled, " + \
"            B.commission AS commission, " + \
"            @val1ptn := IF(D.fut_valuepoint <0.0 , 1.0, D.fut_valuepoint) AS val1ptn, " + \
"            @xrate := IFNULL(C.ccy_xrate, 1.0) AS xrate, " + \
"            @prevtotal := @total as prev_total_shares, " + \
"            @total := @total + A.trd_filled AS total_shares, " + \
"            @pricegross := (A.trd_pricegross * @val1ptn) AS trd_pricegross, " + \
"            @pricenet := if( A.trd_filled > 0.0, " + \
"                                @pricegross * (1.0 + B.commission/10000.0), " + \
"                                @pricegross * (1.0 - B.commission/10000.0) " + \
"                            ) AS trd_pricenet, " + \
"            @prev_avgprice_ccy := @avgprice_ccy AS prev_avgprice_ccy, " + \
"            @avgprice_ccy := (if(@total = 0.0, " + \
"                            0.0, " + \
"                            if( A.trd_filled > 0.0 AND @total > 0.0, " + \
"                                ((@prevtotal * @prev_avgprice_ccy) + (trd_filled * @pricenet))/ @total, " + \
"                                if( trd_filled < 0.0 AND @total < 0.0, " + \
"                                    ((@prevtotal * @prev_avgprice_ccy) + (trd_filled * @pricenet))/ @total, " + \
"                                    @prev_avgprice_ccy " + \
"                                ) " + \
"                            ) " + \
"                         ))AS avgprice_ccy, " + \
"            @pricenet_base := (if(A.trd_ccy = 'GBP', (@pricenet/100.0)/@xrate, @pricenet/@xrate)) AS pricenet_base, " + \
"            @prev_avgprice_base := @avgprice_base AS prev_avgprice_base, " + \
"            @avgprice_base := (if(@total = 0.0, " + \
"                            0.0, " + \
"                            if( A.trd_filled > 0.0 AND @total > 0.0, " + \
"                                ((@prevtotal * @prev_avgprice_base) + (trd_filled * @pricenet_base))/ @total, " + \
"                                if( trd_filled < 0.0 AND @total < 0.0, " + \
"                                    ((@prevtotal * @prev_avgprice_base) + (trd_filled * @pricenet_base))/ @total, " + \
"                                   @prev_avgprice_base " + \
"                                ) " + \
"                            ) " + \
"                         ))AS avgprice_base, " + \
"            @prevperf_base := @perf_base AS prevperf_base, " + \
"            @perf_base := if(A.trd_date >= '" + date0.strftime("%Y-%m-%d") + "' AND A.trd_date <= '" + date1.strftime("%Y-%m-%d") + "', " + \
"                            @prevperf_base - A.trd_filled * @pricenet_base, " + \
"                            0.0 " + \
"                          ) AS perf_base, " + \
"            @prev_last_perf_base := @last_perf_base AS prev_last_perf_base, " + \
"            @last_perf_base := if(A.trd_date >= '2015-07-31' AND A.trd_date <= '" + date1.strftime("%Y-%m-%d") + "' AND @total != 0.0, " + \
"                            @prev_last_perf_base - A.trd_filled * @pricenet_base, " + \
"                            0.0 " + \
"                          ) AS last_perf_base, " + \
"			@holding := if( @prevtotal = 0.0 AND @total != 0.0, " + \
"							A.trd_date, " + \
"							@holding) AS holding, " + \
"            @buynumshares := if( A.trd_filled > 0.0, " + \
"                                @buynumshares + A.trd_filled, " + \
"                                @buynumshares " + \
"                            ) AS buynumshares, " + \
"            @sellnumshares := if( A.trd_filled < 0.0, " + \
"                                @sellnumshares + A.trd_filled, " + \
"                                @sellnumshares " + \
"                            ) AS sellnumshares " + \
"        FROM " + tbl + " A " + \
"        LEFT JOIN databasename.brokers   B on A.trd_broker = B.idbroker " + \
"        LEFT JOIN databasename.histccy   C on A.trd_ccy = C.ccy_name AND A.trd_date = C.ccy_date " + \
"        LEFT JOIN databasename.assets    D on A.trd_ticker = D.ticker " + \
"        WHERE " + \
"            A.trd_ticker = '" + ticker + "' AND " + \
"            A.trd_date <= '" + date1.strftime("%Y-%m-%d") + "' " + \
"        ORDER BY trd_date ASC, trd_time ASC; "

#####################################################################################################
def new_parser(args):

    parser = argparse.ArgumentParser()
    parser.add_argument('-i',               nargs='*', dest='insert',           action=Extender)
    parser.add_argument('-l',               nargs='*', dest='mktvallocal',      action=Extender)
    parser.add_argument('-m',               nargs='*', dest='mktvaleur',        action=Extender)
    parser.add_argument('-n',               nargs='*', dest='numshares',        action=Extender)
    parser.add_argument('-p',               nargs='*', dest='performance',      action=Extender)
    parser.add_argument('-s',               nargs='*', dest='select',           action=Extender)
    parser.add_argument('-u',               nargs='*', dest='update',           action=Extender)
    parser.add_argument('--insert',         nargs='*', dest='insert',           action=Extender)
    parser.add_argument('--mktvaleur',      nargs='*', dest='mktvaleur',        action=Extender)
    parser.add_argument('--mktvallocal',    nargs='*', dest='mktvallocal',      action=Extender)
    parser.add_argument('--numshares',      nargs='*', dest='numshares',        action=Extender)
    parser.add_argument('--select',         nargs='*', dest='select',           action=Extender)
    parser.add_argument('--performance',    nargs='*', dest='performance',      action=Extender)
    parser.add_argument('--update',         nargs='*', dest='update',           action=Extender)

    parser.add_argument('--vol',            nargs='*', dest='volatility',       action=Extender)
    parser.add_argument('--draw',           nargs='*', dest='drawdown',         action=Extender)
    parser.add_argument('--loss',           nargs='*', dest='loss',             action=Extender)
    parser.add_argument('--liq',            nargs='*', dest='liquidity',        action=Extender)
    parser.add_argument('--exag',           nargs='*', dest='exageration',      action=Extender)
    parser.add_argument('--expos',          nargs='*', dest='exposure',         action=Extender)

    return parser.parse_args()


##############################################################################################

def run():

    if len(sys.argv) <=1:
        return

    args = new_parser(sys.argv[1:])

    if(args.select):
        data = mysql_conn.select(args.select[0])
    elif(args.update):
        data = mysql_conn.select(args.select[0])
    elif(args.insert):
        insert_mysql(args.insert)
    elif(args.performance):
        data = performance(args.performance)

    elif(args.mktvaleur):  # In Eur
        data = getMktValEur(args.mktvaleur)
    elif(args.mktvallocal):  # In Local ccy
        data = getMktValCcy(args.mktvallocal)
    elif(args.numshares):
        data = getNumShares(args.numshares)


    elif(args.volatility):
        data=volatility(args.volatility)
    elif(args.drawdown):
        data=drawdown(args.drawdown)
    elif(args.loss):
        data=loss(args.loss)
    elif(args.liquidity):
        data=liquidity(args.liquidity)
    elif(args.exageration):
        data=exageration(args.exageration)
    elif(args.exposure):
        data=exposure(args.exposure)

    mysql_conn.close()
    return data


##############################################################################################

if __name__ == "__main__":

    data = run()
    print(data)
    #exit the program
    sys.exit()
