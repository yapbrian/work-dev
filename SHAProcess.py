import sys
import shutil # used for DEX Index file
import os
from time import *
from datetime import *
import logging
import cx_Oracle # 64 bit oracle
import pyodbc # MS sql
import MySQLdb # MySQL
import xlrd # read excel file via python
import argparse
from HelperFunctions import *
from dataBaseFunctions import *

def isCanadianHoliday(date):
    CanadianHolidays = []
    CanadianHolidays.append("1/1/2012")
    CanadianHolidays.append("2/20/2012")
    CanadianHolidays.append("4/6/2012")
    CanadianHolidays.append("5/21/2012")
    CanadianHolidays.append("7/1/2012")
    CanadianHolidays.append("8/6/2012")
    CanadianHolidays.append("9/3/2012")
    CanadianHolidays.append("10/8/2012")
    CanadianHolidays.append("12/25/2012")
    CanadianHolidays.append("12/26/2012")
    CanadianHolidays.append("01/01/2013")
    CanadianHolidays.append("02/18/2013")
    CanadianHolidays.append("03/29/2013")
    CanadianHolidays.append("05/20/2013")
    CanadianHolidays.append("07/01/2013")
    CanadianHolidays.append("08/05/2013")
    CanadianHolidays.append("09/02/2013")
    CanadianHolidays.append("10/14/2013")
    CanadianHolidays.append("12/25/2013")
    CanadianHolidays.append("12/26/2013")
    CanadianHolidays.append("01/01/2014")
    CanadianHolidays.append("02/17/2014")
    CanadianHolidays.append("04/18/2014")
    CanadianHolidays.append("04/21/2014")
    CanadianHolidays.append("05/19/2014")
    CanadianHolidays.append("07/01/2014")
    CanadianHolidays.append("08/04/2014")
    CanadianHolidays.append("09/01/2014")
    CanadianHolidays.append("10/13/2014")
    CanadianHolidays.append("12/25/2014")
    CanadianHolidays.append("12/26/2014")
    CanadianHolidays.append("01/01/2015")
    CanadianHolidays.append("02/16/2015")
    CanadianHolidays.append("04/03/2015")
    CanadianHolidays.append("04/06/2015")
    CanadianHolidays.append("05/18/2015")
    CanadianHolidays.append("07/01/2015")
    CanadianHolidays.append("08/03/2015")
    CanadianHolidays.append("09/07/2015")
    CanadianHolidays.append("10/12/2015")
    CanadianHolidays.append("12/25/2015")
    CanadianHolidays.append("12/26/2015")
    CanadianHolidays.append("1/1/2016")
    CanadianHolidays.append("2/15/2016")
    CanadianHolidays.append("3/25/2016")
    CanadianHolidays.append("3/28/2016")
    CanadianHolidays.append("5/23/2016")
    CanadianHolidays.append("7/1/2016")
    CanadianHolidays.append("8/1/2016")
    CanadianHolidays.append("9/5/2016")
    CanadianHolidays.append("10/10/2016")
    CanadianHolidays.append("12/25/2016")
    CanadianHolidays.append("12/26/2016")
    CanadianHolidays.append("1/1/2017")
    CanadianHolidays.append("2/20/2017")
    CanadianHolidays.append("4/14/2017")
    CanadianHolidays.append("4/17/2017")
    CanadianHolidays.append("5/22/2017")
    CanadianHolidays.append("7/1/2017")
    CanadianHolidays.append("8/7/2017")
    CanadianHolidays.append("9/4/2017")
    CanadianHolidays.append("10/9/2017")
    CanadianHolidays.append("12/25/2017")
    CanadianHolidays.append("12/26/2017")
    CanadianHolidays.append("1/1/2018")
    CanadianHolidays.append("2/19/2018")
    CanadianHolidays.append("3/30/2018")
    CanadianHolidays.append("4/2/2018")
    CanadianHolidays.append("5/21/2018")
    CanadianHolidays.append("7/1/2018")
    CanadianHolidays.append("8/6/2018")
    CanadianHolidays.append("9/3/2018")
    CanadianHolidays.append("10/8/2018")
    CanadianHolidays.append("12/25/2018")
    CanadianHolidays.append("12/26/2018")
    CanadianHolidays.append("1/1/2019")
    CanadianHolidays.append("2/18/2019")
    CanadianHolidays.append("4/19/2019")
    CanadianHolidays.append("4/22/2019")
    CanadianHolidays.append("5/20/2019")
    CanadianHolidays.append("7/1/2019")
    CanadianHolidays.append("8/5/2019")
    CanadianHolidays.append("9/2/2019")
    CanadianHolidays.append("10/14/2019")
    CanadianHolidays.append("12/25/2019")
    CanadianHolidays.append("12/26/2019")
    CanadianHolidays.append("1/1/2020")
    CanadianHolidays.append("2/17/2020")
    CanadianHolidays.append("4/10/2020")
    CanadianHolidays.append("4/13/2020")
    CanadianHolidays.append("5/18/2020")
    CanadianHolidays.append("7/1/2020")
    CanadianHolidays.append("8/3/2020")
    CanadianHolidays.append("9/7/2020")
    CanadianHolidays.append("10/12/2020")
    CanadianHolidays.append("12/25/2020")
    CanadianHolidays.append("12/26/2020")

    if CanadianHolidays.__contains__(date.strftime('%m/%d/%Y')):
        return True
    else:
        return False

class shaProcess(object):

    def __init__(self):
        Enddate = date.today() - timedelta(1)
        while(Enddate.weekday() == 5 or Enddate.weekday() == 6 or isCanadianHoliday(Enddate) ): #ADJUSTING DATE, 6 IS SUNDAY, 5 IS SATURDAY
            Enddate = Enddate - timedelta(1)

        Startdate = Enddate - timedelta(1)
        while(Startdate.weekday() == 5 or Startdate.weekday() == 6 or isCanadianHoliday(Startdate)): #ADJUSTING DATE, 6 IS SUNDAY, 5 IS SATURDAY
            Startdate = Startdate - timedelta(1)

        self.StartDate = Startdate
        self.EndDate = Enddate

    def msSQLConnect(self):

        logFile = 'P:\\SHA\\Wei Wei\\CreditLogs\\SHA_Process.log'
        logging.basicConfig(filename = logFile, filemode='a', level = logging.DEBUG)

        dbConnectionStr = 'DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes'
        try:
            db = pyodbc.connect(dbConnectionStr)
        except pyodbc.Error, err:
            logging.warning("Database connection error. " + str(err))
            sys.exit()
        return db


    def sendEmail(self, database, dstEmail, headerStr, bodyText):

        logFile = 'P:\\SHA\\Wei Wei\\CreditLogs\\SHA_Process.log'
        logging.basicConfig(filename = logFile, filemode='a', level = logging.DEBUG)

        # uses MySQL GCMRW2
        mydb = database.mySQLConnect()
        cur = mydb.cursor()

        sqlStr = """
            INSERT INTO
                application.emailserver_requests(`to`, `subject`, `text`, `timestamp`, `from`)
            VALUES('{dstEmail}', '{header}', '{body}', now(), user())
            """.format(
                dstEmail = dstEmail,
                header = headerStr,
                body = bodyText
                )
        try:
            cur.execute(sqlStr)
            mydb.commit()
        except MySQLdb.Error,e:
            mydb.rollback()
            logging.warning(sqlStr + " " + str(e))
            sys.exit()
        return mydb

    def getDEXIndexID(self, cursor, indexDescription, term):
        sqlStr = """
            select
                DEXIndexID
            from
                SHA2.RISK.DEX_INDEX_LIST
            WHERE
                indexDescription = '{description}' AND
                Term = '{term}'
            """.format(
                description = indexDescription,
                term = term
            )

        try:
            cursor.execute(sqlStr)
            msdb.commit()
        except:
            pass
            # need to make the logging line work, currently hf is passed into
            # here
            #hf.printAndLog("Problem executing: " + sqlStr, False)

        DEXIndexID = cursor.fetchone()

        if DEXIndexID == None:
            return 0
        else:
            return DEXIndexID[0]

    def updateDEXDelta(self, database, view, delta, tenorid, priceDate, DEXAssetID):
        msdb = database.msSQLConnect()
        cur = msdb.cursor()

        sqlStr = """
            UPDATE SHA2.RISK.dex_{view} SET
            {view}_Native = {deltaValue}
            WHERE
            DEXAssetID = {assetid} AND
            PriceDate = '{pricedate}' AND
            tenorID = {tenorid}
            IF @@ROWCOUNT=0 INSERT INTO
            SHA2.RISK.dex_{view} (DEXAssetID, PriceDate, tenorID,
            {view}_Native) values({assetid}, '{pricedate}', {tenorid},
            {deltaValue})
            """.format(
                deltaValue = delta,
                assetid = DEXAssetID,
                pricedate = priceDate,
                tenorid = tenorid,
                view = view
            )
        try:
            cur.execute(sqlStr)
            msdb.commit()
        except:
            msdb.rollback()
            # TO DO:
            # need to make the logging line work, currently hf is passed into
            # here
            #hf.printAndLog("Problem executing: " + sqlStr, False)

        return True

    def to_float(self, s):
        retValue = 0
        try:
            retValue = float(s)
        except:
            retValue = 0

        return retValue

    def procOptionExpiryReminder(self):
        # TO DO:
        # use variables to replace all instances of days to option expiry for
        # which emails are done, basically 3 constants at top of this function

        # TO DO:
        # make a small database table to host option expiry DL list and punch
        # them in here
        #
        # the first 3 lines below must be exactly the way they are..indentation
        # matters
        CONST_HTML_EMAIL = """MIME-Version: 1.0
Content-Type: text/html
reply-to: dgalica@cppib.ca; wchen@cppib.ca; ktan@cppib.com; cpinkney@cppib.com; mflaemrich@cppib.com; byap@cppib.com; dwoolsey@cppib.com;
        <html>
        <head>
        <style>  #optMatToday  {  font-family:Trebuchet MS, Arial, Helvetica, sans-serif;  width:100%;  border-collapse:collapse; } #optMatToday td, #optMatToday th, #optMatToday caption { font-size:1em; border:1px solid #BF2048; padding:3px 7px 2px 7px; } #optMatToday th, #optMatToday caption { font-size:1.1em; text-align:left; padding-top:5px; padding-bottom:4px; background-color:#C74262 ; color:#ffffff; } #optMatToday tr.alt td  { color:#000000; background-color:#EAF2D3; }
        </style>
        </head>
        <body>
        """
        CONST_END_HTML_EMAIL = """
        </body></html>
        """
        CONST_TODAY_EXP_TABLE = """
        <table id=optMatToday>
        <tr bgcolor=#FF0000>
        <td colspan=9 align=center><b>Options Expiring Today</b></td>
        </tr>
        <tr bgcolor=#000000 align=center>
        <td><font color=#FFFFFF>Trade Number</td></font><td><font color=#FFFFFF>Security</td><td><font color=#FFFFFF>CPP Buy/Sell</td><td><font color=#FFFFFF>CallPut</td><td><font color=#FFFFFF>Strike</td><td><font color=#FFFFFF>Maturity</td><td><font color=#FFFFFF>Notional (M)</td><td><font color=#FFFFFF>Currency</td><td><font color=#FFFFFF>CounterParty</td>
        </tr>
        """
        CONST_2DAY_EXP_TABLE = """
        <table id=optMatToday>
        <tr bgcolor=#FFFF00>
        <td colspan=9 align=center><b>Options Expiring in 2 Days</b></td>
        </tr>
        <tr bgcolor=#000000 align=center>
        <td><font color=#FFFFFF>Trade Number</td></font><td><font color=#FFFFFF>Security</td><td><font color=#FFFFFF>CPP Buy/Sell</td><td><font color=#FFFFFF>CallPut</td><td><font color=#FFFFFF>Strike</td><td><font color=#FFFFFF>Maturity</td><td><font color=#FFFFFF>Notional (M)</td><td><font color=#FFFFFF>Currency</td><td><font color=#FFFFFF>CounterParty</td>
        </tr>
        """
        CONST_5DAY_EXP_TABLE = """
        <table id=optMatToday>
        <tr bgcolor=#66FF00>
        <td colspan=9 align=center><b>Options Expiring in 5 Days</b></td>
        </tr>
        <tr bgcolor=#000000 align=center>
        <td><font color=#FFFFFF>Trade Number</td></font><td><font color=#FFFFFF>Security</td><td><font color=#FFFFFF>CPP Buy/Sell</td><td><font color=#FFFFFF>CallPut</td><td><font color=#FFFFFF>Strike</td><td><font color=#FFFFFF>Maturity</td><td><font color=#FFFFFF>Notional (M)</td><td><font color=#FFFFFF>Currency</td><td><font color=#FFFFFF>CounterParty</td>
        </tr>
        """
       # example
       # <tr bgcolor=#E8E8E8 align=center><td>123456</td><td>CDX-NAIGS19V1-5Y</td><td>Put</td><td>0.98</td><td>2013-04-17</td><td>100</td><td>USD</td><td>GSI_LDN</td></tr>
        CONST_OPT_ROW = """
        <tr bgcolor={bgColor} align=center><td>{tradeNum}</td><td>{securityName}</td><td>{buysell}</td><td>{callput}</td><td>{strike}</td><td>{optMaturity}</td><td>{optNotional}</td><td>{optCcy}</td><td>{optCounterParty}</td></tr>
        """
        CONST_LINE_BRK = "<br>"
        CONST_LINE_BG_COLOR = ['#F8F8F8', '#E8E8E8']
        CONST_END_TABLE = "</table>"

        hdlr = logging.FileHandler('P:\\SHA\\Credit Group\\Risk Reporting\\process\\logs\\OptionExpiryReminder'+str(date.today().strftime("%Y%m%d"))+'.log')
        logger = logging.getLogger('logfile')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.DEBUG)

        hf = HelperFunctions(self.StartDate, self.EndDate, logger)

        db = dataBaseFunctions(self.StartDate, self.EndDate, logger)
        msdb = db.msSQLConnect()
        cur = msdb.cursor()

        today = date.today().strftime("%Y-%m-%d")

        # Get options w/ expiry dates greater than today

        sqlStr = """
            SELECT
                option_detail.tradeNum,
                asset_list.securityName,
                option_detail.CallPut,
                option_detail.strike,
                option_detail.maturity,
                option_detail.BuySell,
                Case
                    WHEN lower(option_detail.CallPut) = 'put' AND lower(option_detail.BuySell) = 'b' THEN abs(option_detail.notional) * (-1)
		            WHEN lower(option_detail.CallPut) = 'put' AND lower(option_detail.BuySell) = 's' THEN abs(option_detail.notional)
		            WHEN lower(option_detail.CallPut) = 'call' AND lower(option_detail.BuySell) = 'b' THEN abs(option_detail.notional)
		            WHEN lower(option_detail.CallPut) = 'call' AND lower(option_detail.BuySell) = 's' THEN abs(option_detail.notional) * (-1)
                End as Notional,
                map_currency.currencySymbol as Currency,
                map_counterparty.Description as CounterParty

            FROM
            sha2.risk.trade_detail_option option_detail

            inner join sha2.risk.trade_mkt_detail trade_mkt_detail on
            option_detail.tradeNum = trade_mkt_detail.tradeNum and trade_mkt_detail.PriceDate = '{riskDate}'

            left join sha2.risk.trade_detail trade_detail on
            option_detail.tradeNum = trade_detail.tradeNum and option_detail.assetID = trade_detail.assetID

            left join sha2.risk.asset_list asset_list on
            trade_detail.assetID = asset_list.assetID

            left join sha2.map.map_currency map_currency on
            asset_list.currencyID = map_currency.currencyID

            left join sha2.map.map_counterparty map_counterparty on
            trade_detail.counterpartyID = map_counterparty.counterpartyID

            where option_detail.maturity >= '{today}'
            """.format(today = today, riskDate = self.EndDate)

        try:
            cur.execute(sqlStr)
        except:
            hf.printAndLog("Error retriving option details", False)

        output = []
        outputHeaders = []
        outputData = []

        output = cur.fetchall()
        outputHeaders = [i[0] for i in cur.description]

        for eachRow in output:
            outputData.append(dict(zip(outputHeaders, eachRow)))

        today = datetime.today()

        # Create 3 lists of different emails, 1 for immediate, 1 for 2days, 1
        # for 5 days

        emailToSend = False

        optMatToday = []
        optMat2Days = []
        optMat5Days = []
        optMatTest = []

        for eachTrade in outputData:
            optMaturity = datetime.strptime(eachTrade["maturity"], "%Y-%m-%d")
            #optMaturity = datetime.strptime("2013-03-16", "%Y-%m-%d")

            timeToMaturity = (optMaturity - today).days + 1
            print timeToMaturity

            if timeToMaturity == 5:
                optMat5Days.append(eachTrade)

            elif timeToMaturity == 2:
                optMat2Days.append(eachTrade)

            elif timeToMaturity == 0:
                optMatToday.append(eachTrade)

            #elif timeToMaturity == 10:
            #    optMatTest.append(eachTrade)

            else:
                pass

        outGoingEmail = CONST_HTML_EMAIL
        # send just one email
        if optMatToday:
            print "Today"
            outGoingEmail += CONST_TODAY_EXP_TABLE

            for eachTrade in optMatToday:
                outGoingEmail += CONST_OPT_ROW.format(
                    bgColor = CONST_LINE_BG_COLOR[optMatToday.index(eachTrade) % 2],
                    tradeNum = eachTrade["tradeNum"],
                    securityName = eachTrade["securityName"],
                    buysell = eachTrade["BuySell"],
                    callput = eachTrade["CallPut"],
                    strike = eachTrade["strike"],
                    optMaturity = eachTrade["maturity"],
                    optNotional = (eachTrade["Notional"]/1000000),
                    optCcy = eachTrade["Currency"],
                    optCounterParty = eachTrade["CounterParty"]
                )

            outGoingEmail += CONST_END_TABLE
            outGoingEmail += CONST_LINE_BRK
            emailToSend = True

        if optMat2Days:
            print "2 days"
            outGoingEmail += CONST_2DAY_EXP_TABLE

            for eachTrade in optMat2Days:
                outGoingEmail += CONST_OPT_ROW.format(
                    bgColor = CONST_LINE_BG_COLOR[optMat2Days.index(eachTrade) % 2],
                    tradeNum = eachTrade["tradeNum"],
                    securityName = eachTrade["securityName"],
                    buysell = eachTrade["BuySell"],
                    callput = eachTrade["CallPut"],
                    strike = eachTrade["strike"],
                    optMaturity = eachTrade["maturity"],
                    optNotional = (eachTrade["Notional"]/1000000),
                    optCcy = eachTrade["Currency"],
                    optCounterParty = eachTrade["CounterParty"]
                )

            outGoingEmail += CONST_END_TABLE
            outGoingEmail += CONST_LINE_BRK
            emailToSend = True

        if optMat5Days:
            print "5 days"
            outGoingEmail += CONST_5DAY_EXP_TABLE

            for eachTrade in optMat5Days:
                outGoingEmail += CONST_OPT_ROW.format(
                    bgColor = CONST_LINE_BG_COLOR[optMat5Days.index(eachTrade) % 2],
                    tradeNum = eachTrade["tradeNum"],
                    securityName = eachTrade["securityName"],
                    buysell = eachTrade["BuySell"],
                    callput = eachTrade["CallPut"],
                    strike = eachTrade["strike"],
                    optMaturity = eachTrade["maturity"],
                    optNotional = (eachTrade["Notional"]/1000000),
                    optCcy = eachTrade["Currency"],
                    optCounterParty = eachTrade["CounterParty"]
                )

            outGoingEmail += CONST_END_TABLE
            #outGoingEmail += CONST_LINE_BRK
            emailToSend = True

        #if optMatTest:
        #    print "10 days"
        #    outGoingEmail += CONST_5DAY_EXP_TABLE

        #    for eachTrade in optMatTest:
        #        outGoingEmail += CONST_OPT_ROW.format(
        #            bgColor = CONST_LINE_BG_COLOR[optMatTest.index(eachTrade) % 2],
        #            tradeNum = eachTrade["tradeNum"],
        #            securityName = eachTrade["securityName"],
        #            callput = eachTrade["CallPut"],
        #            strike = eachTrade["strike"],
        #            optMaturity = eachTrade["maturity"],
        #            optNotional = (eachTrade["Notional"]/1000000),
        #            optCcy = eachTrade["Currency"],
        #            optCounterParty = eachTrade["CounterParty"]
        #        )

        #    outGoingEmail += '</tr>'
        #    outGoingEmail += CONST_END_TABLE
        #    outGoingEmail += CONST_LINE_BRK
        #    emailToSend = True

        outGoingEmail += CONST_END_HTML_EMAIL
        #print outGoingEmail

        if emailToSend == True:
            self.sendEmail(db, 'wchen@cppib.ca;dgalica@cppib.ca', 'Option Expiry Reminder', outGoingEmail)

        cur.close()
        msdb.close()

###########################################################################
#
# procUpdateDEXIndex
#
# copies the daily DEX index file from W:\pcbond\mail to
# P:\SHA\Credit Group\HG Credit\DEX All Governments Risk Profile\DEX data
# then uploads the market data to database (SHA2.RISK.DEX_INDEX_MKT_DETAIL
#
###########################################################################
    def procUpdateDEXIndex(self):

        CONST_NUM_OF_IDX_STATS = 13

        hdlr = logging.FileHandler('P:\\SHA\\Credit Group\\Risk Reporting\\process\\logs\\UpdateDEXIndex'+str(self.EndDate)+'.log')
        logger = logging.getLogger('logfile')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.DEBUG)

        hf = HelperFunctions(self.StartDate, self.EndDate, logger)

        hf.printAndLog("Copy DEX Index file started", True)

        # copy the DEX Index File from network drive to SHA folder
        newDEXFile = False

        srcFolder = 'W:\\pcbond\\mail\\'
        DEXFileName = 'canadian universe bond index summary.xls'
        dstFolder = 'P:\\SHA\\Credit Group\\HG Credit\\DEX All Governments Risk Profile\\DEX data\\'
        dstFileName = 'DEX ' + str(self.EndDate) + '.xls'

        lastMod = datetime.fromtimestamp(os.path.getmtime((srcFolder + DEXFileName))).date()

        if lastMod == self.EndDate:
            newDEXFile = True
        else:
            hf.printAndLog("DEX Index file NOT updated, exiting....", False)
            sys.exit()

        if (os.path.exists(srcFolder + DEXFileName)) & (newDEXFile == True):
            hf.printAndLog("DEX Index file found", False)
            try:
                shutil.copy(srcFolder + DEXFileName, dstFolder + dstFileName)
                hf.printAndLog("DEX Index file copy OK", False)
            except:
                raise
                hf.printAndLog("DEX Index file copy problem", False)
                sys.exit()
        else:
            hf.printAndLog("DEX Index file not found", False)
            sys.exit()

        # now read that copied DEX Index file and upload to database
        db = dataBaseFunctions(self.StartDate, self.EndDate, logger)
        msdb = db.msSQLConnect()
        cur = msdb.cursor()

        wb = xlrd.open_workbook(dstFolder + dstFileName)
        sh = wb.sheet_by_name(u'Universe Bond Index Statistics')

        # row and column start at 0,0

        # Assume Excel Row 14 has the "terms": SHORT, MID, LONG and UNIVERSE
        # Assume Excel Row 17 has the dates
        # doing some VERY trivial checks..

        # TO DO
        # if dates are not correct (ie, do not line up), need to handle that
        # case
        shortColIdx = 0
        shortDate = datetime(*xlrd.xldate_as_tuple(sh.cell(rowx = 16, colx = 2).value, wb.datemode)).date()
        shortTermCheck = sh.cell(rowx = 13, colx = 2).value
        if (shortDate == self.EndDate) and (shortTermCheck == 'SHORT TERM'):
            shortColIdx = 2 # ie. column C in Excel

        midColIdx = 0
        midDate = datetime(*xlrd.xldate_as_tuple(sh.cell(rowx = 16, colx = 4).value, wb.datemode)).date()
        midTermCheck = sh.cell(rowx = 13, colx = 4).value
        if (midDate == self.EndDate) and (midTermCheck == 'MID TERM'):
            midColIdx = 4 # ie. column E in Excel

        longColIdx = 0
        longDate = datetime(*xlrd.xldate_as_tuple(sh.cell(rowx = 16, colx = 6).value, wb.datemode)).date()
        longTermCheck = sh.cell(rowx = 13, colx = 6).value
        if (longDate == self.EndDate) and (longTermCheck == 'LONG TERM'):
            longColIdx = 6 # ie. column G in Excel

        universeColIdx = 0
        universeDate = datetime(*xlrd.xldate_as_tuple(sh.cell(rowx = 16, colx = 8).value, wb.datemode)).date()
        universeTermCheck = sh.cell(rowx = 13, colx = 8).value
        if (universeDate == self.EndDate) and (universeTermCheck == 'UNIVERSE'):
            universeColIdx = 8 # ie. column I in Excel

        # grab all indices from database, these will be used to check against
        # what's in the excel spreadsheet
        sqlStr = """
            select distinct(indexDescription) from sha2.risk.dex_index_list
        """
        try:
            cur.execute(sqlStr)
            msdb.commit()
        except:
            msdb.rollback()
            hf.printAndLog("Problem executing: " + sqlstr, False)

        DEXIndexList = cur.fetchall()

        DEXIndexList = [x[0] for x in DEXIndexList]

        numRows = sh.nrows - 1
        currRow = 0
        currIndex = 'blank'
        currStat = 'blank'

        shortStatDict = {}
        midStatDict = {}
        longStatDict = {}
        uniStatDict = {}

        while currRow < numRows:

            currRow += 1

            # once an Index is found, the next 13 rows should be index
            # statistics found in SHA2.RISK.DEX_INDEX_MKT_DETAIL
            if sh.cell(rowx = currRow, colx = 0).value in DEXIndexList:
                currIndex = sh.cell(rowx = currRow, colx = 0).value
                print currIndex

                # update each of the 13 statics for the index
                for i in xrange(CONST_NUM_OF_IDX_STATS):
                    currRow += 1

                    currStat = sh.cell(rowx = currRow, colx = 0).value
                    currStat = currStat.replace(".", "")
                    currStat = currStat.replace(" ", "")

                    if currStat == "#ISSUES":
                        shortStatDict['numOfIssues'] = sh.cell(rowx = currRow, colx = shortColIdx).value
                        midStatDict["numOfIssues"] = sh.cell(rowx = currRow, colx = midColIdx).value
                        longStatDict["numOfIssues"] = sh.cell(rowx = currRow, colx = longColIdx).value
                        uniStatDict["numOfIssues"] = sh.cell(rowx = currRow, colx = universeColIdx).value

                    elif currStat == "MKTVALUE(Billions)":
                        shortStatDict['mktValue'] = sh.cell(rowx = currRow, colx = shortColIdx).value
                        midStatDict["mktValue"] = sh.cell(rowx = currRow, colx = midColIdx).value
                        longStatDict["mktValue"] = sh.cell(rowx = currRow, colx = longColIdx).value
                        uniStatDict["mktValue"] = sh.cell(rowx = currRow, colx = universeColIdx).value

                    else:
                        shortStatDict[currStat] = sh.cell(rowx = currRow, colx = shortColIdx).value
                        midStatDict[currStat] = sh.cell(rowx = currRow, colx = midColIdx).value
                        longStatDict[currStat] = sh.cell(rowx = currRow, colx = longColIdx).value
                        uniStatDict[currStat] = sh.cell(rowx = currRow, colx = universeColIdx).value

                # now insert each of the 4 dictionaries

                # 1.update SHORT term
                sqlStr = "UPDATE SHA2.RISK.DEX_INDEX_MKT_DETAIL SET "

                columnHeaders = [x for x in shortStatDict.keys()]

                for key, value in shortStatDict.items():
                    sqlStr = sqlStr + key + " =" + str(value) + ","

                sqlStr = sqlStr[:-len(",")] # remove last semi-colon

                dexIndexID = self.getDEXIndexID(cur, currIndex, "SHORT")

                # this can be cleaned up....
                sqlStr = sqlStr + " WHERE DEXIndexID = " + str(dexIndexID)
                sqlStr = sqlStr + " and PriceDate = '" + str(shortDate) + "' "
                sqlStr += "if @@rowcount = 0 insert into SHA2.RISK.DEX_INDEX_MKT_DETAIL(DEXIndexID,PriceDate,"
                sqlStr += ','.join(columnHeaders)
                sqlStr += ") values("
                sqlStr += str(dexIndexID) + ",'" + str(shortDate) + "',"
                sqlStr += str(shortStatDict.values())[1:-1]
                sqlStr += ")"

                try:
                    cur.execute(sqlStr)
                    msdb.commit()
                except:
                    msdb.rollback()
                    hf.printAndLog("Problem executing: " + sqlStr, False)

                # 2.update MID term
                sqlStr = "UPDATE SHA2.RISK.DEX_INDEX_MKT_DETAIL SET "

                columnHeaders = [x for x in midStatDict.keys()]

                for key, value in midStatDict.items():
                    sqlStr = sqlStr + key + " =" + str(value) + ","

                sqlStr = sqlStr[:-len(",")] # remove last semi-colon

                dexIndexID = self.getDEXIndexID(cur, currIndex, "MID")

                # this can be cleaned up....
                sqlStr = sqlStr + " WHERE DEXIndexID = " + str(dexIndexID)
                sqlStr = sqlStr + " and PriceDate = '" + str(midDate) + "' "
                sqlStr += "if @@rowcount = 0 insert into SHA2.RISK.DEX_INDEX_MKT_DETAIL(DEXIndexID,PriceDate,"
                sqlStr += ','.join(columnHeaders)
                sqlStr += ") values("
                sqlStr += str(dexIndexID) + ",'" + str(midDate) + "',"
                sqlStr += str(midStatDict.values())[1:-1]
                sqlStr += ")"

                try:
                    cur.execute(sqlStr)
                    msdb.commit()
                except:
                    msdb.rollback()
                    hf.printAndLog("Problem executing: " + sqlStr, False)

                # 3.update LONG term
                sqlStr = "UPDATE SHA2.RISK.DEX_INDEX_MKT_DETAIL SET "

                columnHeaders = [x for x in longStatDict.keys()]

                for key, value in longStatDict.items():
                    sqlStr = sqlStr + key + " =" + str(value) + ","

                sqlStr = sqlStr[:-len(",")] # remove last semi-colon

                dexIndexID = self.getDEXIndexID(cur, currIndex, "LONG")

                # this can be cleaned up....
                sqlStr = sqlStr + " WHERE DEXIndexID = " + str(dexIndexID)
                sqlStr = sqlStr + " and PriceDate = '" + str(longDate) + "' "
                sqlStr += "if @@rowcount = 0 insert into SHA2.RISK.DEX_INDEX_MKT_DETAIL(DEXIndexID,PriceDate,"
                sqlStr += ','.join(columnHeaders)
                sqlStr += ") values("
                sqlStr += str(dexIndexID) + ",'" + str(longDate) + "',"
                sqlStr += str(longStatDict.values())[1:-1]
                sqlStr += ")"

                try:
                    cur.execute(sqlStr)
                    msdb.commit()
                except:
                    msdb.rollback()
                    hf.printAndLog("Problem executing: " + sqlStr, False)

                # 4.update UNIVERSE term
                sqlStr = "UPDATE SHA2.RISK.DEX_INDEX_MKT_DETAIL SET "

                columnHeaders = [x for x in uniStatDict.keys()]

                for key, value in uniStatDict.items():
                    sqlStr = sqlStr + key + " =" + str(value) + ","

                sqlStr = sqlStr[:-len(",")] # remove last semi-colon

                dexIndexID = self.getDEXIndexID(cur, currIndex, "UNIVERSE")

                # this can be cleaned up....
                sqlStr = sqlStr + " WHERE DEXIndexID = " + str(dexIndexID)
                sqlStr = sqlStr + " and PriceDate = '" + str(universeDate) + "' "
                sqlStr += "if @@rowcount = 0 insert into SHA2.RISK.DEX_INDEX_MKT_DETAIL(DEXIndexID,PriceDate,"
                sqlStr += ','.join(columnHeaders)
                sqlStr += ") values("
                sqlStr += str(dexIndexID) + ",'" + str(universeDate) + "',"
                sqlStr += str(uniStatDict.values())[1:-1]
                sqlStr += ")"

                try:
                    cur.execute(sqlStr)
                    msdb.commit()
                except:
                    msdb.rollback()
                    hf.printAndLog("Problem executing: " + sqlStr, False)
            else:
                # this row is of no interest to me
                pass


    def procUpdateDEX(self):

        dataStartsONRow = 6 # excel row #, kinda silly...

        #path = "P:\\GCM\\Fixed Income Portfolio\\Nominal Bonds\Marketable Nominal Bonds\\Canada Bonds\\Rebalancing Files\\Templates\\SHA\\"
        path = "P:\SHA\Credit Group\HG Credit\DEX All Governments Risk Profile\SHA_HG_DEX_BM\\"
        fileName = "DEXData.xlsx"

        #DEXNominalPath = "P:\\SHA\\Credit Group\\Risk Reporting\\beta reports\\"
        #DEXNominalFileName = "DEX Index Rep Nominal.xlsx"

        hdlr = logging.FileHandler('P:\\SHA\\Credit Group\\Risk Reporting\\process\\logs\\UpdateDEX'+str(self.EndDate)+'.log')
        logger = logging.getLogger('logfile')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.DEBUG)

        hf = HelperFunctions(self.StartDate, self.EndDate, logger)

        hf.printAndLog("Update DEX started", True)

        if os.path.exists(path + fileName):
            hf.printAndLog("File Found", False)
        else:
            hf.printAndLog("DEXData.xls not found", False)
            sys.exit()

        #if os.path.exists(DEXNominalPath + DEXNominalFileName):
        #    hf.printAndLog("DEX Index Rep Nominal.xlsx File Found", False)
        #else:
        #    hf.printAndLog("DEX Index Rep Nominal.xlsx not found", False)
        #    sys.exit()

        db = dataBaseFunctions(self.StartDate, self.EndDate, logger)
        msdb = db.msSQLConnect()
        cur = msdb.cursor()

        DEXAssetListMap = db.generateMap("DEXAssetListMap.mapquery")
        SPRatingMap = db.generateMap("ratingMap.mapquery")
        tenorMap = db.generateMap("tenorMap.mapquery")

        # Get DEX Index Rep nominal value
        #wb = xlrd.open_workbook(DEXNominalPath + DEXNominalFileName)
        #sh = wb.sheet_by_name(u'DEX')

        #DEXRepNominal = sh.cell(0,1).value

        wb = xlrd.open_workbook(path + fileName)
        sh = wb.sheet_by_name(u'Portfolio')

        row1Header = []
        row2Header = []

        row1Header = sh.row_values(0)
        row2Header = sh.row_values(1)

        # use "Price" column to check date
        pcbondDate = row2Header[row1Header.index("Price")]
        priceDate = date(int(pcbondDate[0:4]), int(pcbondDate[4:6]), int(pcbondDate[6:8]))


        if priceDate != self.EndDate:
            logMsg = "Price Date of {pricedate} in DEXData.xlsx is not expected date of {expectedDate}".format(
                    pricedate = str(priceDate),
                    expectedDate = str(self.EndDate)
            )
            hf.printAndLog(logMsg, False)
            sys.exit()

        header = []
        output=  []
        correctFile = False

        header = [x + " " + y for x,y in zip(row1Header, row2Header)]
        header = [x.strip() for x in header]

        DEXIndexNotional = sum([item for item in sh.col_values(header.index("Current Holdings")) if hf.isNumber(item)])




        ################ Getting DEX FMV ################

        # this line below (commented) out if using DEX provided FMV, we dont
        # use this due to amortizers in the DEX index. go to "DEX FMV and Amortizers.xlsx"
        # for more analysis. We use calculated FMV to derive SHA notionals etc
        # etc

        #DEXIndexMktValue = sum([item for item in sh.col_values(header.index("FX+MktVal Current")) if hf.isNumber(item)])

        # here are the NEW method to calculate DEX Bond FMV (due to amortizers)
        notional = [item for item in sh.col_values(header.index("Current Holdings")) if hf.isNumber(item)]
        cleanPrice = [item for item in sh.col_values(header.index("Price " + pcbondDate)) if hf.isNumber(item)]
        cleanPrice.pop(0)
        accrual = [item for item in sh.col_values(header.index("Accrued Interest")) if hf.isNumber(item)]
        dirtyPrice = []
        DEXIndexMktValue = 0

        # this is how you will check for errors

        if (len(cleanPrice) != len(accrual) or len(cleanPrice) != len(notional)):
            logMsg = "Number of Clean Price, Accrued Interest and Current Holdings in the DEX Bond file do not match. Exiting......."
            hf.printAndLog(logMsg, False)
            sys.exit()

        for index, eachPrice in enumerate(cleanPrice):
            dirtyPrice.append(eachPrice + accrual[index])

        for index, eachDirtyPrice in enumerate(dirtyPrice):
            calculatedSecurityFMV = eachDirtyPrice / 100 * notional[index]
            #print calculatedSecurityFMV
            DEXIndexMktValue += calculatedSecurityFMV

        ################ Done Getting DEX FMV ################





        # Calculate Replication Portfolio Nominal holding
        sqlStr = """
            execute sha2.[CPPIB\wchen].procGetNotionalDelta '{pricedate}', 'BOND', 'DEX ALL GOV'
        """.format(pricedate = self.EndDate)

        try:
            cur.execute(sqlStr)
            msdb.commit()
        except:
            msdb.rollback()
            hf.printAndLog("Problem executing: " + sqlstr, False)

        columnHeaders = [i[0] for i in cur.description]

        resultSet = cur.fetchone()

        my_dict = dict(zip(columnHeaders, resultSet))

        DEXUnit = my_dict["Notional_Native"] / 1000

        # get DEX close level
        sqlStr = """
            SELECT
                totalReturn
            FROM
                sha2.risk.dex_index_mkt_detail mkt_detail
                inner join sha2.risk.dex_index_list index_list
                on mkt_detail.DEXIndexID = index_list.DEXIndexID
            WHERE
                index_list.Term = 'UNIVERSE' and
                index_list.indexDescription = 'All Governments' and
                mkt_detail.PriceDate = '{date}'
            """.format(date = self.EndDate)

        try:
            cur.execute(sqlStr)
            msdb.commit()
        except:
            hf.printAndLog("Problem executing: " + sqlStr, False)

        DEXTotalReturn = cur.fetchone()
        # TO DO:
        # if there are no dex level then we shouln't proceed...
        if DEXTotalReturn == None:
            hf.printAndLog("DEX Total Return was not found in database, DEX bonds NOT updated", False)
            sys.exit()
        else:
            DEXTotalReturn = DEXTotalReturn[0]

        DEXRepNominal = DEXTotalReturn * (-1) * DEXUnit * 1000 / 100 / (DEXIndexMktValue / DEXIndexNotional)

        # find the extra rows and extra dates/error check
        # this is a SHORT loop
        for rownum in range(sh.nrows):
            if rownum > 0:
                if str(sh.cell(rownum, 0).value) == "DOMESTIC BOND INDICES - UNIVERSE - All Governments":
                    correctFile = True
                    break

        if not correctFile:
            hf.printAndLog("DEX File was Not correct. Database not updated", False)
            sys.exit()

        # start getting values

        for rownum in range(sh.nrows):
            if rownum > dataStartsONRow - 2:
                output.append(dict(zip(header, sh.row_values(rownum))))

        for eachRow in output:

            # updating SHA2.RISK.DEX_LIST
            year, month, day = xlrd.xldate_as_tuple(eachRow["Maturity Date"], wb.datemode)[0:3]
            maturity = str(month) + "/" + str(day) + "/" + str(year)

            CUSIP = eachRow["CUSIP"]
            Issuer = eachRow["Issue Name"].replace("'","") # replace ' with nothing
            Security = str(eachRow["Issue Name"].replace("'","")) + " " + str(eachRow["Coupon"]) + " " + maturity
            Sector = eachRow["Industry Group"]
            SubSector = eachRow["Industry SubGroup"]
            SPRatingID = SPRatingMap[str(eachRow["PC-Bond Rating"])]

            # grab PCBond Security ID
            try:
                PCBondID = int(eachRow["Security ID Num."])
            except:
                PCBondID = 0
                logMsg = "Could not get PCBond Security ID Num. for {Security}, CUSIP = {cusip}".format(Security = Security, cusip = CUSIP)
                hf.printAndLog(logMsg, False)

            # grab PCBond Benchmark security ID
            try:
                PCBondBenchID = int(eachRow["Trader Bench ID"])
            except:
                PCBondBenchID = 0
                logMsg = "Could not get PCBond Benchmark Security ID for {Security}, CUSIP = {cusip}".format(Security = Security, cusip = CUSIP)
                hf.printAndLog(logMsg, False)

            try:
                DEXAssetID = DEXAssetListMap[str(CUSIP)]
            except:
                DEXAssetID = db.getLastRow("dex_list") + 1
                logMsg = "New DEX item, security = {Security}, CUSIP = {cusip}, New DEXAssetID = {assetid}".format(Security = Security, cusip = CUSIP, assetid = DEXAssetID)
                hf.printAndLog(logMsg, False)

            sqlStr = """
                UPDATE SHA2.RISK.dex_list SET
                Issuer = '{issuer}',
                Security = '{security}',
                Sector = '{sector}',
                SubSector = '{subsector}',
                SPRatingID = {ratingID},
                PCBondID = {pcbondid},
                PCBondBenchID = {pcbondbenchid}
                WHERE
                DEXAssetID = {assetID} AND
                CUSIP = '{cusip}'
                IF @@ROWCOUNT = 0 INSERT INTO
                SHA2.RISK.dex_list(DEXAssetID, CUSIP, Issuer, Security,
                Sector, SubSector, SPRatingID, PCBondID, PCBondBenchID)
                values({assetID}, '{cusip}', '{issuer}', '{security}',
                '{sector}', '{subsector}', {ratingID}, {pcbondid},
                {pcbondbenchid})
                """.format(
                    issuer = Issuer,
                    security = Security,
                    sector = Sector,
                    subsector = SubSector,
                    ratingID = SPRatingID,
                    cusip = CUSIP,
                    assetID = DEXAssetID,
                    pcbondid = PCBondID,
                    pcbondbenchid = PCBondBenchID
                    )

            try:
                cur.execute(sqlStr)
                msdb.commit()
            except:
                msdb.rollback()
                hf.printAndLog("Problem executing: " + sqlstr, False)

            # updating SHA2.RISK.DEX_MKT_DETAIL
            Yield = eachRow["Yield " + pcbondDate]
            bondPrice = eachRow["Price " + pcbondDate]
            accruedInterest = eachRow["Accrued Interest"]
            MacDuration = eachRow["Macaulay Duration"]
            ModDuration = eachRow["Modified Duration"]
            DollarDuration = eachRow["Dollar Duration"]
            Notional = eachRow["Current Holdings"]
            PV01 = eachRow["Value of .01%"]

            SprdToBenchmark = eachRow["TraderSprd Bench"]
            if not hf.isNumber(SprdToBenchmark):
                SprdToBenchmark = 0

            JTD = -(Notional / DEXIndexNotional) * DEXRepNominal * (1 - 0.4)

            SHAEquivalentNotional = -(Notional / DEXIndexNotional) * DEXRepNominal

            sqlStr = """
                UPDATE SHA2.RISK.dex_mkt_detail SET
                CleanPrice = {price},
                AccruedInterest = {accruedinterest},
                Yield = {bondyield},
                PV01 = {pv01},
                MacaulayDuration = {macduration},
                ModifiedDuration = {modduration},
                DollarDuration = {dollarduration},
                SprdToBench = {sprdtobench},
                Notional = {notional},
                DEXRepNominal = {dexrepnominal},
                SHAJumpToDefault = {jtd},
                SHAEquivalentNotional = {shaequivalentnotional}
                WHERE
                DEXAssetID = {assetid} AND
                PriceDate = '{pricedate}'
                IF @@ROWCOUNT=0 INSERT INTO
                SHA2.RISK.dex_mkt_detail(DEXAssetID, PriceDate, CleanPrice,
                AccruedInterest, Yield,
                PV01, MacaulayDuration, ModifiedDuration, DollarDuration,
                SprdToBench, Notional, DEXRepNominal, SHAJumpToDefault, SHAEquivalentNotional)
                values({assetid}, '{pricedate}', {price}, {accruedinterest}, {bondyield}, {pv01},
                {macduration}, {modduration}, {dollarduration}, {sprdtobench}, {notional},
                {dexrepnominal}, {jtd}, {shaequivalentnotional})
                """.format(
                    price = bondPrice,
                    accruedinterest = accruedInterest,
                    bondyield = Yield,
                    macduration = MacDuration,
                    modduration = ModDuration,
                    dollarduration = DollarDuration,
                    sprdtobench = SprdToBenchmark,
                    notional = Notional,
                    dexrepnominal = DEXRepNominal,
                    jtd = JTD,
                    shaequivalentnotional = SHAEquivalentNotional,
                    assetid = DEXAssetID,
                    pricedate = priceDate,
                    pv01 = PV01
                )

            try:
                cur.execute(sqlStr)
                msdb.commit()
            except:
                msdb.rollback()
                hf.printAndLog("Problem executing: " + sqlStr, False)

            # updating SHA2.RISK.DEX_DV01 and SHA2.RISK.DEX_CR01
            DV01_3M = self.to_float(eachRow["KeyRate Dur 0.25"])
            DV01_6M = self.to_float(eachRow["KeyRate Dur 0.5"])
            DV01_1Y = self.to_float(eachRow["KeyRate Dur 0.75"]) + self.to_float(eachRow["KeyRate Dur 1.0"]) + self.to_float(eachRow["KeyRate Dur 1.25"])
            DV01_2Y = self.to_float(eachRow["KeyRate Dur 1.5"]) + self.to_float(eachRow["KeyRate Dur 1.75"]) + self.to_float(eachRow["KeyRate Dur 2"])
            DV01_3Y = self.to_float(eachRow["KeyRate Dur 2.5"]) + self.to_float(eachRow["KeyRate Dur 3"])
            DV01_4Y = self.to_float(eachRow["KeyRate Dur 3.5"]) + self.to_float(eachRow["KeyRate Dur 4"])
            DV01_5Y = self.to_float(eachRow["KeyRate Dur 4.5"]) + self.to_float(eachRow["KeyRate Dur 5"])
            DV01_6Y = self.to_float(eachRow["KeyRate Dur 5.5"]) + self.to_float(eachRow["KeyRate Dur 6"])
            DV01_7Y = self.to_float(eachRow["KeyRate Dur 7"])
            DV01_8Y = self.to_float(eachRow["KeyRate Dur 8"])
            DV01_9Y = self.to_float(eachRow["KeyRate Dur 9"])
            DV01_10Y = self.to_float(eachRow["KeyRate Dur 10"])
            DV01_12Y = self.to_float(eachRow["KeyRate Dur 11"]) + self.to_float(eachRow["KeyRate Dur 12"]) + self.to_float(eachRow["KeyRate Dur 13"])
            DV01_15Y = self.to_float(eachRow["KeyRate Dur 14"]) + self.to_float(eachRow["KeyRate Dur 15"]) + self.to_float(eachRow["KeyRate Dur 16"]) + self.to_float(eachRow["KeyRate Dur 17"])
            DV01_20Y = self.to_float(eachRow["KeyRate Dur 18"]) + self.to_float(eachRow["KeyRate Dur 19"]) + self.to_float(eachRow["KeyRate Dur 20"]) + self.to_float(eachRow["KeyRate Dur 21"]) + self.to_float(eachRow["KeyRate Dur 22"]) + self.to_float(eachRow["KeyRate Dur 23"]) + self.to_float(eachRow["KeyRate Dur 24"])
            DV01_30Y = self.to_float(eachRow["KeyRate Dur 25"]) + self.to_float(eachRow["KeyRate Dur 26"]) + self.to_float(eachRow["KeyRate Dur 27"]) + self.to_float(eachRow["KeyRate Dur 28"]) + self.to_float(eachRow["KeyRate Dur 29"]) + self.to_float(eachRow["KeyRate Dur 30"])

            deltas = [DV01_3M, DV01_6M, DV01_1Y, DV01_2Y, DV01_3Y, DV01_4Y, DV01_5Y, DV01_6Y, DV01_7Y, DV01_8Y, DV01_9Y, DV01_10Y, DV01_12Y, DV01_15Y, DV01_20Y, DV01_30Y]

            scaledDelta = [((PV01 * x) / sum(deltas)) for x in deltas]

            tenorID = [tenorMap["3M"], tenorMap["6M"], tenorMap["1Y"], tenorMap["2Y"], tenorMap["3Y"], tenorMap["4Y"], tenorMap["5Y"], tenorMap["6Y"], tenorMap["7Y"], tenorMap["8Y"], tenorMap["9Y"], tenorMap["10Y"], tenorMap["12Y"], tenorMap["15Y"], tenorMap["20Y"], tenorMap["30Y"]]

            aggList = zip(scaledDelta, tenorID)

            for deltaValue, tenor in aggList:
                self.updateDEXDelta(db, "DV01", deltaValue, tenor, priceDate, DEXAssetID)

            if (Sector != "Federal") or (SubSector != "Non-Agency"):
                CR01_6M = DV01_6M + DV01_3M
                CR01_1Y = DV01_1Y
                CR01_2Y = DV01_2Y
                CR01_3Y = DV01_3Y
                CR01_4Y = DV01_4Y
                CR01_5Y = DV01_5Y + (DV01_6Y/2)
                CR01_7Y = DV01_7Y + DV01_8Y + (DV01_6Y/2)
                CR01_10Y = DV01_9Y + DV01_10Y + DV01_12Y
                CR01_15Y = DV01_15Y
                CR01_20Y = DV01_20Y
                CR01_30Y = DV01_30Y

                creditDeltas = [CR01_6M, CR01_1Y, CR01_2Y, CR01_3Y,
                        CR01_4Y, CR01_5Y, CR01_7Y, CR01_10Y, CR01_15Y,
                        CR01_20Y, CR01_30Y]

                scaledCreditDelta = [((PV01 * x) / sum(creditDeltas)) for x in creditDeltas]

                creditTenorID = [tenorMap["6M"], tenorMap["1Y"], tenorMap["2Y"], tenorMap["3Y"], tenorMap["4Y"], tenorMap["5Y"], tenorMap["7Y"], tenorMap["10Y"], tenorMap["15Y"], tenorMap["20Y"], tenorMap["30Y"]]
                creditAggList = zip(scaledCreditDelta, creditTenorID)

                for deltaValue, tenor in creditAggList:
                    self.updateDEXDelta(db, "CR01", deltaValue, tenor, priceDate, DEXAssetID)

    def procUpdateIndexNotional(self):

        # update tranche notional

        path = "P:\\SHA\\Credit Group\\Risk Reporting\\beta reports\\"
        fileName = "Database Index Override.xlsm"

        hdlr = logging.FileHandler('P:\\SHA\\Credit Group\\Risk Reporting\\process\\logs\\UpdateIndex'+str(self.EndDate)+'.log')
        logger = logging.getLogger('logfile')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.DEBUG)

        hf = HelperFunctions(self.StartDate, self.EndDate, logger)

        hf.printAndLog("Update Index started", True)

        if os.path.exists(path + fileName):
            hf.printAndLog("File Found", False)
        else:
            hf.printAndLog("Database Index Override.xlsm not found", False)
            sys.exit()

        db = dataBaseFunctions(self.StartDate, self.EndDate, logger)
        msdb = db.msSQLConnect()
        cur = msdb.cursor()

        wb = xlrd.open_workbook(path + fileName)
        sh = wb.sheet_by_name(u'Index')

        header = []
        header = sh.row_values(0)

        isHeader = True
        output = []

        for rownum in range(sh.nrows):
            if rownum > 0:
                output.append(dict(zip(header, sh.row_values(rownum))))

        for eachRow in output:
            tradeNum = eachRow["Index"]
            assetID = eachRow["AssetID"]
            notional = eachRow["Notional"]

            #sqlStr = """
            #    UPDATE SHA2.RISK.TRADE_DETAIL
            #    SET notional = 0
            #    WHERE assetID = {assetID}
            #    """.format(
            #        assetID = assetID
            #        )
            #print sqlStr

            #try:
            #    pass
            #    cur.execute(sqlStr)
            #    msdb.commit()
            #except:
            #    msdb.rollback()
            #    hf.printAndLog("Problem executing: " + sqlStr, False)


            sqlStr = """
                UPDATE SHA2.RISK.TRADE_DETAIL
                SET notional = {notionalAmt}
                WHERE tradeNum = {tradeNumber} AND
                assetID = {assetID}
                """.format(
                    notionalAmt = notional,
                    tradeNumber = tradeNum,
                    assetID = assetID
                    )
            print sqlStr


            try:
                pass
                cur.execute(sqlStr)
                msdb.commit()
            except:
                msdb.rollback()
                hf.printAndLog("Problem executing: " + sqlStr, False)


# update tranche delta
        sh = wb.sheet_by_name(u'Delta')

        header = []
        header = sh.row_values(0)

        isHeader = True
        output = []

        for rownum in range(sh.nrows):
            if rownum > 0:
                output.append(dict(zip(header, sh.row_values(rownum))))

        for eachRow in output:
            tradeNum = eachRow["Index"]
            assetID = eachRow["AssetID"]
            delta = eachRow["Delta"]
            tenor = eachRow["Tenor"]

            #sqlStr = """
            #    UPDATE SHA2.RISK.CREDIT_CR01
            #    SET CR01_Native = 0
            #    WHERE assetID = {assetID}
            #    AND tenorID = {tenor}
            #    AND PriceDate = (
            #        Select Max(PriceDate) from SHA2.RISK.CREDIT_CR01
            #        )
            #    """.format(
            #        assetID = assetID,
            #        tenor = tenor
            #        )
            #print sqlStr

            #try:
            #    pass
            #    cur.execute(sqlStr)
            #    msdb.commit()
            #except:
            #    msdb.rollback()
            #    hf.printAndLog("Problem executing: " + sqlStr, False)

            sqlStr = """
                UPDATE SHA2.RISK.CREDIT_CR01
                SET CR01_Native = {deltaAmt}
                WHERE tradeNum = {tradeNumber}
                AND assetID = {assetID}
                AND tenorID = {tenor}
                AND PriceDate = '{pricedate}'
		IF @@ROWCOUNT=0 INSERT INTO
		SHA2.RISK.CREDIT_CR01(tradeNum, assetID, PriceDate, tenorID, CR01_Native)
		values({tradeNumber}, {assetID},'{pricedate}', {tenor}, {deltaAmt})
                """.format(
		    pricedate = self.EndDate,
                    deltaAmt = delta,
                    tradeNumber = tradeNum,
                    assetID = assetID,
                    tenor = tenor
                    )
            print sqlStr

            try:
                cur.execute(sqlStr)
                msdb.commit()
            except:
                msdb.rollback()
                hf.printAndLog("Problem executing: " + sqlStr, False)

    def procBulkInsertCreditRVCash(self):

        hdlr = logging.FileHandler('P:\\SHA\\Credit Group\\Risk Reporting\\process\\logs\\BulkInsertCreditRVCash'+str(self.EndDate)+'.log')
        logger = logging.getLogger('logfile')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.DEBUG)

        oHF = HelperFunctions(self.StartDate, self.EndDate, logger)

        db = dataBaseFunctions(self.StartDate, self.EndDate, logger)

        msdb = db.msSQLConnect()
        cur = msdb.cursor()

        sqlStr = """
            BULK INSERT SHA2.RISK.creditrv_cashflows FROM '\\\pmi-fss\\public markets\SHA\Credit Group\Risk Reporting\process\logs\cashflow\CF{pricedate}.csv' WITH (fieldterminator = ',', rowterminator = '\n')
            """.format(pricedate = self.EndDate)
        print sqlStr

        try:
            cur.execute(sqlStr)
            msdb.commit()
        except Exception as e:
            #msdb.rollback()
            oHF.printAndLog("Problem executing: " + sqlStr + "\n" + str(e), False)

    def procBulkInsertDV01(self):

        hdlr = logging.FileHandler('P:\\SHA\\Credit Group\\Risk Reporting\\process\\logs\\BulkInsertDV01'+str(self.EndDate)+'.log')
        logger = logging.getLogger('logfile')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.DEBUG)

        oHF = HelperFunctions(self.StartDate, self.EndDate, logger)

        db = dataBaseFunctions(self.StartDate, self.EndDate, logger)

        msdb = db.msSQLConnect()
        cur = msdb.cursor()

        sqlStr = """
            BULK INSERT SHA2.RISK.CREDIT_DV01 FROM '\\\pmi-fss\\public markets\SHA\Credit Group\Risk Reporting\process\logs\DV01\DV01{pricedate}.txt' WITH (fieldterminator = ',', rowterminator = '\\n')
            """.format(pricedate = self.EndDate)
        print sqlStr

        try:
            cur.execute(sqlStr)
            msdb.commit()
        except Exception as e:
            #msdb.rollback()
            oHF.printAndLog("Problem executing: " + sqlStr + "\n" + str(e), False)


    def procBulkInsertCR01(self):

        hdlr = logging.FileHandler('P:\\SHA\\Credit Group\\Risk Reporting\\process\\logs\\BulkInsertCR01'+str(self.EndDate)+'.log')
        logger = logging.getLogger('logfile')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.DEBUG)

        oHF = HelperFunctions(self.StartDate, self.EndDate, logger)

        db = dataBaseFunctions(self.StartDate, self.EndDate, logger)

        msdb = db.msSQLConnect()
        cur = msdb.cursor()

        sqlStr = """
            BULK INSERT SHA2.RISK.CREDIT_CR01 FROM '\\\pmi-fss\\public markets\SHA\Credit Group\Risk Reporting\process\logs\CR01\cr01{pricedate}.txt' WITH (fieldterminator = ',', rowterminator = '\\n')
            """.format(pricedate = self.EndDate)
        print sqlStr

        try:
            cur.execute(sqlStr)
            msdb.commit()
        except Exception as e:
            #msdb.rollback()
            oHF.printAndLog("Problem executing: " + sqlStr + "\n" + str(e), False)

        self.procUpdateIndexNotional()


    def procUpdateSHASector(self):
        # overrides ICB sector by issuer to move names amongst traders

        path = "P:\\SHA\\Credit Group\\Risk Reporting\\beta reports\\"
        fileName = "SHA Sector.xlsm"

        hdlr = logging.FileHandler('P:\\SHA\\Credit Group\\Risk Reporting\\process\\logs\\UpdateSHASector'+str(self.EndDate)+'.log')
        logger = logging.getLogger('logfile')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.DEBUG)

        hf = HelperFunctions(self.StartDate, self.EndDate, logger)

        hf.printAndLog("Update SHA sector started", True)

        if os.path.exists(path + fileName):
            hf.printAndLog("File Found", False)
        else:
            hf.printAndLog("SHA Sector.xlsm not found", False)
            sys.exit()

        db = dataBaseFunctions(self.StartDate, self.EndDate, logger)
        sectorMap = db.generateMap("sectorMap.mapquery")
        referenceEntityMap = db.generateMap("referenceEntityMap.mapquery")
        # there is another ref entity with identical name but not the one we
        # want
        referenceEntityMap['WEATHERFORD INTL LTD'] = 178
        issuerExceptionMap = db.generateMap("issuerExceptionMap.mapquery")

        msdb = db.msSQLConnect()
        cur = msdb.cursor()

        wb = xlrd.open_workbook(path + fileName)
        sh = wb.sheet_by_name(u'SHASector')

        header = []
        header = sh.row_values(0)

        isHeader = True
        output = []

        for rownum in range(sh.nrows):
            if rownum > 0:
                output.append(dict(zip(header, sh.row_values(rownum))))

        for eachRow in output:

            issuer = eachRow["issuer"]

            if referenceEntityMap.has_key(issuer):
                if issuer in issuerExceptionMap:
                    eachRow["ISSUER"] = str(issuerExceptionMap[issuer]).upper()

                try:
                    eachRow["ICB_SECTOR"] = sectorMap[eachRow["ICB_SECTOR"]]
                    eachRow["SHA_SECTOR"] = sectorMap[eachRow["SHA_SECTOR"]]
                except:
                    hf.printAndLog(issuer + " has unrecognized sector. ICB_SECTOR = " + eachRow["ICB_SECTOR"] + " SHA_SECTOR = " + eachRow["SHA_SECTOR"], False)
                    continue

                sqlStr = """
                    UPDATE SHA2.risk.asset_list
                    set SHAsectorID = {SHASectorID}
                    WHERE issuer = '{issuerName}'
                    """.format(
                        SHASectorID = eachRow["SHA_SECTOR"],
                        issuerName = eachRow["issuer"]
                    )

                try:
                    cur.execute(sqlStr)
                    msdb.commit()
                except:
                    msdb.rollback()
                    hf.printAndLog("Problem executing: " + sqlStr, False)

            else:
                hf.printAndLog(issuer + " not found in SHA2.cds_mgmt.map_referenceentity. Sector not changed..", False)

        msdb.close()
        hf.printAndLog("Update SHA sector completed", False)

def main():

    actionList = ['procOptionExpiryReminder',
                  'procUpdateDEX',
                  'procUpdateIndexNotional',
                  'procUpdateSHASector',
                  'procBulkInsertCR01',
                  'procBulkInsertDV01',
                  'procBulkInsertCreditRVCash',
                  'procUpdateDEXIndex'
                  ]

    # argument parsing
    parser = argparse.ArgumentParser(description='SHA Credit Processes')
    parser.add_argument('-p', '--action', action='store', nargs='+', choices=actionList, required=True, help='Allowed action are:'.join(actionList))

    args = parser.parse_args()

    print args.action

    shaProcessObj = shaProcess()

    for action in args.action:
        if action == 'procOptionExpiryReminder':
            shaProcessObj.procOptionExpiryReminder()
            pass

        elif action == 'procUpdateDEX':
            shaProcessObj.procUpdateDEX()

        elif action == 'procUpdateIndexNotional':
            shaProcessObj.procUpdateIndexNotional()
            pass

        elif action == 'procUpdateSHASector':
            shaProcessObj.procUpdateSHASector()
            pass
        elif action == 'procBulkInsertCR01':
            shaProcessObj.procBulkInsertCR01()
            pass
        elif action == 'procBulkInsertDV01':
            shaProcessObj.procBulkInsertDV01()
            pass
        elif action == 'procBulkInsertCreditRVCash':
            shaProcessObj.procBulkInsertCreditRVCash()
            pass
        elif action == 'procUpdateDEXIndex':
            shaProcessObj.procUpdateDEXIndex()
            pass
        else:
            pass

if __name__ == '__main__':
# unit testing
    main()

    # debugging

    #shaProcessObj = shaProcess()
    #shaProcessObj.procUpdateDEX()
