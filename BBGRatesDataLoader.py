import argparse
import pyodbc
import datetime
import logging
import re
from shaLib import shaDBUtil
from shaLib import shaBBGUtil

# log file
LOGFILE = 'P:\\SHA\\ke\\logs\\BBGRatesDataLoader' + datetime.datetime.now().strftime("%Y%m%d.log")
logging.basicConfig(filename=LOGFILE, filemode='w', level=logging.DEBUG, 
    format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %I:%M:%S')

# timestamp
TIMESTAMP = ''


class Security:
    def __init__(self):
        self.assetID = -1
        self.name = ''
        self.bbgCode = ''
        self.tableName = ''
        self.quote = -999.0
    
    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return str(self.__dict__)


def DateFormat(s):
    try:
        return re.match("([0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9])", s).group(0)
    except:
        raise argparse.ArgumentTypeError("Date {0} does not match required format yyyy-MM-dd".format(s))


def loadAssetList(cur):
    logging.debug("Loading Asset List")
    if cur == "ALL":
        cur =''

    sqlStr = """
            SELECT a.asset_id, a.asset_name, a.bloomberg_code, b.tableName 
            FROM (SELECT * FROM SHA2.rates_data.asset_table 
            WHERE bloomberg_code <> 'None' AND bloomberg_code IS NOT NULL 
                AND asset_type_id <> 99999
                AND asset_name like '%{0}%') a 
            LEFT JOIN sha2.map.map_rates_quote_tables b ON a.quote_table_id = b.ratesQuoteTableID
            """.format(cur)
    results = shaDBUtil.ExecuteSelectStmt(sqlStr)
    if results:
        assetList = []
        for row in results:
            newSecurity = Security()
            newSecurity.assetID = row[0]
            newSecurity.name = row[1]
            newSecurity.bbgCode = row[2]
            newSecurity.tableName = row[3]
            assetList.append(newSecurity)
        return assetList
    else:
        logging.error("Failed to load asset list from db")
        exit()

def getFutCode(dt):
    # logging.debug("Looking up futures code")
    sqlStr = """
            SELECT code + RIGHT(datename(yy, '{0}'), 1)
            FROM SHA2.map.map_fut_code
            WHERE [month] = CONVERT(VARCHAR(3), datename(m, '{0}'))
            """.format(dt)
    results = shaDBUtil.ExecuteSelectStmt(sqlStr)

    if results:
        return results[0][0]
    else:
        logging.error("Failed to get futures code")
        return None


def pullBBGHistoricalData(assetList, startDate, endDate):
    logging.debug("Requesting historical data for %d assets", len(assetList))
    session = shaBBGUtil.BBGConnect()
    fieldList = ["PX_LAST"]
    for i in xrange(0, len(assetList)):
        ticker = [assetList[i].bbgCode]
        results = shaBBGUtil.BBGHisDataRequest(ticker, fieldList, startDate, endDate, session)
        assetList[i].__dict__["bbgData"] = results[ticker[0]]
    session.stop()


def pullBBGReferenceData(assetList):
    logging.debug("Requesting data for %d assets", len(assetList))
    session = shaBBGUtil.BBGConnect()
    fieldList = ["PX_LAST"]
    bondFutFieldList = ["FUT_FIRST_TRADE_DT", "FUT_DLV_DT_FIRST", "FUT_DLV_DT_LAST", "LAST_TRADEABLE_DT", "PX_LAST"]
    for i in xrange(0, len(assetList)):
        ticker = [assetList[i].bbgCode]
        if "_BONDFUT_" in assetList[i].name:
            results = shaBBGUtil.BBGRefDataRequest(ticker, bondFutFieldList, session)
            if results[ticker[0]]:
                assetList[i].__dict__["firstTradeDate"] = results[ticker[0]]["FUT_FIRST_TRADE_DT"]
                assetList[i].__dict__["lastTradeDate"] = results[ticker[0]]["LAST_TRADEABLE_DT"]
                assetList[i].__dict__["firstDlvDate"] = results[ticker[0]]["FUT_DLV_DT_FIRST"]
                assetList[i].__dict__["lastDlvDate"] = results[ticker[0]]["FUT_DLV_DT_LAST"]
                assetList[i].quote = float(results[ticker[0]]["PX_LAST"])
        else:
            results = shaBBGUtil.BBGRefDataRequest(ticker, fieldList, session)
            if results[ticker[0]]:
                quote = float(results[ticker[0]]["PX_LAST"])
                if "_SWAP_" in assetList[i].name:
                    quote = quote / 100
                elif "_MM_" in assetList[i].name:
                    quote = quote / 100
                elif "_FRA_" in assetList[i].name:
                    quote = quote / 100
                elif "_BASIS_" in assetList[i].name:
                    if ("USD_OIS_BASIS" not in assetList[i].name) and ("EUR_OIS_BASIS" not in assetList[i].name):
                        quote = quote * -1
                assetList[i].quote = quote
    session.stop()


def runHistoricalRatesLoader(startDate, endDate, cur):
    logging.debug("Loading rates data from %s to %s for %s assets", 
        startDate.strftime("%Y-%m-%d"), endDate.strftime("%Y-%m-%d"), cur)
    assetList = loadAssetList(cur)
    pullBBGHistoricalData(assetList, startDate.strftime("%Y%m%d"), endDate.strftime("%Y%m%d"))
    success = 0
    # loop through list and upload to db
    for asset in assetList:
        if asset.bbgData:
            for date, values in asset.bbgData.items():
                quote = float(values["PX_LAST"])
                if "_SWAP_" in asset.name:
                    quote = quote / 100
                elif "_MM_" in asset.name:
                    quote = quote / 100
                elif "_FRA_" in asset.name:
                    quote = quote / 100
                elif "_BASIS_" in asset.name:
                    if ("USD_OIS_BASIS" not in asset.name) and ("EUR_OIS_BASIS" not in asset.name):
                        quote = quote * -1
                values = "('{0}', {1}, {2}, '{3}')".format(date, quote, asset.assetID, TIMESTAMP)
                sqlStr = """
                        UPDATE {0} SET rate_quote = {1}, save_time = '{2}'
                        WHERE save_date = '{3}' AND asset_id = {4}
                        IF @@ROWCOUNT = 0
                        INSERT INTO {0}(save_date, rate_quote, asset_id, save_time)
                        VALUES {5}
                        """.format(asset.tableName, quote, TIMESTAMP, date, asset.assetID, values)
                sqlStrIntraday = """
                        UPDATE {0}_intraday SET rate_quote = {1}
                        WHERE save_date = '{2}' AND asset_id = {3} AND save_time = '{4}'
                        IF @@ROWCOUNT = 0
                        INSERT INTO {0}_intraday(save_date, rate_quote, asset_id, save_time)
                        VALUES {5}
                        """.format(asset.tableName, quote, date, asset.assetID, TIMESTAMP, values)
                if not shaDBUtil.UpdateInsertStmt(sqlStr):
                    logging.error("Quote table update failed")
                if not shaDBUtil.UpdateInsertStmt(sqlStrIntraday):
                    logging.error("Intraday Quote table update failed")
    logging.debug("Success: %d", success)


def runDailyRatesLoader(today, cur):
    logging.debug("Loading rates data on %s for %s assets", today.strftime("%Y-%m-%d"), cur)
    assetList = loadAssetList(cur)
    success = 0
    pullBBGReferenceData(assetList)
    # loop through list and upload to db
    for asset in assetList:
        if asset.quote != -999:
            values = ""
            if "_BONDFUT_" in asset.name:
                futCode = getFutCode(asset.lastTradeDate)
                if futCode is None:
                    futCode = ""
                    logging.error("futures_code look up failed for: %s->%s", asset.name, asset.lastTradeDate)
                values = "('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', {6}, {7}, '{8}')".format(today.strftime("%Y-%m-%d"), 
                    asset.firstTradeDate, asset.lastTradeDate, asset.firstDlvDate, asset.lastDlvDate, futCode, 
                    asset.quote, asset.assetID, TIMESTAMP)
                sqlStr = """
                        UPDATE {0} SET first_trade_date = '{1}', last_trade_date = '{2}', first_delivery_date = '{3}', 
                            last_delivery_date = '{4}', futures_code = '{5}', rate_quote = {6}, save_time = '{7}' 
                        WHERE save_date = '{8}' AND asset_id = {9}
                        IF @@ROWCOUNT = 0
                        INSERT INTO {0}(save_date, first_trade_date, last_trade_date, first_delivery_date, 
                            last_delivery_date, futures_code, rate_quote, asset_id, save_time)
                        VALUES {10}
                        """.format(asset.tableName, asset.firstTradeDate, asset.lastTradeDate, asset.firstDlvDate, 
                            asset.lastDlvDate, futCode, asset.quote, TIMESTAMP, today.strftime("%Y-%m-%d"), asset.assetID, values)
                sqlStrIntraday = """
                        UPDATE {0}_intraday SET first_trade_date = '{1}', last_trade_date = '{2}', first_delivery_date = '{3}', 
                            last_delivery_date = '{4}', futures_code = '{5}', rate_quote = {6}
                        WHERE save_date = '{7}' AND asset_id = {8} AND save_time = '{9}'
                        IF @@ROWCOUNT = 0
                        INSERT INTO {0}_intraday(save_date, first_trade_date, last_trade_date, first_delivery_date, 
                            last_delivery_date, futures_code, rate_quote, asset_id, save_time)
                        VALUES {10}
                        """.format(asset.tableName, asset.firstTradeDate, asset.lastTradeDate, asset.firstDlvDate, 
                            asset.lastDlvDate, futCode, asset.quote, today.strftime("%Y-%m-%d"), asset.assetID, TIMESTAMP, values)
            else:
                values = "('{0}', {1}, {2}, '{3}')".format(today.strftime("%Y-%m-%d"), asset.quote, asset.assetID, TIMESTAMP)
                sqlStr = """
                        UPDATE {0} SET rate_quote = {1}, save_time = '{2}'
                        WHERE save_date = '{3}' AND asset_id = {4}
                        IF @@ROWCOUNT = 0
                        INSERT INTO {0}(save_date, rate_quote, asset_id, save_time)
                        VALUES {5}
                        """.format(asset.tableName, asset.quote, TIMESTAMP, today.strftime("%Y-%m-%d"), asset.assetID, values)
                sqlStrIntraday = """
                        UPDATE {0}_intraday SET rate_quote = {1}
                        WHERE save_date = '{2}' AND asset_id = {3} AND save_time = '{4}'
                        IF @@ROWCOUNT = 0
                        INSERT INTO {0}_intraday(save_date, rate_quote, asset_id, save_time)
                        VALUES {5}
                        """.format(asset.tableName, asset.quote, today.strftime("%Y-%m-%d"), asset.assetID, TIMESTAMP, values)
            
            if not shaDBUtil.UpdateInsertStmt(sqlStr):
                logging.error("Quote table update failed")
            if not shaDBUtil.UpdateInsertStmt(sqlStrIntraday):
                logging.error("Intraday Quote table update failed")
            success = success + 1
    logging.debug("Success: %d", success)


# main method parse arguments and run appropriate loader function
def main(cmdLineArgs):
    print "Arguments: {0}".format(cmdLineArgs)
    startDate = datetime.datetime.strptime(cmdLineArgs.startDate, "%Y-%m-%d")
    if cmdLineArgs.endDate is None:
        endDate = startDate
    else:
        endDate = datetime.datetime.strptime(cmdLineArgs.endDate, "%Y-%m-%d")
    print "Start: {0}".format(startDate)
    print "End: {0}".format(endDate)
    mode = cmdLineArgs.mode
    print "Mode: {0}".format(mode)
    cur = cmdLineArgs.cur
    print "Cur: {0}".format(cur)
    
    if mode=="DAILY":
        today = startDate
        runDailyRatesLoader(today, cur)
    elif mode == "HISTORICAL":
        TIMESTAMP = "23:59"
        runHistoricalRatesLoader(startDate, endDate, cur)
    else:
        logging.error("Unsupported mode")


# program entry setting up arguments
if __name__ == "__main__":
    logging.debug("START BBGRatesDataLoader")
    today = datetime.datetime.now()
    TIMESTAMP = today.strftime("%H:%M")
    cmdLineParser = argparse.ArgumentParser(description="""rates data for all assets in asset_table""")
    cmdLineParser.add_argument('-mode', default = 'DAILY', choices = ['DAILY', 'HISTORICAL'], 
        help = 'specify which mode to run, default is DAILY')
    cmdLineParser.add_argument('-cur', default = 'ALL', metavar = 'USD', 
        help = 'Specify currency, or runs all by default')
    cmdLineParser.add_argument('-startDate', type = DateFormat, default = today.strftime('%Y-%m-%d'), 
        metavar = 'yyyy-MM-dd', help = 'most recent date, default is today')
    cmdLineParser.add_argument('-endDate', type = DateFormat, metavar = 'yyyy-MM-dd', 
        help = 'least recent date')
    cmdLineArgs = cmdLineParser.parse_args()

    try:
        main(cmdLineArgs)
    except KeyboardInterrupt:
        print "Ctrl+C pressed. Stopping..."
    finally:
        logging.debug("FINISH BBGRatesDataLoader")