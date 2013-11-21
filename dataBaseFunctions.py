import cx_Oracle
import MySQLdb # MySQL
import pyodbc
import logging
import HelperFunctions
from HelperFunctions import *
import time
from datetime import *
import sys
class dataBaseFunctions(object):

    def __init__(self, StartDate, EndDate, mylogger):    # Constructor of the class
        self.StartDate = StartDate
        self.EndDate = EndDate
        self.mylogger = mylogger
        self.mapIDColumn = {
                "asset_list":"assetID",
                "fxd_list":"fxdID",
                "scf_list":"SCFID"
                }


    def oracleConnect(self):
        try:
            #con = cx_Oracle.connect('MX_DM_READ_PROD/pr0dmx_re*d@prodscan.cppib.ca/murex_prod_rpt.cppib.ca')
            con = cx_Oracle.connect('svc_mx_sha_bu_prod/Ntskw!472@prodscan.cppib.ca/murex_prod_rpt.cppib.ca')
        except cx_Oracle.Error, err:
            logging.warning("Database connection error. " + str(err))
            sys.exit()
        return con

    def mySQLConnect(self):
        mySQLServer = "SHARW1"
        mySQLPort = 3311
        mySQLuser = "root"
        mySQLpwd = "cpp"
        mySQLDefaultDB = "application"

        try:
            mySQLdb = MySQLdb.connect(
                    host = mySQLServer,
                    port = mySQLPort,
                    user = mySQLuser,
                    passwd = mySQLpwd,
                    db = mySQLDefaultDB
                    )
        except MySQLdb.Error, err:
            self.logging.warning("My SQL Database Connection Error. " + str(err))
            sys.exit()
        return mySQLdb

    def msSQLConnect(self):
        dbConnectionStr = 'DRIVER={SQL Server};SERVER=PRIDBSQLV01\PRI2008A;DATABASE=SHA2;Trusted_Connection=yes'
        try:
            #db = pyodbc.connect(dbConnectionStr)
            # Wei Wei
            db = pyodbc.connect(dbConnectionStr, autocommit = True)
        except pyodbc.Error, err:
            logging.warning("Database connection error. " + str(err))
            sys.exit()
        return db

    def getQuery(self, fileName): #retrieves the query from file specified in the argument '
        DATE = self.EndDate
        #DATE = self.EndDate - timedelta(2)
        print "Date used in SQL Query: " + str(DATE)
        #sys.exit()
        with open("P:\\SHA\\Credit Group\\Risk Reporting\\process\\python\\queries\\" + fileName) as f:
            queryArray = f.readlines()
        queryString = ''
        for f in queryArray:
            if f.count("%QUERYDATE%") > 0:
                f = f.replace("%QUERYDATE%", DATE.strftime('%d/%m/%Y'))
            queryString = queryString + f
        return queryString

    def generateMap(self, mapName):  #this function accepts the file name of the file which contains the query to retrieve the map returns the map
        DATE = self.EndDate
        myMap = dict()
        with open("P:\\SHA\\Credit Group\\Risk Reporting\\process\\python\\queries\\" + mapName) as f:
            queryArray = f.readlines()
        queryString = ''
        for f in queryArray:
            if f.count("%QUERYDATE%") > 0:
                f = f.replace("%QUERYDATE%", DATE.strftime('%m/%d/%Y'))
            queryString = queryString + f
        msdb = dataBaseFunctions.msSQLConnect(self)
        print(queryString)
        cur = msdb.cursor()
        cur.execute(queryString)
        for row in cur:
            myMap[row[1]] = row[0]
        myMap[None] = '0'
        msdb.close()
        return myMap

    def getMaxID(self, tableName): #accepts tablename and gets the last row
        view = "risk"
        if (tableName.startswith("map_")): #change view to map for query
            view = "map"
        queryString = "SELECT MAX(" + self.mapIDColumn[tableName] + ")  FROM [SHA2].["+view+"].["+tableName+"]"
        msdb = dataBaseFunctions.msSQLConnect(self)
        cur = msdb.cursor()
        cur.execute(queryString)
        for row in cur:
            count = row[0]
        if count == None:
            return 0
        msdb.close()
        return count

    def getLastRow(self, tableName): #accepts tablename and gets the last row
        view = "risk"
        if (tableName.startswith("map_")): #change view to map for query
            view = "map"
        queryString = "SELECT COUNT(*)  FROM [SHA2].["+view+"].["+tableName+"]"
        msdb = dataBaseFunctions.msSQLConnect(self)
        cur = msdb.cursor()
        cur.execute(queryString)
        for row in cur:
            count = row[0]
        if count == None:
            return 0
        if count == '':
            return 0
        msdb.close()
        return count

    def getMaxCol(self,colName, tableName): #accepts returns max value in a col accepts the col name and table name
        view = "risk"
        if (tableName.startswith("map_")): #change view to map for query
            view = "map"
        queryString = "SELECT MAX("+colName+")  FROM [SHA2].["+view+"].["+tableName+"]"
        msdb = dataBaseFunctions.msSQLConnect(self)
        cur = msdb.cursor()
        cur.execute(queryString)
        for row in cur:
            count = row[0]
        if count == '':
            return 0
        msdb.close()
        return count

    #########################################################################
    #
    # Function Name: msInsert(tableName, colNames, colNameMap, values, keys)
    #
    # Comment: used to generate query and insert into MS... tablename is the
    #          name of table inserting into... col Names are the col names.
    #          ColNameMap has the index of each header,  values are the actual
    #          values, and keys are the tables keys. refer to trade_detail
    #          tblt, colNames is the tmpColName created with all the non
    #          essential columns removed. same thing wt "values", all the
    #          non essential data were popped from the end of the
    #          list (rowObject)
    #
    #########################################################################

    def msInsert(self, tableName, colNames, colNameMap, values, keys, cursor):
        #print("Begin of insert" + str(datetime.now()))
        hf = HelperFunctions(self.StartDate, self.EndDate,  self.mylogger)
        initialRowCount = dataBaseFunctions.getLastRow(self, tableName)

        lastRow = initialRowCount + 1
        colCount = 0
        sqlstr = "UPDATE sha2.risk."+tableName+" SET "

        for value in values:
            #IF THE COLUMN NAME IS NOT A KEY, THEN SET IT a NEW VALUE
            if(str(colNames[colCount]) not in keys):
                sqlstr = sqlstr + str(colNames[colCount]) + " = '" + str(value) + "', "
            colCount = colCount + 1
        sqlstr = sqlstr[:-len(", ")] #removes last comma
        sqlstr = sqlstr + " WHERE "

        for key in keys: #KEYS
            sqlstr = sqlstr + str(key) + " = '" + str(values[colNameMap[key]]) + "' AND "

        sqlstr = sqlstr[:-len("AND ")] #removes last AND
        sqlstr = sqlstr + " IF @@ROWCOUNT = 0 INSERT INTO sha2.risk."+tableName+"( "

        # the next 3 sets of lines are used becuaase in the asset_list.query, the assetid or fxid or scfid
        # are not pulled so here we are just manually creating that column.....this is done here becuase
        # this is a generic insert function for database and we dont always know the assetid, fxid or scfid values
        # when doing inserts for none asset_list tables...

        if(tableName == "asset_list"): #if its the assetList or then assetID is manually set
            sqlstr = sqlstr + "ASSETID, "

        if(tableName == "fxd_list"): #if its the fx_list or then FXID is manually set
            sqlstr = sqlstr + "FXDID, "

        if(tableName == "scf_list"): #if its the scf_list or then SCFID is manually set
            sqlstr = sqlstr + "SCFID, "

        for colName in colNames:
            sqlstr = sqlstr + str(colName) + ", "
        sqlstr = sqlstr[:-len(", ")] #removes last comma
        sqlstr = sqlstr + ")values("

        if(tableName == "asset_list"): #if its the assetList then assetID is manually set
            maxColID = self.getMaxID(tableName)
            maxColID += 1
            #sqlstr = sqlstr + "'" + str(lastRow) + "', " # assetID first
            sqlstr = sqlstr + "'" + str(maxColID) + "', " # assetID first

        if(tableName == "fxd_list"): #if its the fxd list then FXID is manually set
            maxColID = self.getMaxID(tableName)
            maxColID += 1
            #sqlstr = sqlstr + "'" + str(lastRow) + "', " # FXID first
            sqlstr = sqlstr + "'" + str(maxColID) + "', " # FXID first

        if(tableName == "scf_list"): #if its the fxd list then SCFID is manually set
            maxColID = self.getMaxID(tableName)
            maxColID += 1
            #sqlstr = sqlstr + "'" + str(lastRow) + "', " # FXID first
            sqlstr = sqlstr + "'" + str(maxColID) + "', " # SCFID first

        for value in values:
            sqlstr = sqlstr + "'" + str(value) + "', "

        sqlstr = sqlstr[:-len(", ")] #removes last comma
        sqlstr = sqlstr + ")"

        # Wei Wei
        #msdb = dataBaseFunctions.msSQLConnect(self)
        #cur = msdb.cursor()

        #######
        #
        # Debug
        #
        #######
        #print(sqlstr)

        # do try execute and commit, expcet rollback()
        try:
            cursor.execute(sqlstr)
            #msdb.commit()
        except:
            msdb.rollback()
            hf.printAndLog("Problem executing: " + sqlstr, False)

        #print("End of insert" + str(datetime.now()))

        #msdb.close()
        finalRowCount = dataBaseFunctions.getLastRow(self, tableName)
      #  print("End of insert" + str(time.clock()))
        if(initialRowCount - finalRowCount != 0 and 'PRICEDATE' not in keys): #if a new value was added to table log it and its not a daily thing (so i check is the column names contain pricedate)
            hf.printAndLog("Table:" + tableName + "; Item Added:" + str(values) ,False)
        elif(initialRowCount - finalRowCount != 0): #print if you dont log ^above
            print("Table:" + tableName + "; Item Added:" + str(values))

    def msInsert_BACKUP(self, tableName, colNames, colNameMap, values, keys):
        #print("Begin of insert" + str(time.clock()))
        hf = HelperFunctions(self.StartDate, self.EndDate,  self.mylogger)
        initialRowCount = dataBaseFunctions.getLastRow(self, tableName)

        lastRow = initialRowCount + 1
        colCount = 0
        sqlstr = "UPDATE sha2.risk."+tableName+" SET "

        for value in values:
            #IF THE COLUMN NAME IS NOT A KEY, THEN SET IT a NEW VALUE
            if(str(colNames[colCount]) not in keys):
                sqlstr = sqlstr + str(colNames[colCount]) + " = '" + str(value) + "', "
            colCount = colCount + 1
        sqlstr = sqlstr[:-len(", ")] #removes last comma
        sqlstr = sqlstr + " WHERE "

        for key in keys: #KEYS
            sqlstr = sqlstr + str(key) + " = '" + str(values[colNameMap[key]]) + "' AND "

        sqlstr = sqlstr[:-len("AND ")] #removes last AND
        sqlstr = sqlstr + " IF @@ROWCOUNT = 0 INSERT INTO sha2.risk."+tableName+"( "

        # the next 3 sets of lines are used becuaase in the asset_list.query, the assetid or fxid or scfid
        # are not pulled so here we are just manually creating that column.....this is done here becuase
        # this is a generic insert function for database and we dont always know the assetid, fxid or scfid values
        # when doing inserts for none asset_list tables...

        if(tableName == "asset_list"): #if its the assetList or then assetID is manually set
            sqlstr = sqlstr + "ASSETID, "

        if(tableName == "fxd_list"): #if its the fx_list or then FXID is manually set
            sqlstr = sqlstr + "FXDID, "

        if(tableName == "scf_list"): #if its the scf_list or then SCFID is manually set
            sqlstr = sqlstr + "SCFID, "

        for colName in colNames:
            sqlstr = sqlstr + str(colName) + ", "
        sqlstr = sqlstr[:-len(", ")] #removes last comma
        sqlstr = sqlstr + ")values("

        if(tableName == "asset_list"): #if its the assetList then assetID is manually set
            maxColID = self.getMaxID(tableName)
            maxColID += 1
            #sqlstr = sqlstr + "'" + str(lastRow) + "', " # assetID first
            sqlstr = sqlstr + "'" + str(maxColID) + "', " # assetID first

        if(tableName == "fxd_list"): #if its the fxd list then FXID is manually set
            maxColID = self.getMaxID(tableName)
            maxColID += 1
            #sqlstr = sqlstr + "'" + str(lastRow) + "', " # FXID first
            sqlstr = sqlstr + "'" + str(maxColID) + "', " # FXID first

        if(tableName == "scf_list"): #if its the fxd list then SCFID is manually set
            maxColID = self.getMaxID(tableName)
            maxColID += 1
            #sqlstr = sqlstr + "'" + str(lastRow) + "', " # FXID first
            sqlstr = sqlstr + "'" + str(maxColID) + "', " # SCFID first

        for value in values:
            sqlstr = sqlstr + "'" + str(value) + "', "

        sqlstr = sqlstr[:-len(", ")] #removes last comma
        sqlstr = sqlstr + ")"
        msdb = dataBaseFunctions.msSQLConnect(self)
        cur = msdb.cursor()

        #######
        #
        # Debug
        #
        #######
        #print(sqlstr)

        # do try execute and commit, expcet rollback()
        try:
            cur.execute(sqlstr)
            msdb.commit()
        except:
            msdb.rollback()
            hf.printAndLog("Problem executing: " + sqlstr, False)

        msdb.close()
        #print("before rowcount  of end insert" + str(time.clock()))
        finalRowCount = dataBaseFunctions.getLastRow(self, tableName)
      #  print("End of insert" + str(time.clock()))
        if(initialRowCount - finalRowCount != 0 and 'PRICEDATE' not in keys): #if a new value was added to table log it and its not a daily thing (so i check is the column names contain pricedate)
            hf.printAndLog("Table:" + tableName + "; Item Added:" + str(values) ,False)
        elif(initialRowCount - finalRowCount != 0): #print if you dont log ^above
            print("Table:" + tableName + "; Item Added:" + str(values))

    def retrieveCreditCurveID(self, currencyID, seniorityID, restructuringTypeID , referenceEntityID): #function retrieves the credit curve ID using currency, seniority, CURVEID WHICH IS ACTUALLY RESTRUTURINGTYPE BEFORE THIS IS method is run and issuername
        with open("P:\\SHA\\Credit Group\\Risk Reporting\\process\\python\\queries\\retrieve_curveid.query") as f:
            queryArray = f.readlines()
        queryString = ''
        for f in queryArray:
            if f.count("%CURRENCYID%") > 0:
                f = f.replace("%CURRENCYID%", str(currencyID))
            if f.count("%SENIORITYID%") > 0:
                f = f.replace("%SENIORITYID%", str(seniorityID))
            if f.count("%REFERENCEENTITY%") > 0:
                f = f.replace("%REFERENCEENTITY%", str(referenceEntityID))
            if f.count("%RESTRUCTURINGTYPEID%") > 0:
                f = f.replace("%RESTRUCTURINGTYPEID%", str(restructuringTypeID))
            queryString = queryString + f

        msdb = dataBaseFunctions.msSQLConnect(self)
        cur = msdb.cursor()
        cur.execute(queryString)

        creditCurveID =''
        for row in cur:
            creditCurveID = row[0]
        if creditCurveID == '':
            return 0
        return creditCurveID
        msdb.close()

    def retrieveassetID(self, securityName, creditCurveID): #function assetID using curveID and securityName
        with open("P:\\SHA\\Credit Group\\Risk Reporting\\process\\python\\queries\\retrieve_assetid.query") as f:
            queryArray = f.readlines()
        queryString = ''
        for f in queryArray:
            if f.count("%CURRENCYID%") > 0:
                f = f.replace("%CURRENCYID%", str(currencyID))
            if f.count("%SENIORITYID%") > 0:
                f = f.replace("%SENIORITYID%", str(seniorityID))
            if f.count("%ISSUER%") > 0:
                f = f.replace("%ISSUER%", str(issuer))
            if f.count("%RESTRUCTURINGTYPEID%") > 0:
                f = f.replace("%RESTRUCTURINGTYPEID%", str(restructuringTypeID))
            queryString = queryString + f

        msdb = dataBaseFunctions.msSQLConnect(self)
        cur = msdb.cursor()
        cur.execute(queryString)

        creditCurveID =''
        for row in cur:
            creditCurveID = row[0]
        if creditCurveID == '':
            return 0
        return creditCurveID
        msdb.close()

    def msUpdateInsertMap(self, tableName, colNames, colNameMap, values, keys): #used to generate query and insert into MS for the map tables... tablename is the name of table inserting into... col Names are the col names.... ColNameMap has the index of each header,  values are the actual values, and keys are the tables keys
        hf = HelperFunctions(self.StartDate, self.EndDate,  self.mylogger)
        lastID = dataBaseFunctions.getMaxCol(self, colNames[0], tableName) + 1

        initialRowCount = dataBaseFunctions.getLastRow(self, tableName)
        colCount = 0
        sqlstr = "UPDATE sha2.map."+tableName+" SET "
        for key in keys:
            sqlstr = sqlstr + str(key) + " = '" + str(values[colNameMap[key]]) + "', "
            colCount = colCount + 1
        sqlstr = sqlstr[:-len(", ")] #removes last comma
        sqlstr = sqlstr + " WHERE "
        for key in keys: #KEYS
            sqlstr = sqlstr + str(key) + " = '" + str(values[colNameMap[key]]) + "' AND "

        sqlstr = sqlstr[:-len("AND ")] #removes last AND
        sqlstr = sqlstr + " IF @@ROWCOUNT = 0 INSERT INTO sha2.map."+tableName+"( "
        for colName in colNames:
            sqlstr = sqlstr + str(colName) + ", "
        sqlstr = sqlstr[:-len(", ")] #removes last comma
        sqlstr = sqlstr + ")values("
        sqlstr = sqlstr + "'" + str(lastID) + "', " # mapid first
        sqlstr = sqlstr + "'" + str(values[1]) + "', " #the value

        # "map_capitalstructure" has multiple PKs so it needs this added line
        if tableName == "map_capitalstructure":
            sqlstr = sqlstr + "'" + str(values[2]) + "', " #the 2nd value

        sqlstr = sqlstr[:-len(", ")] #removes last comma
        sqlstr = sqlstr + ")"

        print(sqlstr)

        msdb = dataBaseFunctions.msSQLConnect(self)
        cur = msdb.cursor()
        cur.execute(sqlstr)
        msdb.commit()
        msdb.close()
        finalRowCount  = dataBaseFunctions.getLastRow(self, tableName)
        if(initialRowCount - finalRowCount != 0): #if a new value was added to table log it
            hf.printAndLog("Table:" + tableName + "; Item Added:" + values[1] ,False)

