import blpapi
import logging

# create session object that connects to bbg
def BBGConnect(host="localhost", port=8194):
    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(host)
    sessionOptions.setServerPort(port)
    print "Connecting to %s:%d" % (host, port)
    # Create a Session
    session = blpapi.Session(sessionOptions)
    # Start a Session
    if not session.start():
        logging.error("Failed to start BBG session")
        raise Exception("Failed to start BBG session.")

    if not session.openService("//blp/refdata"):
        logging.error("Failed to open //blp/refdata")
        raise Exception("Failed to open //blp/refdata")
    return session


# returns reference request data for list of tickers and fields in a dict(dict({})): {ticker: {fields: }}
def BBGRefDataRequest(tickerList, fieldList, sesh = None):
    results = dict({})
    if sesh is None:
        session = BBGConnect()
    else:
        session = sesh
    refDataService = session.getService("//blp/refdata")
    # request one ticker at a time since bbg is stupid
    for ticker in tickerList:
        results[ticker] = dict({})
        request = refDataService.createRequest("ReferenceDataRequest")
        # append securities to request
        request.append("securities", ticker)
        # append fields to request
        for field in fieldList:
            request.append("fields", field)
        # send request
        session.sendRequest(request)
        try:
            while(True):
                event = session.nextEvent()
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        if msg.hasElement("responseError"):
                            logging.error("%s", msg.getElementAsString("responseError"))
                            print msg
                            raise Exception(msg.getElementAsString("responseError"))
                        securityDataArray = msg.getElement("securityData")
                        for securityData in securityDataArray.values():
                            if securityData.hasElement("securityError"):
                                secTicker = securityData.getElementAsString("security")
                                secErr = securityData.getElement("securityError")
                                logging.warning("%s: %s -> %s, %s", secTicker, 
                                    secErr.getElementAsString("category"),
                                    secErr.getElementAsString("subcategory"),
                                    secErr.getElementAsString("message"))
                            else:
                                fieldData = securityData.getElement("fieldData")
                                for field in fieldList:
                                    try:
                                        element = fieldData.getElement(field)
                                        if element.numValues() > 1:
                                            array = []
                                            for e in element.values():
                                                array.append(e.getElementAsString("Security Description"))
                                            results[ticker][field] = array
                                        else:
                                            results[ticker][field] = element.getValueAsString()
                                    except Exception as err:
                                        logging.warning("%s: %s", ticker, err)
                    break
        except Exception as err:
            raise err
    if sesh is None:
        session.stop()
    return results

# returns historical data request for list of tickers and fields in dict(dict(dict({})))
def BBGHisDataRequest(tickerList, fieldList, startDate, endDate, sesh = None, interval="DAILY"):
    results = dict({})
    if sesh is None:
        session = BBGConnect()
    else:
        session = sesh
    refDataService = session.getService("//blp/refdata")
    #request one ticker at a time since bbg is stupid
    for ticker in tickerList:
        results[ticker] = dict({})
        request = refDataService.createRequest("HistoricalDataRequest")
        # append securities to request
        request.append("securities", ticker)
        # append fields to request
        for field in fieldList:
            request.append("fields", field)
        # set other properties of request
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", interval)
        request.set("startDate", startDate)
        request.set("endDate", endDate)
        # send request
        session.sendRequest(request)
        try:
            while(True):
                event = session.nextEvent()
                if event.eventType() == blpapi.Event.RESPONSE:
                    for msg in event:
                        # print msg
                        if msg.hasElement("responseError"):
                            logging.error("%s", msg.getElementAsString("responseError"))
                            print msg
                            raise Exception(msg.getElementAsString("responseError"))
                        securityData = msg.getElement("securityData")
                        if securityData.hasElement("securityError"):
                            secTicker = securityData.getElementAsString("security")
                            secErr = securityData.getElement("securityError")
                            logging.warning("%s: %s -> %s, %s", secTicker, 
                                secErr.getElementAsString("category"),
                                secErr.getElementAsString("subcategory"),
                                secErr.getElementAsString("message"))
                        else:
                            fieldDataArray = securityData.getElement("fieldData")
                            for fieldData in fieldDataArray.values():
                                priceDate = fieldData.getElementAsString("date")
                                results[ticker][priceDate] = dict({})
                                for field in fieldList:
                                    try:
                                        element = fieldData.getElement(field)
                                        if element.numValues() > 1:
                                            array = []
                                            for e in element.values():
                                                array.append(e.getElementAsString("Security Description"))
                                            results[ticker][priceDate][field] = array
                                        else:
                                            results[ticker][priceDate][field] = element.getValueAsString()
                                    except Exception as err:
                                        logging.warning("%s: %s", ticker, err)
                    break
        except Exception as err:
            raise err
    if sesh is None:
        session.stop()
    return results