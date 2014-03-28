import blpapi

session = blpapi.Session()
session.start()
subscriptions = blpapi.SubscriptionList()
subscriptions.add("IBM US Equity", "LAST_PRICE,BID,ASK", "",
                  blpapi.CorrelationId(1))
session.subscribe(subscriptions)
while (True):
    event = session.nextEvent()
    for msg in event:
        print("IBM: ", msg)