
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub
import numpy as np

columnSeparator = "|"
bidCounter = 0
# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

tempList = []
userTempList = []
bidsTempList = []
bidderTempList = []
itemTableTempList = []
catItemsTempList = []
categoryTempList = []
def write(tableList, fileName):

    file = open(fileName + ".dat", "a+")
    for x in (tableList):
        temp = ""
        for y in (x.values()):
            attr = ""
            if (type(y) == str):
                y = y.replace("\"", "\"\"")
                attr = "\"" + y.strip() + "\""
            else:
                attr = str(y)
            temp = temp + attr + "|"

        if (fileName == 'catItems'):
            if (temp not in catItemsTempList):
                file.write(temp[:-1] + "\n")
                catItemsTempList.append(temp)

        elif (fileName == "category"):
            if (temp not in categoryTempList):
                file.write(temp[:-1] + "\n")
                categoryTempList.append(temp)

        elif (fileName == "itemTable"):
            if (temp not in itemTableTempList):
                file.write(temp[:-1] + "\n")
                itemTableTempList.append(temp)

        elif (fileName == "user"):
            if (temp not in userTempList):
                file.write(temp[:-1] + "\n")
                userTempList.append(temp)

        elif (fileName == "bids"):
            if (temp not in bidsTempList):
                file.write(temp[:-1] + "\n")
                bidsTempList.append(temp)

        elif (fileName == "bidder"):
            if (temp not in bidderTempList):
                file.write(temp[:-1] + "\n")
                bidderTempList.append(temp)

    file.close()

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""
def parseJson(json_file):
    global bidCounter
    itemTable = []
    bids = []
    category = []
    user = []
    bidder = []
    catItems = []



    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file

        for item in items:

            itemDic = {}
            itemDic["itemID"] = int(item["ItemID"])
            itemDic["firstBid"] = float(transformDollar(item["First_Bid"]))
            itemDic["currentBid"] = float(transformDollar(item["Currently"]))
            itemDic["numBids"] = int(item["Number_of_Bids"])
            itemDic["description"] = item["Description"]
            itemDic["name"] = item["Name"]
            itemDic["location"] = item["Location"]
            itemDic["country"] = item["Country"]
            itemDic["startDate"] = transformDttm(item["Started"])
            itemDic["endDate"] = transformDttm(item["Ends"])
            itemDic["sellerID"] = item["Seller"]["UserID"]

            if (itemDic not in itemTable):
                itemTable.append(itemDic)


            for cat in item["Category"]:
                catItemDic = {}
                catDic = {}
                catDic["key"] = cat
                catItemDic["itemID"] = int(item["ItemID"])
                catItemDic["category"] = cat

                catItems.append(catItemDic)

                if catDic not in category:
                    category.append(catDic)

            # if catItemDic not in catItems:
            sellerDic = {}
            sellerDic["userID"] = item["Seller"]["UserID"]
            sellerDic["rating"] = int(item["Seller"]["Rating"])
            if (sellerDic not in user):
                user.append(sellerDic)




            ##### PREVIOUS CODE #####
            # for bid in item["Bids"]:
            #     bidDic["itemId"] = item["ItemID"]
            #
            #     # Writing a for loop (for bid in bid["Bid"]) wouldn't work
            #     bidDic["bidTime"] = transformDttm(bid["Bid"]["Time"])
            #     bidDic["amount"] = transformDollar(bid["Bid"]["Amount"])
            #
            #     for bidder in bid["Bid"]["Bidder"]:
            #         bidderDic["userID"] = bidder["UserID"]
            #         bidderDic["location"] = bidder["Location"]
            #         bidderDic["country"] = bidder["Country"]
            #         userDic["userID"] = bidder["UserID"]
            #         userDic["rating"] = bidder["Rating"]
            #
            #         sellerDic["userID"] = bidder["UserID"]
            #         sellerDic["rating"] = bidder["Rating"]
            #
            #     for seller in item["Seller"]:
            #       sellerDic["userID"] = seller["UserID"]
            #       sellerDic["rating"] = seller["Rating"]

            if (item["Bids"] is None):
                continue



            # userDic = {}



            for bid in item["Bids"]:
                bidDic = {}
                bidderDic = {}
                userDic = {}

                bidDic["bidID"] = bidCounter
                bidDic["bidTime"] = transformDttm(bid["Bid"]["Time"])
                bidDic["amount"] = float(transformDollar(bid["Bid"]["Amount"]))
                bidDic["itemId"] = int(item["ItemID"])


                if ("UserID" in bid["Bid"]["Bidder"]):
                    bidderDic["userID"] = bid["Bid"]["Bidder"]["UserID"]
                else:
                    bidderDic["userID"] = "null"

                if ("Location" in bid["Bid"]["Bidder"]):
                    bidderDic["location"] = bid["Bid"]["Bidder"]["Location"]

                else:
                    bidderDic["location"] = "null"

                if ("Country" in bid["Bid"]["Bidder"]):
                    bidderDic["country"] = bid["Bid"]["Bidder"]["Country"]

                else:
                    bidderDic["country"] = "null"


                userDic["userID"] = bid["Bid"]["Bidder"]["UserID"]
                userDic["rating"] = int(bid["Bid"]["Bidder"]["Rating"])

                if (bidderDic not in bidder):
                  bidder.append(bidderDic)

                if (userDic not in user):
                    user.append(userDic)

                if (bidDic not in bids):
                    bids.append(bidDic)
                    bidCounter += 1


    write(itemTable, "itemTable")
    write(bids, "bids")
    write(category , "category")
    write (user, "user")
    write(bidder, "bidder")
    write(catItems, "catItems")
            #pass

"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):

    if len(argv) < 2:
        print ( sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>')
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print ("Success parsing " + f)

if __name__ == '__main__':
    main(sys.argv)
