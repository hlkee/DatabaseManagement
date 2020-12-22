DROP TABLE IF EXISTS Bids;
CREATE TABLE Bids(
    bidID INTEGER PRIMARY KEY,
    time VARCHAR, 
    amount FLOAT,
    itemID INTEGER,
    FOREIGN KEY (itemID) REFERENCES Item(itemID)
);
DROP TABLE IF EXISTS Category;
CREATE TABLE Category(
    name  VARCHAR PRIMARY KEY
);
DROP TABLE IF EXISTS Cat_Items;
CREATE TABLE Cat_Items(
    itemID  INTEGER, 
    catName VARCHAR,
    PRIMARY KEY (itemID, catName), 
    FOREIGN KEY (catName) REFERENCES Category(name)
    FOREIGN KEY (itemID) REFERENCES Item(itemID)
);
DROP TABLE IF EXISTS User;
CREATE TABLE User(
    UserID VARCHAR PRIMARY KEY, 
    Rating INTEGER
);
DROP TABLE IF EXISTS Bidder;
CREATE TABLE Bidder(
    userID VARCHAR PRIMARY KEY,
    location VARCHAR,
    country VARCHAR,
    FOREIGN KEY(userID) REFERENCES User(userID)
);
DROP TABLE IF EXISTS Item;
CREATE TABLE Item(
    itemID INTEGER PRIMARY KEY,
    firstBid FLOAT, 
    currentBid FLOAT,
    numBids INTEGER, 
    description VARCHAR, 
    name VARCHAR, 
    location VARCHAR, 
    country VARCHAR, 
    startDate VARCHAR, 
    endDate VARCHAR,
    sellerID VARCHAR,
    FOREIGN KEY (sellerID) REFERENCES User(userID)
);
