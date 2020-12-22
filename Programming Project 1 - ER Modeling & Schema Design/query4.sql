WITH maxPrice AS (SELECT MAX(currentBid) AS maxBid FROM Item) SELECT itemID FROM Item, maxPrice WHERE Item.currentBid =                               maxPrice.maxBid;
