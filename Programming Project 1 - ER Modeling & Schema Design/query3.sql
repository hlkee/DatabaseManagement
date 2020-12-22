SELECT COUNT(*) FROM (SELECT itemID, COUNT(*) as NumCats FROM Cat_Items GROUP BY itemID) AS C WHERE C.NumCats = 4;
