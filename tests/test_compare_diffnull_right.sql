SELECT mypk
,sometext
,numint
,CASE WHEN adate < '2015-04-04' THEN NULL ELSE adate END AS adate
FROM t1