SELECT mypk
,sometext
,CASE WHEN numint = 1 THEN NULL ELSE numint END AS numint
,adate
FROM t1