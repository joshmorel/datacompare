SET NOCOUNT ON;
DECLARE @test_food TABLE (
	foodrank INT
	,date_ingested DATETIME
	,food VARCHAR(100)
	,price_on_mars DECIMAL(10,2)
	,i_like BIT
	,number_of_calories INT
)

INSERT INTO @test_food
VALUES
(1,'2014-04-01','broccoli',10023.33,0,5),
(2,'2014-05-01','sandwich',30023.44,1,null),
(3,'2014-01-01','egg yolk',223023.12,1,20),
(4,'2013-01-01','bean stew',3023.12,0,null),
(5,'2013-01-01','cronut burger',null,0,10000)


SELECT * FROM @test_food
WHERE date_ingested BETWEEN ? AND ?
--WHERE date_ingested BETWEEN '2014-04-01' AND '2015-04-01'