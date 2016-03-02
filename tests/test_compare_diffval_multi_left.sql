declare @StartDate as date, @EndDate as date, @StartDateInt as int, @EndDateInt as int;
set @StartDate = '2015-09-29'; set @EndDate = '2015-09-30';
set @StartDateInt = cast(convert(char(8),@StartDate,112) as int);
set @EndDateInt = cast(convert(char(8),@EndDate,112) as int);

SELECT  [FactEventKey]
      ,[OccurrenceDateKey]
	  ,case when AvgTone < -8 then AvgTone*1.1
		else AvgTone end as AvgTone
	  ,replace(action_dg.GeoCountryCode,'S','') as GeoCountryCode
      ,NumMentions
	  ,SourceURL 
	  ,Actor1Key
  FROM [GDELT].[GDELT20].[FactEvent] as fe
	inner join gdelt.gdelt20.DimGeo as action_dg
		on fe.ActionGeoKey = action_dg.GeoKey
  where fe.AddedDateKey >= @StartDateInt
  and fe.AddedDateKey < @EndDateInt
 and fe.Actor1Key >=7000
 and fe.Actor1Key < 20000