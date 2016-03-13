declare @StartDate as date, @EndDate as date, @StartDateInt as int, @EndDateInt as int;
set @StartDate = '2015-09-29'; set @EndDate = '2015-09-30';
set @StartDateInt = cast(convert(char(8),@StartDate,112) as int);
set @EndDateInt = cast(convert(char(8),@EndDate,112) as int);

SELECT  [FactEventKey]
      ,[OccurrenceDateKey]
	  ,cast(dd.FULL_DATE as datetime2(0)) as FullDate
      ,cast(NumMentions as int) as NumMentions
	  	  ,SourceURL
		  ,cast(fe.IsRootEvent as bit) as IsRootEvent
      ,
		cast(NumMentions as decimal(10,2)) as NumMentionsFloatWNulls


  FROM [GDELT].[GDELT20].[FactEvent] as fe
	inner join gdelt.gdelt20.DimGeo as action_dg
		on fe.ActionGeoKey = action_dg.GeoKey
	inner join GDELT.COMMON.DimDate as dd
		on fe.OccurrenceDateKey = dd.DATE_KEY
  where fe.AddedDateKey >= @StartDateInt
  and fe.AddedDateKey < @EndDateInt
 and fe.Actor1Key >=7000
 and fe.Actor1Key < 8000

 order by FactEventKey