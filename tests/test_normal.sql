declare @StartDate as date, @EndDate as date, @StartDateInt as int, @EndDateInt as int;
set @StartDate = '2015-09-29'; set @EndDate = '2015-09-30';
set @StartDateInt = cast(convert(char(8),@StartDate,112) as int);
set @EndDateInt = cast(convert(char(8),@EndDate,112) as int);




SELECT [FactEventKey]
      ,[OccurrenceDateKey]
      ,[AddedDateKey]
      ,[AddedTimeKey]
      ,[QuadClassKey]
      ,[Actor1Key]
      ,[Actor2Key]
      ,[IsRootEvent]
      ,[EventCodeKey]
      ,[Actor1GeoKey]
      ,[Actor2GeoKey]
      ,[ActionGeoKey]
	  ,action_dg.GeoCountryCode
      ,[GoldsteinScale]
      ,[NumMentions]
      ,[NumSources]
      ,[NumArticles]
      ,[AvgTone]
      ,[SourceURL]
      ,[SourceLanguage]
  FROM [GDELT].[GDELT20].[FactEvent] as fe
	inner join gdelt.gdelt20.DimGeo as action_dg
		on fe.ActionGeoKey = action_dg.GeoKey
  where fe.AddedDateKey >= @StartDateInt
  and fe.AddedDateKey < @EndDateInt
  and fe.Actor1Key >=   50
    and action_dg.GeoCountryCode = 'CA'
  order by FactEventKey