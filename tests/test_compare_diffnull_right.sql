declare @StartDate as date, @EndDate as date, @StartDateInt as int, @EndDateInt as int;
set @StartDate = '2015-06-29'; set @EndDate = '2015-09-30';

SELECT [DATE_KEY]
      ,[FULL_DATE]
      ,[DATE_YYYYMMDD]
      ,[DATE_MMDDYYYY]
      ,[DATE_DDMMYYYY]
      ,case when [DAY_OF_WEEK] = 2 then null else [DAY_OF_WEEK] end as [DAY_OF_WEEK]
      ,case when [DAY_OF_WEEK_NAME] = 'Monday' then null else [DAY_OF_WEEK_NAME] end as [DAY_OF_WEEK_NAME]
      ,[DAY_OF_WEEK_ABBREVIATION]
  FROM [GDELT].[COMMON].[DimDate] as dd
  where dd.FULL_DATE >= @StartDate 
  and dd.FULL_DATE < @EndDate