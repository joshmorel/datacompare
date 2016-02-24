declare @StartDate as date, @EndDate as date, @StartDateInt as bigint, @EndDateInt as bigint;
set @StartDate = '2015-09-29'; set @EndDate = '2015-09-30';
set @StartDateInt = cast(convert(char(8),@StartDate,112) as bigint)*1000000;
set @EndDateInt = cast(convert(char(8),@EndDate,112) as bigint)*1000000;
