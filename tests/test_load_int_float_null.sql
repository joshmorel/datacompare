select 
fe.FactEventKey
,case when fe.FactEventKey =  410399548 then null else fe.FactEventKey end as intnull
,case when fe.FactEventKey =  410399544 then cast(dd.FULL_DATE as datetime) else null end as DateDT
,dd.FULL_DATE as DateDT2
,case when fe.FactEventKey =  410399544 then null else fe.AvgTone end as AvgTone
,cast(case when fe.FactEventKey =  410399544 then null else fe.NumArticles  end as int) as NumArticles
,cast(dd.FULL_DATE as date) as DateNoNull
,cast(dd.FULL_DATE as datetime) as DateTimeNoNull
,cast(dd.FULL_DATE as datetime2(0)) as DateTime2NoNull
,case when fe.FactEventKey =  410399548 then null else cast(dd.FULL_DATE as datetime2(0)) end as DateTime2LastNull

from GDELT.GDELT20.FactEvent as fe
	inner join GDELT.COMMON.DimDate as dd
		on fe.OccurrenceDateKey = dd.DATE_KEY
where fe.FactEventKey >= 410399541 and fe.FactEventKey < 410399549
order by FactEventKey