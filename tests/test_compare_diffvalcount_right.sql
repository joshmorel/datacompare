select 
	da.ActorKey
	,da.ActorName
	,'n/a' as ActorEthnicDesc

from GDELT.GDELT20.DimActor as da
where ActorKey < 50