select 
	da.ActorKey
	,da.ActorName
	,da.ActorCode
	,da.ActorCode

from GDELT.GDELT20.DimActor as da
where ActorKey < 200