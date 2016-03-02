select 
	da.ActorKey
	,left(da.ActorName,20) as ActorName
	,da.ActorEthnicDesc

from GDELT.GDELT20.DimActor as da
where ActorKey < 50