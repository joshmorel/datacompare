select 
	da.ActorKey
	,da.ActorName
	,da.ActorCode
	,da.ActorKnownGroupDesc
from GDELT.GDELT20.DimActor as da
where ActorKey < 200