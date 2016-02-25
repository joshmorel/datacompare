select 
	da.ActorKey,
	da.ActorName
from GDELT.GDELT20.DimActor as da
where ActorKey < 200