select 
	da.ActorKnownGroupCode,
	da.ActorKey,
	da.ActorName
from GDELT.GDELT20.DimActor as da
where da.actorkey < 200
