select 
	da.ActorName,
	da.ActorKnownGroupCode,
	da.ActorKey
from GDELT.GDELT20.DimActor as da

where da.actorkey < 200
