select 
	da.ActorKey,
	da.ActorName
from GDELT.GDELT20.DimActor as da

where actorkey between 3 and 10