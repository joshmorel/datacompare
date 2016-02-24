select 
	da.ActorKey,
	da.ActorName
from GDELT.GDELT20.DimActor as da

where actorkey between 7 and 17