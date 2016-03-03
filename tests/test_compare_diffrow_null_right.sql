select 
	da.ActorKey,
	ActorName as AaactorName,
	da.ActorName,
	ActorCountryDesc
from GDELT.GDELT20.DimActor as da

where actorkey between 15 and 26