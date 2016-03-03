select 
	da.ActorKey,
	case when da.ActorName = 'AMERICAN' then null else ActorName end as AaactorName,
	da.ActorName,
	case when da.ActorCountryDesc = 'n/a' then null else ActorCountryDesc end as ActorCountryDesc
from GDELT.GDELT20.DimActor as da

where actorkey between 15 and 25