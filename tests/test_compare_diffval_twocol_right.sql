select 
	da.ActorKey,
	replace(da.ActorName,'h','') ActorName
	,da.ActorCountryDesc
from GDELT.GDELT20.DimActor as da

where ActorKey < 200