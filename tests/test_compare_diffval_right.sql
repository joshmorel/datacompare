select 
	da.ActorKey,
	replace(da.ActorName,'h','') ActorName
from GDELT.GDELT20.DimActor as da

where ActorKey < 200