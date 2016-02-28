select 
	da.ActorKey,
	da.ActorName,
	case when da.ActorCountryDesc = 'n/a' then 'Not available'
		else da.ActorCountryDesc  end as ActorCountryDesc 
from GDELT.GDELT20.DimActor as da
where ActorKey < 200