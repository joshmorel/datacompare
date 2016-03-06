select 
	da.ActorKey
	,case when da.ActorKey = 4 then 'blah' else da.ActorCode end as ActorCode
	,da.ActorEthnicCode
	,da.ActorReligion1Code
from GDELT.GDELT20.DimActor as da
where ActorKey < 200