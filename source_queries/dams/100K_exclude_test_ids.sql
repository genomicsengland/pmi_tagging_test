-- query to get all non-test IDs within DAMS
select p.participant_id
from rarediseases.participant_identifier p
where floor(p.participant_id::int / 1000000) not in (0, 100, 200, 900)
union
select p.participant_id
from gelcancer.participant_identifier p
where floor(p.participant_id::int / 1000000) not in (0, 100, 200, 900)
;
