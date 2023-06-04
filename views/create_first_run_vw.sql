create view first_run_pitch_vw as

select *
from pitch_stg
where is_Pitch = 1
