create view first_run_pitch_vw as

select *, row_number() over(partition by game_key, matchup_pitcher_id order by start_time) as game_pitch_number
from pitch_stg
where is_Pitch = 1
