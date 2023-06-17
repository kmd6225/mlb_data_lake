create view new_pitch_vw as 
	select *, 
	HASHBYTES('SHA2_256', concat(game_date, '|', start_time)) as pitch_key ,
	row_number() over(partition by game_date, game_key, matchup_pitcher_id order by start_time) as game_pitch_number
	from pitch_stg
	where is_Pitch = 1 