	create view dim_player_roster_vw as
	with batters as (
	select batting_team as team, 
		matchup_batter_id as v_player_id, 
		matchup_batter_fullName as v_player_name,
		hashbytes('SHA2_256', concat(
						batting_team, '|',
						matchup_batter_id, '|',
						matchup_batter_fullName, '|'
						)) as player_roster_key
	from pitch_fact
	), 

	fielding as (
	select fielding_team as team, 
		matchup_pitcher_id as v_player_id, 
		matchup_pitcher_fullName as v_player_name,
		hashbytes('SHA2_256', concat(
						fielding_team, '|',
						matchup_pitcher_id, '|',
						matchup_pitcher_fullName, '|'
						)) as player_roster_key

	from pitch_fact
	)

select *
	from batters
	union 
	select *
	from fielding