create database mlb;

use mlb;
drop table pitch_fact;
create table pitch_fact(
game_key nvarchar(max),
game_date date,
start_time nvarchar(max),
is_Pitch int,
pitch_count int,
pitch_type nvarchar(max), 
details_event nvarchar(max),
details_call_description nvarchar(max),
details_home_Score int,
details_away_Score int,
details_is_In_Play int,
details_is_Strike int,
details_is_Ball  int,
count_balls_start  int,
count_strikes_start  int,
count_outs_start  int,
player_id nvarchar(max),
pitch_Data_coordinates_x int,
pitch_Data_coordinates_y  int,
result_type nvarchar(max),
result_event nvarchar(max),
result_event_Type nvarchar(max),
result_away_Score int,
result_home_Score  int,
about_half_Inning nvarchar(max),
about_inning nvarchar(max),
about_is_Scoring_Play  int,
matchup_batter_id nvarchar(max),
matchup_pitcher_fullName nvarchar(max),
matchup_batter_fullName nvarchar(max),
matchup_pitcher_id nvarchar(max),
matchup_pitchHand_description nvarchar(max),
home_team nvarchar(max), 
away_team nvarchar(max),
home_league_name nvarchar(max),
away_league_name nvarchar(max),
batting_team nvarchar(max),
fielding_team nvarchar(max),
pitch_Data_startSpeed  int,
pitch_Data_endSpeed  int,
pitch_Data_zone  int,
pitch_Data_breaks_spin_Rate  int,
hit_Data_launch_Speed  int,
hit_Data_launch_Angle  int,
hit_Data_total_Distance  int,
result_is_Out  int,)



drop table pitch_fact;

drop table pitch_stg;


create table pitch_stg (
game_key int,
game_date date,
start_time nvarchar(max),
is_Pitch nvarchar(max),
details_type_code nvarchar(max),
pitch_Number nvarchar(max), 
details_event nvarchar(max),
details_call_description nvarchar(max),
details_home_Score int,
details_away_Score int,
details_is_In_Play nvarchar(max),
details_is_Strike nvarchar(max),
details_is_Ball nvarchar(max),
count_balls_start int,
count_strikes_start int,
count_outs_start int,
player_id int,
pitch_Data_coordinates_x numeric,
pitch_Data_coordinates_y numeric,
result_type nvarchar(max),
result_event nvarchar(max),
result_event_Type nvarchar(max),
result_away_Score int,
result_home_Score int,
about_half_Inning nvarchar(max),
about_inning nvarchar(max),
about_is_Scoring_Play nvarchar(max),
matchup_batter_id int,
matchup_pitcher_fullName varchar(max),
matchup_batter_fullName varchar,
matchup_pitcher_id int,
matchup_pitchHand_description nvarchar(max),
home_team nvarchar(max), 
away_team nvarchar(max),
home_league_name nvarchar(max),
away_league_name nvarchar(max),
batting_team nvarchar(max),
fielding_team nvarchar(max),
pitch_Data_startSpeed numeric, 
pitch_Data_endSpeed numeric,
pitch_Data_zone int,
pitch_Data_breaks_spin_Rate int,
hit_Data_launch_Speed numeric,
hit_Data_launch_Angle numeric,
hit_Data_total_Distance numeric, 
result_is_Out nvarchar(max));


select * from pitch_stg;

-----


with a as (select hashbytes('SHA2_256', concat(
				game_key, '|',
				game_date, '|',
				is_Pitch, '|',
				details_type_code, '|',
				pitch_Number, '|',
				details_event, '|',
				details_call_description, '|',
				details_home_Score, '|',
				details_away_Score, '|',
				details_is_In_Play, '|',
				details_is_Strike, '|',
				details_is_Ball, '|',
				count_balls_start, '|',
				count_strikes_start, '|',
				count_outs_start, '|',
				player_id, '|',
				pitch_Data_coordinates_x, '|',
				pitch_Data_coordinates_y, '|',
				result_type, '|',
				result_event, '|',
				result_event_Type, '|',
				result_away_Score, '|',
				result_home_Score, '|',
				about_half_Inning, '|',
				about_inning, '|',
				about_is_Scoring_Play, '|',
				matchup_batter_id, '|' ,
				matchup_pitcher_fullName, '|' ,
				matchup_batter_fullName, '|' ,
				matchup_pitcher_id, '|',
				matchup_batter_id, '|',
				matchup_pitcher_id, '|',
				matchup_pitchHand_description, '|',
				home_team, '|',
				away_team, '|',
				home_league_name, '|',
				away_league_name, '|',
				batting_team, '|',
				fielding_team, '|',
				pitch_Data_startSpeed, '|',
				pitch_Data_endSpeed, '|',
				pitch_Data_zone, '|',
				pitch_Data_breaks_spin_Rate, '|',
				hit_Data_launch_Speed, '|',
				hit_Data_launch_Angle, '|',
				hit_Data_total_Distance, '|',
				result_is_Out, '|')) as pitch_key
from pitch_stg
where is_Pitch = 'TRUE')

select pitch_key, count(*)
from a 
group by pitch_key
having count(*) > 1


select *
from information_schema.columns
where table_name = 'pitch_stg';


---- 
drop view new_pitch_vw
create view new_pitch_vw as

with new_pitches as(
	select *, hashbytes('SHA2_256', concat(
				game_key, '|',
				game_date, '|',
				is_Pitch, '|',
				details_type_code, '|',
				pitch_Number, '|',
				details_event, '|',
				details_call_description, '|',
				details_home_Score, '|',
				details_away_Score, '|',
				details_is_In_Play, '|',
				details_is_Strike, '|',
				details_is_Ball, '|',
				count_balls_start, '|',
				count_strikes_start, '|',
				count_outs_start, '|',
				player_id, '|',
				pitch_Data_coordinates_x, '|',
				pitch_Data_coordinates_y, '|',
				result_type, '|',
				result_event, '|',
				result_event_Type, '|',
				result_away_Score, '|',
				result_home_Score, '|',
				about_half_Inning, '|',
				about_inning, '|',
				about_is_Scoring_Play, '|',
				matchup_batter_id, '|' ,
				matchup_pitcher_id, '|',
				matchup_pitchHand_description, '|',
				home_team, '|',
				away_team, '|',
				home_league_name, '|',
				away_league_name, '|',
				batting_team, '|',
				fielding_team, '|',
				pitch_Data_startSpeed, '|',
				pitch_Data_endSpeed, '|',
				pitch_Data_zone, '|',
				pitch_Data_breaks_spin_Rate, '|',
				hit_Data_launch_Speed, '|',
				hit_Data_launch_Angle, '|',
				hit_Data_total_Distance, '|',
				result_is_Out, '|')) as pitch_key
from pitch_stg
where is_Pitch = 1
),

old_pitches as (
	select hashbytes('SHA2_256', concat(
				game_key, '|',
				game_date, '|',
				is_Pitch, '|',
				details_type_code, '|',
				pitch_Number, '|',
				details_event, '|',
				details_call_description, '|',
				details_home_Score, '|',
				details_away_Score, '|',
				details_is_In_Play, '|',
				details_is_Strike, '|',
				details_is_Ball, '|',
				count_balls_start, '|',
				count_strikes_start, '|',
				count_outs_start, '|',
				player_id, '|',
				pitch_Data_coordinates_x, '|',
				pitch_Data_coordinates_y, '|',
				result_type, '|',
				result_event, '|',
				result_event_Type, '|',
				result_away_Score, '|',
				result_home_Score, '|',
				about_half_Inning, '|',
				about_inning, '|',
				about_is_Scoring_Play, '|',
				matchup_batter_id, '|' ,
				matchup_pitcher_id, '|',
				matchup_pitchHand_description, '|',
				home_team, '|',
				away_team, '|',
				home_league_name, '|',
				away_league_name, '|',
				batting_team, '|',
				fielding_team, '|',
				pitch_Data_startSpeed, '|',
				pitch_Data_endSpeed, '|',
				pitch_Data_zone, '|',
				pitch_Data_breaks_spin_Rate, '|',
				hit_Data_launch_Speed, '|',
				hit_Data_launch_Angle, '|',
				hit_Data_total_Distance, '|',
				result_is_Out, '|')) as pitch_key
from pitch_fact
where is_Pitch = 1
)

select distinct game_key,
game_date,
start_time,
is_Pitch,
details_type_code,
pitch_Number,
details_event,
details_call_description,
details_home_Score,
details_away_Score,
details_is_In_Play,
details_is_Strike,
details_is_Ball,
count_balls_start,
count_strikes_start,
count_outs_start,
player_id,
pitch_Data_coordinates_x,
pitch_Data_coordinates_y,
result_type,
result_event,
result_event_Type,
result_away_Score,
result_home_Score,
about_half_Inning,
about_inning,
about_is_Scoring_Play,
matchup_batter_id,
matchup_pitcher_fullName,
matchup_batter_fullName,
matchup_pitcher_id,
matchup_pitchHand_description,
home_team,
away_team,
home_league_name,
away_league_name,
batting_team,
fielding_team,
pitch_Data_startSpeed,
pitch_Data_endSpeed,
pitch_Data_zone,
pitch_Data_breaks_spin_Rate,
hit_Data_launch_Speed,
hit_Data_launch_Angle,
hit_Data_total_Distance,
result_is_Out,
row_number() over(partition by game_key, matchup_pitcher_id order by 
from new_pitches 
where pitch_key not in (select pitch_key from old_pitches)
----
drop view first_run_pitch_vw
create view first_run_pitch_vw as

select *
from pitch_stg
where is_Pitch = 1




-----
drop procedure mlb_pipeline

create procedure mlb_pipeline
@is_first_run int
as
		if @is_first_run = 1
		begin  
			declare @current_table nvarchar(max) = 'first_run_pitch_vw'
			declare @target_table nvarchar(max) = 'pitch_fact'
			declare @dyn_sql nvarchar(max)  
			set @dyn_sql = 'insert into ' + @target_table + ' select * from ' + @current_table
			execute sp_executesql @dyn_sql
			end
		else
		begin
		if exists(select table_name from INFORMATION_SCHEMA.tables where table_name = 'new_pitch_tbl') 
		begin 
			declare @current_table1 nvarchar(max) = 'new_pitch_tbl'
			declare @dyn_sql1 nvarchar(max)
			set @dyn_sql1 = 'drop table ' + @current_table1
			execute sp_executesql @dyn_sql1
			end

		begin
			declare @dyn_sql2 nvarchar(max)
			declare @current_table2 nvarchar(max) = 'new_pitch_tbl'
			declare	@current_vw2 nvarchar(max) = 'new_pitch_vw'
			set @dyn_sql2 = 'select * into ' + @current_table2 + ' from ' + @current_vw2
			execute sp_executesql @dyn_sql2
			end
		begin
		declare @dyn_sql3 nvarchar(max)
		declare @current_table3 nvarchar(max) = 'new_pitch_tbl'
		declare @target_table3 nvarchar(max) = 'pitch_fact'
		set @dyn_sql3 = 'insert into ' + @target_table3 + ' select * from ' + @current_table3
		execute sp_executesql @dyn_sql3
			end
		end




			select * from first_run_pitch_vw

exec dbo.mlb_pipeline @is_first_run = 0
	
	select * from pitch_stg

	select * from pitch_fact;

	select * from INFORMATION_SCHEMA.columns where table_name = 'new_pitch_vw'


	select count(*) from pitch_fact where home_team = 'San Diego Padres' and away_team = 'Colorado Rockies';


	select * from pitch_fact where home_team = 'New York Yankees' or away_team = 'New York Yankees'


	---
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


 dim_player_roster_vw 
select * from  dim_player_roster_vw 

------views for tableau  ---

select fielding_team, matchup_pitcher_id, matchup_pitcher_fullName, pitch_count, round(avg(cast(pitch_Data_startSpeed as float)),2) as avg_pitch_speed
from pitch_fact
--where matchup_pitcher_fullName = 'Gerrit Cole'
group by fielding_team, matchup_pitcher_id, matchup_pitcher_fullName, pitch_count
order by matchup_pitcher_fullName, cast(pitch_count as int);


select *
from pitch_fact
where is_Pitch = 1;

alter table pitch_fact 
alter column is_Pitch int;
alter table pitch_fact 
alter column pitch_count int;
alter table pitch_fact 
alter column details_is_in_play int
alter table pitch_fact 
alter column details_is_strike int
alter table pitch_fact 
alter column details_is_Ball int
alter table pitch_fact 
alter column count_balls_start int
alter table pitch_fact  
alter column count_strikes_start int
alter table pitch_fact 
alter column count_outs_start int
alter table pitch_fact 
alter column pitch_data_coordinates_x int
alter table pitch_fact 
alter column pitch_data_coordinates_y int
alter table pitch_fact 
alter column result_away_score int
alter table pitch_fact 
alter column result_home_score int
alter table pitch_fact 
alter column about_is_Scoring_play int
alter table pitch_fact 
alter column pitch_Data_endSpeed int 
alter table pitch_fact  
alter column pitch_data_zone int 
alter table pitch_fact  
alter column pitch_Data_breaks_spin_Rate int 
alter table pitch_fact 
alter column hit_Data_launch_Speed int
alter table pitch_fact  
alter column hit_Data_launch_Angle int
alter table pitch_fact  
alter column hit_Data_total_Distance int
alter table pitch_fact  
alter column result_is_Out int
alter table pitch_stg
alter column pitch_Data_endSpeed int
alter table pitch_stg
alter column details_home_Score int 
alter table pitch_stg 
alter column details_away_Score int

alter table pitch_stg
alter column game_date date



