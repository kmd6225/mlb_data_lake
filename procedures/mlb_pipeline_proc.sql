create or replace procedure mlb_db.mlb_pipeline(is_first_run bool)
begin 
declare v_dyn_sql_1 string;
declare v_interval int64;
declare starting_date date;
declare v_dyn_sql_2 string;
declare max_date date;

set v_interval = 1;
if is_first_run = true then
		set v_dyn_sql_1 = 'create table mlb_db.pitch_fact as select * from mlb_db.new_pitch_vw';
		execute immediate v_dyn_sql_1;

		set starting_date = (select game_date from mlb_db.config_param where prev_run_status = 'initial run');

    update mlb_db.config_param set run_status = 'success' where game_date = starting_date;
		update mlb_db.config_param set next_game_date = date_add(starting_date, INTERVAL v_interval DAY) where game_date = starting_date;
	  insert into mlb_db.config_param values(concat('report run at ',cast(date_trunc(CURRENT_TIMESTAMP, DAY) as string)), date_add(starting_date, INTERVAL v_interval DAY), null, null);
		
else
  set v_dyn_sql_2 = 'insert into mlb_db.pitch_fact select distinct * from mlb_db.new_pitch_vw where game_key not in (select distinct game_key from mlb_db.pitch_fact)';
	execute immediate  v_dyn_sql_2;
  set max_date = (select max(game_date) from mlb_db.config_param);
	update mlb_db.config_param set run_status = 'success' where game_date = max_date;
	update mlb_db.config_param set next_game_date = date_add(max_date, INTERVAL v_interval DAY) where game_date = max_date;
	insert into mlb_db.config_param values(concat('report run at ',cast(date_trunc(CURRENT_TIMESTAMP, DAY) as string)), date_add(max_date, INTERVAL v_interval DAY), null, null);
  end if;
	end
