create or replace procedure mlb_db.mlb_pipeline()
begin 

  --- create team dim

   begin 
    create temp table team_dim_inter as 
    select * from (
      select home_team as team_name, home_league_name as league_name, FARM_FINGERPRINT(home_team) as team_key
      from mlb_db.pitch_stg 
      union distinct 
      select away_team as team_name, away_league_name as league_name, FARM_FINGERPRINT(away_team) as team_key
      from mlb_db.pitch_stg);
    end;
    insert into mlb_db.team_dim select * from team_dim_inter where team_key not in (select team_key from mlb_db.team_dim);
   
 


  ---- create player dim

   begin  
    create temp table player_dim_inter as
    select * from (
      select matchup_batter_fullName as player_name, FARM_FINGERPRINT(batting_team) as team_key, FARM_FINGERPRINT(matchup_batter_fullName) as player_key
      from mlb_db.pitch_stg 
      union distinct
      select matchup_pitcher_fullName as player_name, FARM_FINGERPRINT(fielding_team) as team_key, FARM_FINGERPRINT(matchup_pitcher_fullName) as player_key
      from mlb_db.pitch_stg);
    end;
    insert into mlb_db.player_dim select * from player_dim_inter where concat(player_key,team_key) not in (select concat(player_key,team_key) from mlb_db.player_dim);


  --- create batter fact and pitcher fact
  begin
    create temp table pitches as
    select *,
      row_number() over(partition by game_date, game_key, matchup_pitcher_id, matchup_batter_id, about_inning, result_event  order by start_time) as pitch_count
	  from mlb_db.pitch_stg
    where is_Pitch = true;
  end;

  begin 

  declare v_dyn_sql_1 string;
  declare v_dyn_sql_2 string;
  declare v_dyn_sql_3 string;
  declare v_dyn_sql_4 string;
  declare var1 string;
  declare var2 string;

    for record in 
      (select 'batter' as arg, 'batting' as arg2 union distinct select 'pitcher' as arg, 'fielding' as arg2)
    do 
      set var1 = record.arg;
      set var2 = record.arg2;

       set v_dyn_sql_1 =  'create temp table pitches_filtered_'||var1||' as ' ||  
                      'select game_date, game_key, FARM_FINGERPRINT(matchup_'||var1||'_fullName) as '||var1||'_key,' ||
                              'FARM_FINGERPRINT('||var2||'_team) as '||var2||'_team_key, about_inning, result_event, max(pitch_count) as max_pitch_count '||
                      'from pitches '||
                      'group by 1,2,3,4,5,6;';

      begin                  
      execute immediate v_dyn_sql_1;
      end;


      if var1 = 'batter' then 

        set v_dyn_sql_2 = 'create temp table '||var1||'_fact_inter as ' ||
                            'select p.game_key,'|| 
                                  'p.game_date,FARM_FINGERPRINT(matchup_'||var1||'_fullName) as '||var1||'_key,'|| 
                                  'FARM_FINGERPRINT('||var2||'_team) as '||var2||'_team_key,'||
                                  'count(*) as at_bats,'||
                                  "sum(case when p.result_event = 'Walk' then 1 else 0 end) as walks,"||
                                  "sum(case when p.result_event = 'Single' or p.result_event = 'Double' or p.result_event = 'Triple' or p.result_event =  'Home Run' then 1 else 0 end) as hits,"||
                                  "sum(case when p.result_event = 'Strikeout' then 1 else 0 end) as strikeouts,"||
                                  "sum(case when p.result_event = 'Single' or p.result_event = 'Double' or p.result_event = 'Triple' or p.result_event =  'Home Run' then 1 else 0 end)/count(*) as batting_avg,"||
                                  "sum(case when p.result_event = 'Walk' or p.result_event = 'Single' or p.result_event = 'Double' or p.result_event = 'Triple' or p.result_event =  'Home Run' then 1 else 0 end)"||
                                  "/count(*) as obp,"||
                                  "sum(case when p.result_event = 'Single' then 1 "|| 
                                            "when p.result_event = 'Double' then 2 "||
                                            "when p.result_event = 'Triple' then 3 "||
                                            "when p.result_event = 'Home Run' then 4 "||
                                            "end)/count(*) as slg ";
      else
        set v_dyn_sql_2 = "create temp table "||var1||"_fact_inter as "|| 
                            "select p.game_key,"|| 
                                  "p.game_date,"||
                                  "FARM_FINGERPRINT(matchup_"||var1||"_fullName) as "||var1||"_key,"||
                                  "FARM_FINGERPRINT("||var2||"_team) as "||var2||"_team_key,"||
                                  "count(*) as at_bats, count(distinct p.about_inning) as innnings_pitched, sum(case when p.result_event = 'Walk' then 1 else 0 end) as walks,"||
                                  "sum(case when p.result_event = 'Single' or p.result_event = 'Double' or p.result_event = 'Triple' or p.result_event =  'Home Run' then 1 else 0 end) as hits,"||
                                  "sum(case when p.result_event = 'Strikeout' then 1 else 0 end) as strikeouts,"||
                                  "sum(case when p.result_event = 'Hit By Pitch' then 1 else 0 end) as hit_by_pitch ";
      end if;
      set v_dyn_sql_3 =   "from pitches p "|| 
                          "join pitches_filtered_"||var1||" pf "|| 
                          "on p.game_date = pf.game_date "||
                          "and p.game_key = pf.game_key "|| 
                          "and FARM_FINGERPRINT(p.matchup_"||var1||"_fullName) = pf."||var1||"_key "|| 
                          "and FARM_FINGERPRINT(p."||var2||"_team) = pf."||var2||"_team_key "||
                          "and p.about_inning = pf.about_inning "||
                          "and p.result_event = pf.result_event "||
                          "and p.pitch_count = pf.max_pitch_count "||
                          "group by 1,2,3,4;";
      begin 
      execute immediate v_dyn_sql_2 || v_dyn_sql_3;
      end;
   set v_dyn_sql_4 = "insert into mlb_db."||var1||"_fact select * from "||var1||"_fact_inter where concat("||var1||"_key,"||var2||"_team_key, game_key) not in (select concat("||var1||"_key,"||var2||"_team_key, game_key) from mlb_db."||var1||"_fact);";
   execute immediate v_dyn_sql_4;
    end for;
  end;


  --- create game fact
  begin 
  create temp table game_fact_inter as 
  select game_date, 
         game_key,
         FARM_FINGERPRINT(home_team) as home_team_key,
         FARM_FINGERPRINT(away_team) as away_team_key ,
         max(result_home_Score) as home_score, 
         max(result_away_Score) as away_score
  from `mlb_db.pitch_stg`
  group by 1,2,3,4;
  end; 
  insert into mlb_db.game_fact select * from game_fact_inter where concat(game_date,home_team_key,away_team_key) not in 
    (select concat(game_date,home_team_key,away_team_key) from mlb_db.game_fact);

-- mlb config param update
begin
  declare max_date date;
  declare v_interval int64;

  set max_date = (select max(game_date) from mlb_db.config_param);
  set v_interval = 1;
	update mlb_db.config_param set run_status = 'success' where game_date = max_date;
	update mlb_db.config_param set next_game_date = date_add(max_date, INTERVAL v_interval DAY) where game_date = max_date;
	insert into mlb_db.config_param values(concat('report run at ',cast(date_trunc(CURRENT_TIMESTAMP, DAY) as string)), date_add(max_date, INTERVAL v_interval DAY), null, null);
end;

  ---truncate stage after completion to avoid duplicates

  ---truncate table mlb_db.pitch_stg;

  end;