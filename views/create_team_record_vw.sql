create view mlb_db.team_record_vw as
 with run_totals as (
  select team_name, sum(runs_scored) as runs_scored, sum(runs_allowed) as runs_allowed
  from ( (
    select td.team_name, sum(home_score) as runs_scored, sum(away_score) as runs_allowed
  from `mlb_db.game_fact` gf 
  join mlb_db.team_dim td 
  on gf.home_team_key = td.team_key
  group by 1)
  union all 
  (select td.team_name, sum(away_score) as runs_scored, sum(home_score) as runs_allowed
  from `mlb_db.game_fact` gf 
  join mlb_db.team_dim td 
  on gf.away_team_key = td.team_key
  group by 1))
  group by 1),

 projected as ( 
  select team_name, round(power(runs_scored,1.83)/(power(runs_scored, 1.83) + power(runs_allowed, 1.83)) * 162,0) as expected_wins
  from run_totals
  order by 2 desc),

record as (
select team_name, sum(case when runs_allowed < runs_scored then 1 else 0 end) as wins, sum(case when runs_allowed > runs_scored then 1 else 0 end) as losses
from ((select td.team_name, game_key, sum(home_score) as runs_scored, sum(away_score) as runs_allowed
  from `mlb_db.game_fact` gf 
  join mlb_db.team_dim td 
  on gf.home_team_key = td.team_key
  group by 1,2)
  union all 
  (select td.team_name, game_key, sum(away_score) as runs_scored, sum(home_score) as runs_allowed
  from `mlb_db.game_fact` gf 
  join mlb_db.team_dim td 
  on gf.away_team_key = td.team_key
  group by 1,2))
group by 1
  )

select r.*, p,expected_wins
from record r 
join projected p 
on r.team_name = p.team_name