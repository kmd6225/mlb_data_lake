create table mlb_db.pitcher_fact (
  game_key int,
  game_date date,
  pitcher_key int, 
  pitching_team_key int,
  at_bats int,
  innings_pitched int,
  walks int,
  hits int,
  strikeouts int,
  hit_by_pitch int
);