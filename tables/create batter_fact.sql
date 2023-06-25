create table mlb_db.batter_fact (
  game_key int,
  game_date date,
  batter_key int, 
  batting_team_key int,
  at_bats int,
  walks int,
  hits int,
  strikeouts int,
  batting_avg float64,
  obp float64,
  slg float64
);