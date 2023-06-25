

library(baseballr)
library(DBI)
library(bigrquery)
library(dplyr)
library(data.table)
#-------------------------get pitch by pitch data for each game-------------------------------------
get_pitches <- function(start_date){
  game_ids <- get_game_pks_mlb(start_date)
  
  
  keys <- game_ids$game_pk
  #get all pitches 
  
  pitches <- get_pbp_mlb(keys[1])
  
  if (length(keys) >= 2){
    
    for (k in 2:length(keys)){
      
      current_key <- keys[k]
      
      pitches <- rbind(pitches, get_pbp_mlb(current_key), fill = TRUE)}}
 
  return(pitches)}
# connect to the db

con <- dbConnect(bigquery(),
                 project = 'apt-terrain-390117' ,
                 dataset = 'mlb_db',
                 billing = 'apt-terrain-390117')




start_dat <- dbGetQuery(con, 'select max(game_date) from mlb_db.config_param')

while (as.Date(start_dat[[1]][1]) <= as.Date('2023-04-10')){


#call the function to get data from the MLB api 

pitches <- get_pitches(start_dat[[1]][1])
if (nrow(pitches) > 0){
#select only relevant columns





pitches_filtered <-pitches %>% select(game_pk, game_date,startTime, isPitch, pitchNumber, details.type.code,  
                              details.event, details.call.description, details.homeScore, details.awayScore, details.isInPlay,
                              details.isStrike, details.isBall, count.balls.start,count.strikes.start,count.outs.start,
                              player.id,pitchData.coordinates.x, pitchData.coordinates.y, result.type, result.event,
                              result.eventType, result.awayScore, result.homeScore, about.halfInning,
                              about.inning,about.isScoringPlay, matchup.batter.id, matchup.pitcher.fullName, matchup.batter.fullName,
                              matchup.pitcher.id,matchup.pitchHand.description,
                              home_team, away_team, home_league_name, away_league_name,batting_team, fielding_team,
                              pitchData.startSpeed, pitchData.endSpeed, pitchData.zone, pitchData.breaks.spinRate,
                              hitData.launchSpeed, hitData.launchAngle,hitData.totalDistance, result.isOut)
col_names <- colnames(pitches_filtered)
new_col_names <- c('game_key',
                   'game_date',
                   'start_time',
                   'is_Pitch',
                   'details_type_code',
                   'pitch_Number',
                   'details_event',
                   'details_call_description',
                   'details_home_Score',
                   'details_away_Score',
                   'details_is_In_Play',
                   'details_is_Strike',
                   'details_is_Ball',
                   'count_balls_start',
                   'count_strikes_start',
                   'count_outs_start',
                   'player_id',
                   'pitch_Data_coordinates_x',
                   'pitch_Data_coordinates_y',
                   'result_type',
                   'result_event',
                   'result_event_Type',
                   'result_away_Score',
                   'result_home_Score',
                   'about_half_Inning',
                   'about_inning',
                   'about_is_Scoring_Play',
                   'matchup_batter_id',
                   'matchup_pitcher_fullName',
                   'matchup_batter_fullName',
                   'matchup_pitcher_id',
                   'matchup_pitchHand_description',
                   'home_team',
                   'away_team',
                   'home_league_name',
                   'away_league_name',
                   'batting_team',
                   'fielding_team',
                   'pitch_Data_startSpeed',
                   'pitch_Data_endSpeed',
                   'pitch_Data_zone',
                   'pitch_Data_breaks_spin_Rate',
                   'hit_Data_launch_Speed',
                   'hit_Data_launch_Angle',
                   'hit_Data_total_Distance',
                   'result_is_Out')

pitches_filtered_renamed <- setnames(pitches_filtered, old = col_names,
                                     new = new_col_names )



# Write to SQL server


stg <- bq_table(project = 'apt-terrain-390117', dataset = 'mlb_db', table = 'pitch_stg')

bq_table_upload(x=stg, values=data.frame(pitches_filtered_renamed), create_disposition='CREATE_IF_NEEDED', write_disposition='WRITE_TRUNCATE')
}


rs <- bq_perform_query('call mlb_db.mlb_pipeline()',project = 'apt-terrain-390117', dataset = 'mlb_db', billing = 'apt-terrain-390117' )

print(start_dat)
print('sleeping')
Sys.sleep(15)
print('waking up')
start_dat <- dbGetQuery(con, 'select max(game_date) from mlb_db.config_param')
}

dbDisconnect(con)

