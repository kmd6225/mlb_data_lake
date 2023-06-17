create table config_param (
prev_run_status nvarchar(max),
game_date date,
next_game_date date,
run_status nvarchar(max)
);

insert into config_param values ('initial run' , cast('2023-04-04' as date), null, null);