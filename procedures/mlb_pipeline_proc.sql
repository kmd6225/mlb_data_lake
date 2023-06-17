create procedure mlb_pipeline 
@is_first_run int 
as 
	if @is_first_run = 1
		begin 
			declare @v_dyn_sql_1 nvarchar(max)
			set @v_dyn_sql_1 = 'select * into pitch_fact from new_pitch_vw'
			execute sp_executesql  @v_dyn_sql_1
		end 
	else
		begin 
			declare @v_dyn_sql_2 nvarchar(max)
			set @v_dyn_sql_2 = 'insert  into pitch_fact select * from new_pitch_vw where game_key not in (select distinct game_key from new_pitch_vw)'
			execute sp_executesql  @v_dyn_sql_2
		end 
	begin 
		declare @max_date date
		declare cursor_config cursor for 
			select max(game_date) from config_param
		open cursor_config 
		fetch next from cursor_config into @max_date

		while @@FETCH_STATUS = 0

		begin 
		
			update config_param set run_status = 'success' where game_date = @max_date
			update config_param set next_game_date = dateadd(day,1,@max_date) where game_date = @max_date
			insert into config_param values(concat('report run at ',cast(datetrunc(day,CURRENT_TIMESTAMP) as nvarchar(max))), dateadd(day,1,@max_date), null, null)
			fetch next from cursor_config into @max_date
		end
		close cursor_config
		deallocate cursor_config
	end