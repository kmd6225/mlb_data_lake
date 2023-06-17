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