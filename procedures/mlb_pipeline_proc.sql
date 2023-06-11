create procedure mlb_pipeline
@is_first_run int
as
		if @is_first_run = 1
		begin  
			declare @current_table nvarchar(max) = 'first_run_pitch_vw'
			declare @target_table nvarchar(max) = 'pitch_fact'
			declare @dyn_sql nvarchar(max)  
			set @dyn_sql = 'select * into ' + @target_table + ' from ' + @current_table
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