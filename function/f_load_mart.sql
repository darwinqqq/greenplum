-- DROP FUNCTION std11_3.f_load_mart(varchar);

CREATE OR REPLACE FUNCTION std11_3.f_load_mart(p_month varchar)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
 v_table_name text;
 v_sql text;
 v_return int;
 v_mart_name text;
begin
   
	v_mart_name := 'plan_fact'||p_month;

	perform std11_3.f_load_write_log(p_log_type := 'INFO',
									  p_log_message := 'Start f_load_mart',
									  p_location := 'Sales mart calculation');

	drop table if exists std11_3.v_mart_name;
	
		create table std11_3.v_mart_name
		 WITH (
			appendonly=true,
			orientation=column,
			compresstype=zstd,
			compresslevel=1)
		 as 
		  select region
		 		 , material
		 		 , distr_chan
		 		 , sum(quantity) as qnt
		 		 , count(distinct check_nm) as chk_cnt
		  from std11_26.sales
		 	where "date" between date_trunc('month', to_date(p_month, 'YYYYMM')) - interval '3 month'
		 	  and date_trunc('month', to_date(p_month, 'YYYYMM'))
		  group by 1,2,3
		 distributed by (material);

	select count(*) into v_return from std11_26.mart;
	
	perform std11_3.f_load_write_log(p_log_type := 'INFO',
									  p_log_message := v_return || ' rows inserted',
									  p_location := 'Sales mart calculation');

	perform std11_3.f_load_write_log(p_log_type := 'INFO',
									  p_log_message := 'End f_load_mart',
									  p_location := 'Sales mart calculation');
	
	return v_return;
end;

$$
EXECUTE ON ANY;
