-- DROP FUNCTION std11_3.f_load_write_log(text, text, text);

CREATE OR REPLACE FUNCTION std11_3.f_load_write_log(p_log_type text, p_log_message text, p_location text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare
 v_log_type text;
 v_log_message text;
 v_sql text;
 v_location text;
 v_res text;

begin

-- проверка типа сообщения
 v_log_type = upper(p_log_type);
 v_location = lower(p_location);

 if v_log_type not in ('ERROR', 'INFO') then 
	raise exception 'Illegal log type! Use one of: ERROR, INFO.';
 end if;

 raise notice '%: %: <%> Location[%]', clock_timestamp(), v_log_type, p_log_message, v_location;

 v_log_message := replace(p_log_message, '''', '''''');

 v_sql := 'insert into std11_3.logs 
			values (' || nextval('std11_3.log_id_seq') ||',
					 current_timestamp, 
					''' || v_log_type || ''',
					' || coalesce('''' || v_log_message || '''', '''empty''') || ',
					' || coalesce('''' || v_location || '''', 'null') || ',
					' || case when v_log_type = 'ERROR' then true else false end || ',
					 current_user);';

 raise notice 'INSERT SQL IS: %', v_sql;
 -- v_res := dblink('adb_server', v_sql);
 execute v_sql; 

end;

$$
EXECUTE ON ANY;
