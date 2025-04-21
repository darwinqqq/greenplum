-- DROP FUNCTION std11_3.f_delta_partition(text, text, timestamp, text, text, text);

CREATE OR REPLACE FUNCTION std11_3.f_delta_partition(p_table text, p_partition_field text, p_start_date timestamp, p_pxf_table text, p_user text, p_pass text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare

	v_ext_table text;
	v_temp_table text;
	v_sql text;
	v_pxf text;
	v_cnt int8;
	v_result text;
	v_dist_key text;
	v_where text;
	v_param text;
	v_load_interval interval;
	v_start_date date;
	v_end_date date;
	v_table_oid int4;
	
begin
	
	v_ext_table = p_table||'_ext';
	v_temp_table = p_table||'_tmp';

	SELECT c.oid
	INTO v_table_oid
	FROM pg_class AS c INNER JOIN pg_namespace AS n ON c.relnamespace = n.oid
	WHERE n.nspname||'.'||c.relname = p_table
	LIMIT 1;

	IF v_table_oid = 0 OR v_table_oid IS NULL THEN
		v_dist_key = 'RANDOMLY';
	ELSE
		v_dist_key = pg_get_table_distributedby(v_table_oid);
	END IF;
	
	SELECT COALESCE('with (' ||ARRAY_TO_STRING (reloptions, ',') || ')', ' ')
	FROM pg_class
	INTO v_param
	WHERE oid = p_table::REGCLASS;

	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table;
	
	v_load_interval := '1_month'::INTERVAL;
	v_start_date := DATE_TRUNC('month', p_start_date);
	v_end_date := v_start_date + v_load_interval ;

	v_where = p_partition_field||' >= '''||v_start_date||'''::date AND '||p_partition_field||' < '''||v_end_date||'''::date';

	RAISE NOTICE 'Where is: %', v_where;	

	v_pxf = 'pxf://'||p_pxf_table||'?&PROFILE=jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER='
					||p_user||'&PASS='||p_pass;

	RAISE NOTICE 'PXF CONNECTION STRING: %', v_pxf;

	v_sql := 'CREATE EXTERNAL TABLE '||v_ext_table||' (LIKE '||p_table||') '
			'LOCATION ('''||v_pxf||''') ON ALL '
			'FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'') '
			'ENCODING ''UTF8'';';
	
	RAISE NOTICE 'EXTERNAL TABLE IS: %', v_sql;

	EXECUTE v_sql;

	v_sql := 'DROP TABLE IF EXISTS '||v_temp_table||';'
			'CREATE TABLE '||v_temp_table||' (LIKE '||p_table||' ) '||v_param||' '||v_dist_key||';';
			
	RAISE NOTICE 'TEMP TABLE IS: %', v_sql;

	EXECUTE v_sql;
	
	EXECUTE 'INSERT INTO '||v_temp_table||' SELECT * FROM '||v_ext_table||' WHERE '||v_where;

	v_sql := 'ALTER TABLE '||p_table||' EXCHANGE PARTITION FOR (DATE '''||v_start_date||''') WITH TABLE '||v_temp_table||' WITH VALIDATION';
	
	RAISE NOTICE 'EXCHANGE PARTITION SCRIPT: %', v_sql;

	EXECUTE v_sql;
	
	EXECUTE 'SELECT COUNT(1) FROM '||p_table||' WHERE '||v_where INTO v_result;
	
	RETURN v_result;
	
end;


$$
EXECUTE ON ANY;
