-- DROP FUNCTION std11_3.f_full(text, text);

CREATE OR REPLACE FUNCTION std11_3.f_full(p_table text, p_file_name text)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	

declare
v_ext_table_name text;
v_sql text;
v_gpfdist text;
v_result int;

begin

	v_ext_table_name = p_table||'_ext';

	execute 'TRUNCATE TABLE '||p_table;
	execute 'DROP EXTERNAL TABLE IF EXISTS '||v_ext_table_name;
	
	v_gpfdist = 'GPFDIST://172.16.128.218:8080/'||p_file_name||'.CSV';

	v_sql = 'CREATE EXTERNAL TABLE '||v_ext_table_name||' (LIKE '||p_table||')
			LOCATION ('''||v_gpfdist||'''
			) ON ALL
			FORMAT ''CSV'' ( delimiter '';'' null '''' escape ''"'' quote ''"'' header )
			ENCODING ''UTF8''';
	execute v_sql;

	execute 'INSERT INTO '||p_table||' SELECT * FROM '||v_ext_table_name;
	execute 'SELECT COUNT(1) FROM '||p_table into v_result;
	
	return v_result;
end;



$$
EXECUTE ON ANY;
