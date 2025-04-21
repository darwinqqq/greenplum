CREATE EXTERNAL TABLE std11_3.plan_ext (
    "date" date ,
	region varchar(20) ,
	matdirec varchar(20) ,
	quantity int4 ,
	distr_chan varchar(100)
)
LOCATION ( 'pxf://gp.plan?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';





CREATE EXTERNAL TABLE std11_3.sales_ext (
    "date" date ,
	region varchar(20) ,
	material varchar(20),
	distr_chan varchar(100) ,
	quantity int4 ,
	check_nm varchar(100) ,
	check_pos varchar(100)
)
location ('pxf://gp.sales?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern&PARTITION_BY=date:date&RANGE=2021-01-02:2021-07-26&INTERVAL=1:month'
) 
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' );

