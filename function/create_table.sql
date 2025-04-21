CREATE TABLE std11_3.sales (
	check_nm varchar(255),
	check_pos varchar(255),
	material varchar(255),
	region varchar(255),
	distr_chan varchar(255),
	quantity int4,
	"date" date)
	WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=1)
	DISTRIBUTED BY (date);


CREATE TABLE std11_3.plan (
	"date" date,
	region varchar(20),
	matdirec varchar(20),
	distr_chan varchar(100),
	quantity int4 
    )
    WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=1)
	DISTRIBUTED BY (date);


CREATE TABLE std11_3.price (
	material int NULL,
	region varchar NULL,
	distr_chan varchar NULL,
	price numeric(18, 2) NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zlib,
	compresslevel=5
)
DISTRIBUTED REPLICATED;


CREATE TABLE std11_3.product (
	material int4 NULL,
	asgrp int4 NULL,
	brand int4 NULL,
	matcateg varchar(2) NULL,
	matdirec int4 NULL,
	txt text NULL
)
WITH (
	appendonly=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED REPLICATED;


CREATE TABLE std11_3.channel (
    distr_chan VARCHAR(255) ,
    txtsh VARCHAR(255)
)WITH (
	appendonly=true,
	orientation=column,
	compresstype=zlib,
	compresslevel=5
)DISTRIBUTED REPLICATED;

CREATE TABLE std11_3.region (
    region VARCHAR(255),
    txt VARCHAR(255)
)WITH (
	appendonly=true,
	orientation=column,
	compresstype=zlib,
	compresslevel=5
) DISTRIBUTED REPLICATED;

CREATE TABLE std11_3.logs (
	log_id int8 NOT NULL,
	log_timestamp timestamp DEFAULT now() NOT NULL,
	log_type text NOT NULL,
	log_msg text NOT NULL,
	log_location text NULL,
	is_error bool NULL,
	log_user text DEFAULT '"current_user"()' NULL,
	CONSTRAINT pk_log_id PRIMARY KEY (log_id)
)
DISTRIBUTED BY (log_id);

CREATE SEQUENCE std11_3.log_id_seq
	MINVALUE 0
	NO MAXVALUE
	START 0
	NO CYCLE;