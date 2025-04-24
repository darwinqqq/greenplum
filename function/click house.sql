CREATE DATABASE std11_3;

CREATE TABLE std11_3.ch_plan_fact_ext
(
    `region` String,
    `matdirec` Int32,
    `distr_chan` Int32,
    `plan` Int32,
    `fact` Int32,
    `percent` Decimal(10, 2),
    `most_sold` Int32
)
ENGINE = PostgreSQL('192.168.214.203:5432',
 'adb',
 'plan_fact_202104',
 'std11_3',
 'B0Yc4aR562mKWT',
 'std11_3'); 


CREATE DICTIONARY std11_3.ch_price_dict
(
    `material` Int32 ,
	`region` varchar ,
	`distr_chan` varchar ,
	`price` numeric(18, 2) 
)
PRIMARY KEY material
SOURCE(POSTGRESQL(PORT 5432 HOST '192.168.214.203' USER 'std11_3' PASSWORD 'B0Yc4aR562mKWT' DB 'adb' TABLE 'std11_3.price'))
LIFETIME(MIN 3000 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());


CREATE DICTIONARY std11_3.ch_chanel_dict
(
    `distr_chan` Int32,
    `txtsh` String
)
PRIMARY KEY distr_chan
SOURCE(POSTGRESQL(PORT 5432 HOST '192.168.214.203' USER 'std11_3' PASSWORD 'B0Yc4aR562mKWT' DB 'adb' TABLE 'std11_3.chanel'))
LIFETIME(MIN 3000 MAX 3600)
LAYOUT(FLAT());


CREATE DICTIONARY std11_3.ch_product_dict
(
    `material` Int32,
    `asgrp` Int32,
    `brand` Int32,
    `matcateg` String,
    `matdirec` Int32,
    `txt` String
)
PRIMARY KEY material
SOURCE(POSTGRESQL(PORT 5432 HOST '192.168.214.203' USER 'std11_3' PASSWORD 'B0Yc4aR562mKWT' DB 'adb' TABLE 'std11_3.product'))
LIFETIME(MIN 3000 MAX 3600)
LAYOUT(HASHED());

CREATE DICTIONARY std11_3.ch_region_dict
(
    `region` String,
    `txt` String
)
PRIMARY KEY region
SOURCE(POSTGRESQL(PORT 5432 HOST '192.168.214.203' USER 'std11_3' PASSWORD 'B0Yc4aR562mKWT' DB 'adb' TABLE 'std11_3.region'))
LIFETIME(MIN 3000 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED());


