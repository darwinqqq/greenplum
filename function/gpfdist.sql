CREATE external TABLE std11_3.product_ext (
    material INT ,
    asgrp INT,
    brand INT,
    matcateg VARCHAR,
    matdirec INT,
    txt VARCHAR
)
LOCATION ('gpfdist://192.168.1.47:8080/product.csv')
FORMAT 'CSV' (DELIMITER ';')
ENCODING 'UTF8'
segment reject limit 10 rows;

CREATE external TABLE std11_3.region_ext (
    region VARCHAR,
    txt VARCHAR
) 
LOCATION ('gpfdist://192.168.1.47:8080/region.csv')
FORMAT 'CSV' (DELIMITER ';')
ENCODING 'UTF8'; 

CREATE external TABLE std11_3.channel_ext (
    distr_chan INT ,
    txtsh VARCHAR(255)
)
LOCATION ('gpfdist://192.168.1.47:8080/channel.csv')
FORMAT 'CSV' (DELIMITER ',');


CREATE external TABLE std11_3.price_ext (
    material INT,
    region VARCHAR,
    distr_chan INT,
    price INT,
    PRIMARY KEY (material, region, distr_chan),
    FOREIGN KEY (material) REFERENCES std11_3.product(material),
    FOREIGN KEY (region) REFERENCES std11_3.region(region),
    FOREIGN KEY (distr_chan) REFERENCES std11_3.channel(distr_chan)
) 
LOCATION ('gpfdist://192.168.1.47:8080/price.csv')
FORMAT 'CSV' (DELIMITER ',')
DISTRIBUTED BY (material);



