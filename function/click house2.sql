CREATE TABLE std11_3.ch_plan_fact
(
    `region` String,
    `matdirec` Int32,
    `distr_chan` Int32,
    `plan` Int32,
    `fact` Int32,
    `percent` Decimal(10,2),
    `most_sold` Int32
)
ENGINE = ReplicatedMergeTree('/click/std11_3/ch_plan_fact/{shard}','{replica}')
ORDER BY region
SETTINGS index_granularity = 8192;


CREATE TABLE std11_3.ch_plan_fact_distr
(
    `region` String,
    `matdirec` Int32,
    `distr_chan` Int32,
    `plan` Int32,
    `fact` Int32,
    `percent` Decimal(10,2),
    `most_sold` Int32
)
ENGINE = Distributed('default_cluster','std11_3','ch_plan_fact',matdirec);

INSERT INTO std11_3.ch_plan_fact_distr
SELECT * FROM std11_3.ch_plan_fact_ext;
