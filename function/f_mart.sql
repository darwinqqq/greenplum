-- DROP FUNCTION std11_3.f_mart(varchar);

CREATE OR REPLACE FUNCTION std11_3.f_mart(p_month varchar)
	RETURNS int4
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
declare 
v_table_name text;
v_sql text;
v_return int;

begin

	perform std11_3.f_load_write_log(p_log_type := 'INFO',
									p_log_message := 'Start f_mart',
									p_location := 'Report Calculation');

v_sql =	'drop table if exists std11_3.plan_fact_'||p_month||' cascade;
	create table std11_3.plan_fact_'||p_month||'
	(
	region varchar(4),
	matdirec int2,
	distr_chan int2,
	plan int4,
	fact int4,
	"percent" numeric,
	most_sold int4
	)
	with (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
	)';

execute v_sql;
	
v_sql =	'insert into std11_3.plan_fact_'||p_month||'
select distinct p.region, 
				p.matdirec::int2, 
				p.distr_chan::int2, 
				p.quantity as plan, 
				s.sm as fact, 
				round(s.sm / p.quantity::numeric * 100, 2) as percent,
				ms.most_sold
from std11_3.plan p
left join (select date, region,  
				distr_chan, 
				sum(quantity) as sm
			from std11_3.sales
			group by region, distr_chan, date) s 
on p.region = s.region and p.distr_chan = s.distr_chan and p.date = s.date
left join (select region, material as most_sold
			from (select region, material::int4, sum(quantity), rank() over(partition by region order by sum(quantity) desc) as rnk
				from sales
				group by region, material
				order by 1, 3 desc) t1
				where rnk = 1) ms
on p.region = ms.region

where p.date >= to_date('''||p_month||''', ''YYYYMM'') and p.date < (to_date('''||p_month||''', ''YYYYMM'') + ''1 month''::interval)';

execute v_sql;

get diagnostics v_return=row_count;

perform std11_3.f_load_write_log(p_log_type := 'INFO',
									p_log_message := v_return||' rows inserted',
									p_location := 'Report Calculation');

perform std11_3.f_load_write_log(p_log_type := 'INFO',
									p_log_message := 'End f_mart',
									p_location := 'Report Calculation');

v_sql = 'CREATE OR REPLACE VIEW std11_3.v_plan_fact
			AS SELECT mart.region,
			    region.txt AS region_txt,
			    mart.matdirec,
			    chanel.txtsh AS chanel_txt,
			    mart.percent,
			    mart.most_sold,
			    product.brand,
			    product.txt AS material_txt,
			    price.price
			   FROM plan_fact_'||p_month||' mart
			     LEFT JOIN region ON mart.region::VARCHAR = region.region 
				 LEFT JOIN chanel ON mart.distr_chan::VARCHAR = chanel.distr_chan
			     LEFT JOIN product ON mart.most_sold = product.material
			     LEFT JOIN price ON mart.most_sold = price.material;';

execute v_sql;

return v_return;
end;


$$
EXECUTE ON ANY;
