create table dm.tmp as
with rnk as (
	select 
		*,
		row_number() over (partition by c.client_rk,  c.effective_from_date order by c.client_rk) as rn
	from dm.client c
)
select 
	r.client_rk,
	r.effective_from_date,
	r.effective_to_date,
	r.account_rk,
	r.address_rk,
	r.department_rk,
	r.card_type_code,
	r.client_id,
	r.counterparty_type_cd,
	r.black_list_flag,
	r.client_open_dttm,
	r.bankruptcy_rk
from rnk r
where r.rn = 1;

truncate dm.client;

insert into dm.client
select * from dm.tmp;

drop table dm.tmp;