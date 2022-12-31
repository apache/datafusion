-- SQLBench-H query 4 derived from TPC-H query 4 under the terms of the TPC Fair Use Policy.
-- TPC-H queries are Copyright 1993-2022 Transaction Processing Performance Council.
select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= date '1995-04-01'
	and o_orderdate < date '1995-04-01' + interval '3' month
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority;
