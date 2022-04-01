select
    l_returnflag,
    l_linestatus,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax
from
    lineitem
order by
    l_extendedprice,
    l_discount;