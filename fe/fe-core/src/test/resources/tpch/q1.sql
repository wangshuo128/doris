select
    l_returnflag, --返回标志
    l_linestatus,
    sum(l_quantity) as sum_qty, --总的数量
    sum(l_extendedprice) as sum_base_price, --聚集函数操作
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order --每个分组所包含的行数
from
    lineitem
where
    l_shipdate <= date'1998-12-01' - interval '90' day --时间段是随机生成的
group by --分组操作
    l_returnflag,
    l_linestatus
order by --排序操作
    l_returnflag,
    l_linestatus