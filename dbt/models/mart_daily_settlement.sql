select order_date, count(*) order_count, sum(paid) paid,
       sum(refunded) refunded, sum(net_paid) net_paid,
       max(mart_built_at) mart_built_at
from {{ ref('fct_order_settlement') }}
group by order_date
