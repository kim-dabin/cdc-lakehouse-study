select * from {{ ref('mart_order_settlement') }}
where net_paid != paid-refunded or paid < 0 or refunded < 0
