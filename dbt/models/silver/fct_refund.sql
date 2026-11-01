-- Silver core: one row per refund that is not deleted (smallest grain).
-- The order key comes from the refund's payment; payment_status keeps the payment's state.
select r.refund_id, r.payment_id, p.order_id, r.amount, r.status,
       p.status payment_status
from {{ source('silver','refunds') }} r
left join {{ ref('fct_payment') }} p on r.payment_id=p.payment_id
where not r.is_deleted
