-- Silver core: one row per payment that is not deleted (smallest grain).
select payment_id, order_id, amount, status
from {{ source('silver','payments') }}
where not is_deleted
