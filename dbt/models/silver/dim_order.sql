-- Silver core: one row per order that is not deleted.
-- The order date (Asia/Seoul) is fixed here so every Gold table uses the same date.
select order_id, ordered_at,
       to_date(from_utc_timestamp(ordered_at,'Asia/Seoul')) order_date,
       status
from {{ source('silver','orders') }}
where not is_deleted
