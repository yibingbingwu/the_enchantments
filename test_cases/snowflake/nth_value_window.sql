select distinct
    user_id
    , (nth_value(created_at, 2) from last over (partition by user_id order by created_at))
        as second_last_check_deposit_date
from mysql_db.chime_prod.user_check_deposits