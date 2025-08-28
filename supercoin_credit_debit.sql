with bbps_coins as 
(select date(created_at,'Asia/Kolkata') as date,sum(coins_granted) as coins_credit,
sum(discount*20) as coins_used

from `analytics_dataset.bbps_transactions_flat`
where date(created_at) >= '2019-01-01'
and bbps_payment_status = 'PAYMENT_SUCCESS'
and payment_status = 'PAYMENT_SUCCESS'
group by 1
),

coin_rewards as 
(select date(created_at,'Asia/Kolkata') as date,sum(reward_coins) as coins_credit
from `analytics_dataset.coin_rewards`
group by 1
)



select date,sum(coins_credit) as coins_credit,sum(coins_used) as coins_used
from
(select * from bbps_coins
union all
select *,null from coin_rewards)
where coalesce(coins_credit,coins_used) is not null
group by 1
order by 1