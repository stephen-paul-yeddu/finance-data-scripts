with phonepe_refund as
(SELECT *except(source,state),
case when source is null and transaction_amount = 99 then 'GoodScore'
when source is null and transaction_amount != 99 then 'BBPS'
else source end as product,
-- case when source is null then True else False end as product_fallback,
coalesce(state,'Karnataka') as state
from
(select a.*,
case when b.document_id is not null then 'GoodScore'
when c.document_id is not null then 'BBPS'
when d.document_id is not null then 'BBPS'
else null end as source,
uf.state_v1 as state


from `finance_dataset.phonepe_refund` a
left join `analytics_dataset.subscriptions_transactions_flat` b on a.forward_merchant_transaction_id = b.document_id and date(b.created_at) >= '2019-01-01'
left join `analytics_dataset.bbps_transactions_flat` c on a.forward_transaction_reference_id = c.transaction_id and date(c.created_at) >= '2019-01-01'
left join `analytics_dataset.loan_subscription_autopay_flat` d on a.forward_merchant_transaction_id = d.document_id
                                                              and date(d.createdat) >= '2019-01-01'
left join `adhoc_analytics.users_flat_state` uf on coalesce(b.userid,c.userid,d.userId) = uf.user_id
)
where date_trunc(date(transaction_date),month) = date_input
and transaction_status = 'COMPLETED'),


phonepe_revenue as 
(    SELECT *except(source,state),
    case when source is null and total_transaction_amount = 99 then 'GoodScore'
    when source is null and total_transaction_amount != 99 then 'BBPS'
    else source end as product,
    case when source is null then True else False end as product_fallback,
    coalesce(state,'Karnataka') as state,
    case when state is null then True else False end as state_fallback,

    FROM
    (select a.*,
    case when b.document_id is not null then 'GoodScore'
    when c.document_id is not null then 'BBPS'
    when d.document_id is not null then 'BBPS'
    else null end as source,
    uf.state_v1 as state

    from
    `finance_dataset.phonepe_revenue` a
    left join `analytics_dataset.subscriptions_transactions_flat` b on split(a.merchant_reference_id,'-')[0] = b.document_id and date(b.created_at) >= '2019-01-01'
    left join `analytics_dataset.bbps_transactions_flat` c on a.phonepe_reference_id = c.transaction_id and date(c.created_at) >= '2019-01-01'
    left join `analytics_dataset.loan_subscription_autopay_flat` d on split(a.merchant_reference_id,'-')[0] = d.document_id
    left join `adhoc_analytics.users_flat_state` uf on coalesce(b.userid,c.userid,d.userId) = uf.user_id
    )
    where date_trunc(date(transaction_date),month) = date_input
    and transaction_status = 'COMPLETED')


SELECT coalesce(a.state,b.state) as state,coalesce(a.product,b.product) as product,coalesce(a.merchant_id,b.merchant_id) as merchant_id,
a.paid_amount,
b.refund_amount
FROM
(SELECT state,product,merchant_id,sum(total_transaction_amount) as paid_amount from
phonepe_revenue
group by 1,2,3) a
FULL OUTER JOIN
(SELECT state,product,merchant_id,sum(transaction_amount) as refund_amount from
phonepe_refund
group by 1,2,3) b on a.state = b.state and a.product = b.product and a.merchant_id = b.merchant_id

ORDER BY 1,2,3
