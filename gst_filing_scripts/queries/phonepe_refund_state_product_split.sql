SELECT *except(source,state),
case when source is null and transaction_amount = 99 then 'GoodScore'
when source is null and transaction_amount != 99 then 'BBPS'
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

from `finance_dataset.phonepe_refund` a
left join `analytics_dataset.subscriptions_transactions_flat` b on a.forward_merchant_transaction_id = b.document_id and date(b.created_at) >= '2019-01-01'
left join `analytics_dataset.bbps_transactions_flat` c on a.forward_transaction_reference_id = c.transaction_id and date(c.created_at) >= '2019-01-01'
left join `analytics_dataset.loan_subscription_autopay_flat` d on a.forward_merchant_transaction_id = d.document_id
                                                              and date(d.createdat) >= '2019-01-01'
left join `adhoc_analytics.users_flat_state` uf on coalesce(b.userid,c.userid,d.userId) = uf.user_id
)
where date_trunc(date(transaction_date),month) = date_input
