SELECT *except(source,state),
case when source is null and amount = 99 then 'GoodScore'
when source is null and amount != 99 then 'BBPS'
else source end as product,
case when source is null then True else False end as product_fallback,
coalesce(state,'Karnataka') as state,
case when state is null then True else False end as state_fallback

FROM

(select
case when (notes = 'nan' or notes = '{{}}') then false else true end as identity_available_to_map,
a.*,safe.parse_json(notes) as notes_json,
case when b.document_id is not null then 'GoodScore'
when c.document_id is not null then 'BBPS'
when d.document_id is not null then 'BBPS'
else null end as source,
uf.state_v1 as state

from finance_dataset.razorpay a 
left join `analytics_dataset.subscriptions_transactions_flat` b on coalesce(json_extract_scalar(safe.parse_json(notes),'$.transactionId'),
json_extract_scalar(safe.parse_json(notes),'$.transactionDocIdRecurring')) = b.document_id
                                                            and date(b.created_at) >= '2022-01-01'
left join `analytics_dataset.bbps_transactions_flat` c on coalesce(json_extract_scalar(safe.parse_json(notes),'$.transactionId'),
json_extract_scalar(safe.parse_json(notes),'$.transactionDocIdRecurring')) = c.document_id
                                                            and date(c.created_at) >= '2022-01-01'
left join `analytics_dataset.loan_subscription_autopay_flat` d on coalesce(json_extract_scalar(safe.parse_json(notes),'$.transactionId'),
json_extract_scalar(safe.parse_json(notes),'$.transactionDocIdRecurring')) = d.document_id
                                                            and date(d.createdat) >= '2022-01-01'
left join `adhoc_analytics.users_flat_state` uf on coalesce(b.userid,c.userid,d.userId) = uf.user_id

)
where date_trunc(date(created_at),month) = date_input