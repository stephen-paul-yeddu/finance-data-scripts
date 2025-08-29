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
where date_trunc(date(transaction_date),month) >= '2025-02-01'
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
    uf.state_v1 as state,
    coalesce(c.ref_id,e.ref_id) as bbps_ref_id,
    coalesce(c.payment_status,e.payment_status) as payment_status,
    coalesce(c.bbps_payment_status,e.bbps_payment_status) as bbps_payment_status,
    coalesce(c.refund_status,e.refund_status) as refund_status

    from
    `finance_dataset.phonepe_revenue` a
    left join `analytics_dataset.subscriptions_transactions_flat` b on split(a.merchant_reference_id,'-')[0] = b.document_id and date(b.created_at) >= '2019-01-01'
    left join `analytics_dataset.bbps_transactions_flat` c on a.phonepe_reference_id = c.transaction_id and date(c.created_at) >= '2019-01-01'
    left join `analytics_dataset.loan_subscription_autopay_flat` d on split(a.merchant_reference_id,'-')[0] = d.document_id
    left join `analytics_dataset.bbps_transactions_flat` e on d.document_id = e.autopayId and e.created_at >= '2019-01-01'
    left join `adhoc_analytics.users_flat_state` uf on coalesce(b.userid,c.userid,d.userId) = uf.user_id
    )
    where date_trunc(date(transaction_date),month) >= '2025-02-01'
    and transaction_status = 'COMPLETED'),


razorpay_data as

(
SELECT
DISTINCT
entity_id,
type,
transaction_id,
bbps_ref_id,
payment_status,
bbps_payment_status,
refund_status,
credit,
amount,
debit,
date(created_at) as created_at,
case when source is null and amount = 99 then 'GoodScore'
when source is null and amount != 99 then 'BBPS'
else source end as product,
case when source is null then True else False end as product_fallback,
coalesce(state,'Karnataka') as state,
case when state is null then True else False end as state_fallback

FROM

(select
case when (notes = 'nan' or notes = '{{}}') then false else true end as identity_available_to_map,
a.*,
-- safe.parse_json(notes) as notes_json,
case when b.document_id is not null then 'GoodScore'
when c.document_id is not null then 'BBPS'
when d.document_id is not null then 'BBPS'
else null end as source,
coalesce(json_extract_scalar(safe.parse_json(notes),'$.transactionId'),
json_extract_scalar(safe.parse_json(notes),'$.transactionDocIdRecurring')) as transaction_id,
-- coalesce(b.document_id,c.document_id,d.document_id) as document_id,
uf.state_v1 as state,
coalesce(c.ref_id,e.ref_id) as bbps_ref_id,
     coalesce(c.payment_status,e.payment_status) as payment_status,
    coalesce(c.bbps_payment_status,e.bbps_payment_status) as bbps_payment_status,
    coalesce(c.refund_status,e.refund_status) as refund_status

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
left join `analytics_dataset.bbps_transactions_flat` e on d.document_id = e.autopayId and e.created_at >= '2019-01-01'
left join `adhoc_analytics.users_flat_state` uf on coalesce(b.userid,c.userid,d.userId) = uf.user_id

)
where date_trunc(date(created_at),month) >= '2025-02-01'
),


razorpay_final_data as
(select a.created_at,a.entity_id,a.type,a.transaction_id,a.bbps_ref_id,
a.payment_status,
a.bbps_payment_status,
a.amount,
case when b.transaction_id is not null or a.refund_status = 'PAYMENT_SUCCESS' then True else False end as refunded,
b.amount as refund_amount,
c.amount as payu_amount_paid,
d.amount as payu_amount_refund,

from razorpay_data a
left join razorpay_data b on a.transaction_id = b.transaction_id and b.type = 'refund'
left join `finance_dataset.payu` c on a.bbps_ref_id = c.transaction_id and c.activitytype = 'Bill Payment'
left join `finance_dataset.payu` d on a.bbps_ref_id = d.transaction_id and d.activitytype = 'Refund'
where a.type = 'payment'
and a.product = 'BBPS'
and a.product_fallback is false
),

phonepe_final_data as

(SELECT
DISTINCT
a.transaction_date,
a.merchant_id,a.merchant_order_id,a.merchant_reference_id,a.phonepe_reference_id,a.phonepe_transaction_reference_id,
a.total_transaction_amount,
case when b.forward_merchant_transaction_id is not null or a.refund_status = 'PAYMENT_SUCCESS' then True else False end as refunded,
b.total_refund_amount,
a.bbps_ref_id,
payment_status,
bbps_payment_status,
c.amount as payu_amount_paid,
d.amount as payu_amount_refund

 from phonepe_revenue a
left join phonepe_refund b on a.merchant_reference_id = b.forward_merchant_transaction_id
left join `finance_dataset.payu` c on a.bbps_ref_id = c.transaction_id and c.activitytype = 'Bill Payment'
left join `finance_dataset.payu` d on a.bbps_ref_id = d.transaction_id and d.activitytype = 'Refund'
where a.product = 'BBPS'
and a.product_fallback = False
)


select * from
(select DISTINCT a.*,b.paid_amount as source_amount,
b.bill_amount,
d.amount as payu_refund_amount,
from `finance_dataset.payu` a
left join `analytics_dataset.bbps_transactions_flat` b on a.transaction_id = b.ref_id and date(b.created_at) >= '2019-01-01' and payment_status = 'PAYMENT_SUCCESS'
left join `finance_dataset.payu` d on a.transaction_id = d.transaction_id and d.activitytype = 'Refund'
where a.activitytype = 'Bill Payment'
and a.date >= '2025-02-01'
)
where source_amount is null
and bill_amount is null
and payu_refund_amount is null
and date_trunc(date(date),month) >= '2025-02-01'
