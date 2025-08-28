select
date_trunc(date(report_fetch_timestamp),month) as month,
report_source,
count(distinct document_id) as times_fetched,
count(distinct case when nullif(parentReportId,'') is null then document_id end) as fresh_pull_no_parent_id,
count(distinct case when nullif(parentReportId,'') is not null then document_id end) as repeat_pull_with_parent_id,

from

(select *,row_number() over(partition by userid,report_source order by report_fetch_timestamp) as rk
from
(select distinct
document_id,
userid,
report_fetch_timestamp,
credit_score,
parentReportId,
case when lower(report_source) like '%crif%' then 'Crif' else 'Experian' end as report_source
from
analytics_dataset.accounts_final_flat
where date(report_fetch_timestamp) >= '2019-01-01'

union all

select distinct
document_id,userid,
report_fetch_timestamp,credit_score,
parentReportId,
case when lower(report_source) like '%crif%' then 'Crif' else 'Experian' end as report_source
from
analytics_dataset.accounts_archive_flat
where date(report_fetch_timestamp) >= '2019-01-01'
)
)
group by 1,2
order by 1,2