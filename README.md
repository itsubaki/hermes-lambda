# hermes-lambda
lambda function of hermes

## deploy
```shell script
AWS_PROFILE=hoghoge S3Bucket=foobar make deploy
```

## BigQuery Example

```sql
select
  description,
  sum(unblended_cost) as unblended_cost
from
  `hermes_lambda.1d_account_cost`
 where
  timestamp =
  (
  select
    timestamp
   from
    `hermes_lambda.1d_account_cost`
   where
    date = "2020-04-12"
    order by timestamp desc
    limit 1
  )
  group by description
  order by unblended_cost desc
```

```sql
select
  description,
  round(sum(covering_cost_percentage), 4) as covering_cost_percentage
from
  `hermes_lambda.1m_utilization`
 where
  region = "ap-northeast-1" and
  timestamp =
  (
  select
    timestamp
   from
    `hermes_lambda.1m_utilization`
   where
    date = "2020-03-01"
    order by timestamp desc
    limit 1
  )
  group by description
  order by covering_cost_percentage desc
```