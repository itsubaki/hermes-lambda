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