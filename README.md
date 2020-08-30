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
  date = "2020-04-15" and
  timestamp = (select max(timestamp) from `hermes_lambda.1d_account_cost` where date = "2020-04-15" )
  group by description
  order by unblended_cost desc
```

```sql
select
  description,
  round(sum(ondemand_conversion_cost_percentage), 4) as ondemand_conversion_cost_percentage
from
  `hermes_lambda.1d_utilization`
 where
  region = "ap-northeast-1" and
  date = "2020-03-01" and
  timestamp = (select max(timestamp) from `hermes_lambda.1d_utilization` where date = "2020-03-01" )
  group by description
  order by ondemand_conversion_cost_percentage desc
```
