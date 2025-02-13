from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
    config_schema={"s3_key":str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "S3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    key = context.op_config["s3_key"]
    data = context.resources.s3.get_data(key)
    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(ins={"stocks": In(dagster_type=List[Stock])}, 
    out={"Aggregation": Out(dagster_type=Aggregation)},
    description="Process list of stocks from an S3 file and get hightest valued stock",
)
def process_data(stocks:List[Stock]):
    stock = max(stocks, key = lambda x:x.high) 
    return Aggregation(date = stock.date, high = stock.high)


@op(required_resource_keys={"redis"},
    ins={"agg_gv_stock": In(dagster_type=Aggregation)},
    out=Out(Nothing),    
    tags={"kind": "redis"},
    description="Write processed reult from S3 file to Redis",
)
def put_redis_data(context, agg_gv_stock):
    context.resources.redis.put_data(str(agg_gv_stock.date), str(agg_gv_stock.high))


@graph
def week_3_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


@static_partitioned_config(partition_keys=[str(num) for num in range(1, 11)])
def docker_config(partition_key: str):    
    return {
        "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },

    "ops":{"get_s3_data": {"config":{"s3_key": f"prefix/stock_{partition_key}.csv"}}},
    }

local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


# Add your schedule

local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")

#  create a sensor for the docker_week_3_pipeline

@sensor(job=docker_week_3_pipeline, minimum_interval_seconds=30)
def docker_week_3_sensor(context):
    new_s3_keys = get_s3_keys(
        bucket="dagster",  
        prefix="prefix",  
        endpoint_url="http://localstack:4566"
    )
    if not new_s3_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return
        
    for new_s3_key in new_s3_keys:
        yield RunRequest(
            run_key=new_s3_key,
            run_config={
                "resources": {
                    "s3": {"config": {"bucket": "dagster", "access_key": "test", "secret_key": "test", "endpoint_url": "http://localstack:4566",}},
                    "redis": {"config": {"host": "redis", "port": 6379,}},
                },
                "ops": {"get_s3_data": {"config": {"s3_key": new_s3_key}}},
            }
        )
