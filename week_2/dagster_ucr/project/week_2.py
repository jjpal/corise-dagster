from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


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
    description="Process list of stocks from an S3 file and get hightest valued stock"
)
def process_data(stocks):
    # Use your op from week 1
    stock = max(stocks, key = lambda x:x.high) 
    return Aggregation(date = stock.date, high = stock.high)


@op(ins={"agg_gv_stock": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
    description="Write processed reult from S3 file to Redis",
)
def put_redis_data(context, agg_gv_stock):
    context.resources.redis.put_data(
        str(agg_gv_stock.date), str(agg_gv_stock.high)
    )

@graph
def week_2_pipeline():
    # Use your graph from week 1
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
