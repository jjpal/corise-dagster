from typing import List

from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(     
    config_schema={"s3_key":str},
    required_resource_keys={"s3"},
    group_name="corise", 
    op_tags={"kind": "s3"},   
    description="Get a list of stocks from an S3"
)
def get_s3_data(context):
    # Use your op logic from week 3
    output = list()
    key = context.op_config["s3_key"]
    data = context.resources.s3.get_data(key)
    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@asset(
    group_name="corise",
    description="Process list of stocks from an S3 file and get hightest valued stock"
)
def process_data(get_s3_data):
    # Use your op logic from week 3 (you will need to make a slight change)#TODO
    stock = max(get_s3_data, key = lambda x:x.high) 
    return Aggregation(date = stock.date, high = stock.high)


@asset(
    required_resource_keys={"redis"},
    group_name="corise",
    op_tags={"kind": "redis"},
    description="Write processed reult from S3 file to Redis",
)
def put_redis_data(context, process_data):
    # Use your op logic from week 3 (you will need to make a slight change)#TODO
    context.resources.redis.put_data(str(process_data.date), str(process_data.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions = [get_s3_data, process_data, put_redis_data],
    resource_defs = {"s3": s3_resource,"redis": redis_resource},
    resource_config_by_key={       
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
    }
)