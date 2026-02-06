"""
This server provides a token bucket rate limiter backed by Redis.

Buckets must be declared by environment variable prior to starting the server.
Bucket names are derived from the variable name by stripping out the prefix (default: BUCKET_).
e.g. variable `BUCKET_llm-openrouter` defines the bucket `llm-openrouter`.

The value contains a ':' separated list of rates.
Each rate uses the format "{requests: int}/{time: int}{time_unit: s|m|h|d|w}",
where the time units correspond to seconds, minutes, hours, days and weeks, respectively.
Rates must be ordered according to https://pyratelimiter.readthedocs.io/en/latest/#defining-rate-limits-and-buckets.

Example:
    BUCKET_foo=2/5s:15/m:100/4h
    BUCKET_bar=1/s:100/10m:1000/d

Bucket "foo" has limits of 2 every 5 seconds, 15 per minute and 100 every 4 hours.
Bucket "bar" has limits of 1 per second, 100 every 10 minutes and 1000 per day.
"""

import os
import argparse
from typing import Annotated

from fastmcp import FastMCP
from redis import Redis
from pyrate_limiter import Duration, Rate, Limiter, RedisBucket

REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = os.environ.get("REDIS_PORT", 6379)

redis_conn = Redis(host=REDIS_HOST, port=REDIS_PORT)
mcp = FastMCP("rerank-mcp", instructions=__doc__)

BUCKETS = dict()
LIMITERS = dict()


def init_buckets():
    parser = argparse.ArgumentParser(
        prog="myprogram",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--bucket-prefix",
        help="Environment variable prefix for buckets",
        default="BUCKET_",
    )
    args = parser.parse_args()

    for key, value in os.environ.items():
        if key.startswith(args.bucket_prefix):
            bucket_name = key.removeprefix(args.bucket_prefix)
            limits = value.split(",")
            rates = []

            for limit in limits:
                reqs, interval = limit.split("/")
                count = interval[:-1]
                count = int(count) if count else 1
                unit = interval[-1]

                match unit:
                    case "c":
                        rates.append(Rate(int(reqs), count * 10))
                    case "s":
                        rates.append(Rate(int(reqs), count * Duration.SECOND))
                    case "m":
                        rates.append(Rate(int(reqs), count * Duration.MINUTE))
                    case "h":
                        rates.append(Rate(int(reqs), count * Duration.HOUR))
                    case "d":
                        rates.append(Rate(int(reqs), count * Duration.DAY))
                    case "w":
                        rates.append(Rate(int(reqs), count * Duration.WEEK))

            bucket = RedisBucket.init(rates, redis_conn, bucket_name)
            LIMITERS[bucket_name] = Limiter(bucket)


def init_tools():
    for key in LIMITERS.keys():

        def inner(
            blocking: Annotated[
                bool, "Wait until permits available before returning"
            ] = True,
            item: Annotated[str, "Name of the item to store in the bucket"] = "",
        ) -> bool:
            return LIMITERS[key].try_acquire(item, blocking=blocking)

        mcp.tool(
            inner,
            name=f"limit-{key}",
            description=f"""Acquire a permit for the bucket {key}""",
        )


@mcp.tool(name="rate-limit")
def rate_limit(
    bucket: Annotated[str, "Name of a bucket defined in server configuration"],
    blocking: Annotated[bool, "Wait until permits available before returning"] = True,
    item: Annotated[str, "Name of the item to store in the bucket"] = "",
) -> bool:
    """Wait until a permit for `bucket` is available"""
    return LIMITERS[bucket].try_acquire(item, blocking=blocking)


def main():
    init_buckets()
    init_tools()

    mcp.run()


if __name__ == "__main__":
    import asyncio
    from pprint import pp

    init_buckets()
    init_tools()
    tools = asyncio.run(mcp.get_tools())
    pp(tools)

    # for i in range(10):
    #     LIMITERS["foobar"].try_acquire("hello")
    #     pp("Acquired permit")
