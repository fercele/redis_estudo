from redis.asyncio import Redis
import os
from dotenv import load_dotenv, find_dotenv
from util import normalize_redis_hash
import asyncio

load_dotenv(find_dotenv())

def connect() -> Redis:
    return Redis(
        host=os.getenv("REDIS_HOST"), 
        port=os.getenv("REDIS_PORT"), 
        password=os.getenv("REDIS_PW"),
        db=0)


async def main():
    client = connect()

    number_of_added_fields = await client.hset("car", 
        mapping={
            "color": "red",
            "year": "1950",
            "engine": str({"cylinders":"8"}),
            "owner": ""
        }
    )

    print(f"added {number_of_added_fields} fields to car")

    car = await client.hgetall("car")
    print(car)
    car = normalize_redis_hash(car)
    print(car)

    print("Testing with nonexistent objetcs")
    print("1: ")
    nonexistent_car = await client.hgetall("nonexistent_car")
    print(nonexistent_car)

    print("2: ")
    nonexistent_car = await client.get("nonexistent_car")	
    print(nonexistent_car)

    print("Object using GET")
    try:
        car_as_object = await client.get("car")
        print(car_as_object)
    except Exception as e:
        print("does not work")
        print(e)

    result = await client.delete("str_var")
    print(f"Delete result is {result}")

    print("Str var using GET")
    str_var = await client.get("str_var")
    print("not found", str_var)

    await client.set("str_var", "new value")
    str_var = await client.get("str_var")
    print(str_var)


    #checar se um hash nao existe
    result:dict = await client.hgetall("nonexistent_hash")
    if len(result.keys()) > 0:
        print("hash exists")
    else:
        print("hash does not exist")
        
if __name__ == "__main__":
    asyncio.run(main())