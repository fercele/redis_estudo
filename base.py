import redis
from dotenv import load_dotenv, find_dotenv
import os
from redis import Redis
from util import normalize_redis_hash

load_dotenv(find_dotenv())

def connect() -> Redis:
    return Redis(
        host=os.getenv("REDIS_HOST"), 
        port=os.getenv("REDIS_PORT"), 
        password=os.getenv("REDIS_PW"),
        db=0)

def main():
    r:Redis = connect()
    # r.set("message", "Hello from Python")
    # print(r.get("message"))

    # r.execute_command("SET", "other_message", "Hello again from Python")
    # print(r.execute_command("GET other_message"))

    # #Padrão de chaves: <conceito>:<subconceito>#<identificador-unico>
    # #Exemplo: user:chat#1
    # r.set("goingaway#1", "this key will disappear", ex=10)

    # result = r.execute_command("KEYS", "*")
    # print("KEYS: ", result)

    # print("execute command HGETALL")
    # print("-" * 30)
    # result = r.execute_command("HGETALL", "documento")
    # print(result)
    # print(result[ b'metadata#source'])
    # print("-" * 30)

    # normalized_result = normalize_redis_hash(result)
    # print(normalized_result)
    # print(normalized_result["metadata#source"])
    # print("-" * 30)

    print("api hgetall")
    print("-" * 30)
    result = normalize_redis_hash(r.hgetall("documento"))
    print(result)
    print("-" * 30)

    if r.hexists("documento", "metadata#source"):
        print("metadata#source exists")
    print("-" * 30)
    print("criando documento")
    r.hset("documento", mapping={
        "content": "A doc from python",
        "metadata#source": "pythondoc.pdf",
        "metadata#size": "1024"
    })

    print("Criado - lendo")
    print(normalize_redis_hash(r.hgetall("documento")))
    print("-" * 30)

if __name__ == "__main__":
    main()