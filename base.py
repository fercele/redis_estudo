import redis
from dotenv import load_dotenv, find_dotenv
import os
from redis import Redis

load_dotenv(find_dotenv())

def connect() -> Redis:
    return Redis(
        host=os.getenv("REDIS_HOST"), 
        port=os.getenv("REDIS_PORT"), 
        password=os.getenv("REDIS_PW"),
        db=0)

if __name__ == "__main__":
    r:Redis = connect()
    r.set("message", "Hello from Python")
    print(r.get("message"))

    r.execute_command("SET", "other_message", "Hello again from Python")
    print(r.execute_command("GET other_message"))

    #Padrão de chaves: <conceito>:<subconceito>#<identificador-unico>
    #Exemplo: user:chat#1
    r.set("goingaway#1", "this key will disappear", ex=10)

