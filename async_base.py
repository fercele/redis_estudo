import redis
from dotenv import load_dotenv, find_dotenv
import os
#Quando usar async a classe Redis deve ser importada do pacote asyncio
#Cuidar pois tem nome idêntico ao pacote padrão síncrono, redis
from redis.asyncio import BlockingConnectionPool, Redis
import asyncio

load_dotenv(find_dotenv())

#Variavel global fazendo o papelo de um pool de conexão de uma aplicação web
REDIS_CONNECTION_POOL = BlockingConnectionPool(
    host=os.getenv("REDIS_HOST"), 
    port=os.getenv("REDIS_PORT"), 
    password=os.getenv("REDIS_PW"),
    decode_responses=True,
    max_connections=50,
    timeout=20)
    
#Classe base para qualquer ação async a ser feita com o redis    
class AbstractAsyncRedisWorker:
    def __init__(self, redis_connection_pool:BlockingConnectionPool):
        print("Construtor principal")
        self.__pool = redis_connection_pool
    
    #Esse método permite que a chamada ao construtor da classe seja feita de forma assincrona
    def __await__(self):
        print("Awaitable")
        return self.init().__await__()
    
    async def init(self):
        print("init")
        self.connection = await Redis(connection_pool=self.__pool)
        return self
    
    #Não é necessário implementar os métodos abaixo, pois o Redis já implementa o release de conexão para o pool
    # async def __aenter__(self):
    #     print("Entrando async context")
    #     return self

    # async def __aexit__(self, *excinfo):
    #     print("Saindo async context e fechando conexão do pool")
    #     client:Redis = self.connection
    #     await client.aclose()

    
class RedisHash(AbstractAsyncRedisWorker):
    def __init__(self, redis_connection_pool:BlockingConnectionPool):
        super().__init__(redis_connection_pool)
    
    async def get(self, key:str, field:str):
        return await self.connection.hget(key, field)
    
    async def set(self, key:str, field:str, value:str):
        return await self.connection.hset(key, field, value)
    
    async def get_all(self, key:str):
        return await self.connection.hgetall(key)
    
    async def exists(self, key:str, field:str):
        return await self.connection.hexists(key, field)
    
    async def delete(self, key:str, field:str):
        return await self.connection.hdel(key, field)
    
    async def set_all(self, key:str, mapping:dict):
        return await self.connection.hset(key, mapping=mapping)
    
    async def delete_all(self, key:str):
        return await self.connection.delete(key)



async def usando_classe():
    redis_worker = await RedisHash(REDIS_CONNECTION_POOL)
    print("Async Test Criando documento")

    print("Deletando existente")
    await redis_worker.delete_all("documentoasync")

    print("Criando")
    await redis_worker.set_all("documentoasync",{
        "content": "A doc from async python",
        "metadata#source": "pythondoc.pdf",
        "metadata#size": "1024"
    })

    print("Criado - lendo")
    print(await redis_worker.get_all("documentoasync"))

async def pipeline_sem_classe():
    client:Redis = await Redis(connection_pool=REDIS_CONNECTION_POOL)
    pipeline = client.pipeline()

    pipeline.delete("documento1")
    pipeline.delete("documento2")

    pipeline.hset("documento1", mapping={
        "content": "first from a pipeline python",
        "metadata#source": "pythondoc1.pdf",
        "metadata#size": "1024"
    })

    pipeline.hset("documento2", mapping={
        "content": "second from a pipeline python",
        "metadata#source": "pythondoc2.pdf",
        "metadata#size": "2048"
    })

    creation_result = await pipeline.execute()
    print("Creation Result", creation_result)

    #Aproveitando a mesma pipeline para outras operacoes
    pipeline.hgetall("documento1")
    pipeline.hgetall("documento2")

    read_result = await pipeline.execute()
    print("Read Result", read_result)

async def main():
    # await usando_classe()
    await pipeline_sem_classe()

if __name__ == "__main__":
    asyncio.run(main())