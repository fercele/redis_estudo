from pydantic import BaseModel, ValidationError
from datetime import datetime
from enum import IntEnum

from util import normalize_redis_hash
from async_base import RedisHash, REDIS_CONNECTION_POOL

import asyncio


class MsgRoleEnum(IntEnum):
    System = 1
    Agent = 2
    User = 3

    def __str__(self):
        return self.name
    
    def __repr__(self):
        return self.name


class Message(BaseModel):
    id: int
    role: MsgRoleEnum
    content: str
    date: datetime


def messageKey(msg_id: int) -> str:
    return f"message#{msg_id}"


def serialize_message(msg: Message) -> dict:
    result = msg.model_dump(exclude=["id"])
    msg_date:datetime = result["date"]
    
    #Para produção, usar conforme abaixo
    #result["date"] = msg_date.timestamp()

    #Para prototipação e testes, usar a data como string
    result["date"] = msg_date.isoformat()
    return result


def deserialize_message(id:int, msg_hash: dict) -> Message:
    #msg_hash = normalize_redis_hash(msg_hash)
    
    #Para produção, usar conforme abaixo
    #msg_hash["date"] = datetime.fromtimestamp(float(msg_hash["date"]))

    #Para prototipação e testes, usar a data como string
    msg_hash["date"] = datetime.fromisoformat(msg_hash["date"])
    
    #get the correct enum value from the name
    msg_hash["role"] = MsgRoleEnum[msg_hash["role"]]

    msg_hash["id"] = id

    return Message(**msg_hash)


async def save_message(msg: Message):
    redis_client = await RedisHash(REDIS_CONNECTION_POOL)

    result = await redis_client.set_all(messageKey(msg.id), serialize_message(msg))

    return result


async def retrieve_message(msg_id: int) -> Message:
    redis_client = await RedisHash(REDIS_CONNECTION_POOL)
    msg_hash = await redis_client.get_all(messageKey(msg_id))
    
    try:
        return deserialize_message(msg_id, msg_hash)
    except ValidationError as valid_error:
        print("Erro - objeto do redis invalido")
        print(valid_error.json())
        return None


class Chat(BaseModel):
    id: int
    messages: list[Message] = []


def chatKey(chat_id: int) -> str:
    return f"chat#{chat_id}"


def serialize_chat(chat: Chat) -> dict:
    result = chat.model_dump(exclude=["id"])
    result["messages"] = [serialize_message(msg) for msg in result["messages"]]
    return result


def deserialize_chat(id:int, chat_hash: dict) -> Chat:
    chat_hash["messages"] = [deserialize_message(msg["id"], msg) for msg in chat_hash["messages"]]
    chat_hash["id"] = id
    return Chat(**chat_hash)


#Esperar fazer o resto do curso pra ver o pattern de implementação de dados relacionados e objetos aninhados, listas, etc.


def basic_test():
    
    msg1 = Message(id=1, role=MsgRoleEnum.System, content="Olá, eu sou o sistema", date=datetime.now())

    print(msg1.model_dump_json())

    dict_representation:dict = msg1.model_dump()

    print(dict_representation)

    mock_hash_from_redis = {
        b'id': b'1',
        b'role': b'4',
        b'content': b'Ol\xc3\xa1, eu sou o sistema',
        b'date': b'2021-09-30T22:09:19.136000'
    }

    mock_hash_from_redis = normalize_redis_hash(mock_hash_from_redis)

    try:
        msg_from_redis = Message(**mock_hash_from_redis)

        print(msg_from_redis.model_dump_json())
        print(msg_from_redis.model_dump())
    
    except ValidationError as valid_error:
        print("Erro - objeto do redis invalido")
        print(valid_error.json())


async def main():
    client = await RedisHash(REDIS_CONNECTION_POOL)
    
    await client.delete_all(messageKey(1))
    await client.delete_all(messageKey(2))

    msg1 = Message(id=2, role=MsgRoleEnum.User, content="Qual o sentido da vida?", date=datetime.now())

    result = await save_message(msg1)

    print("Resultado do armazenamento da msg", result)

    msg = await retrieve_message(2)
    print(msg.model_dump_json())
    print(msg.model_dump())

if __name__ == "__main__":
    asyncio.run(main())