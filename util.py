def normalize_redis_hash(hash_result:dict) -> dict:
    if hash_result is None:
        return None
    
    return {k.decode():v.decode() for k, v in hash_result.items()}