import redis
import time
import os


r = redis.from_url(os.environ.get("BROKER_URI", "redis://localhost:6379/0"))
r.config_set("notify-keyspace-events", "KEA")

p = r.pubsub(ignore_subscribe_messages=True)
p.psubscribe('__keyevent@0__:expired')

while True:
    message = p.get_message()
    if message:
        key = message['data'].decode()
        if ":" in key:
            queue, task_id = key.split(":")
            payload = r.hget(f"data:{queue}", task_id)
            r.hdel(f"data:{queue}", task_id)
            r.rpush(queue, payload)
    else:
        pass
    time.sleep(0.01)


