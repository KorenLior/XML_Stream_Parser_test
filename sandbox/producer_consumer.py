import asyncio
import random
from typing import AsyncIterator, Dict, Any, Iterable, Optional

async def consumer(id: int, queue: asyncio.Queue)-> AsyncIterator[str]:
    while True:
        item = await queue.get()
        print(item)
        await asyncio.sleep(random.uniform(0.1, 0.5))
        queue.task_done()

async def producer(id: int, queue: asyncio.Queue)-> AsyncIterator[str]:
    for i in range(100):
        await asyncio.sleep(random.uniform(0.1, 0.5))
        await queue.put(f"Producer {id}: Item {i}")
        await asyncio.sleep(random.uniform(0.1, 0.5))

async def main():
    queue = asyncio.Queue()
    producer_task1 = asyncio.create_task(producer(1, queue))
    producer_task2 = asyncio.create_task(producer(2, queue))
    consumer_task = asyncio.create_task(consumer(1, queue))
    try:
        await asyncio.wait_for(asyncio.gather(producer_task1, producer_task2), timeout=10) # producer finished
        await queue.join()  # consumer finished
        consumer_task.cancel() # cancel consumer task
        print("Producer and consumer tasks completed successfully")
    except asyncio.TimeoutError:
        producer_task1.cancel()
        producer_task2.cancel()
        consumer_task.cancel()
        await asyncio.gather(producer_task1, producer_task2, consumer_task, return_exceptions=True)
        print("Producer and consumer tasks cancelled")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        print("EXIT")

if __name__ == "__main__":
    asyncio.run(main())