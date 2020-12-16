import aiometer
import asyncio

# test if aiometer can run tasks from generator instead of list
def task_gen(n):
    i=0
    while i<n:
        yield i
        i+=1
    print('exhausted generator')

async def afunc(n):
    print(f'starting task {n}')
    await asyncio.sleep(1)
    print(f'finished task {n}')

asyncio.run(aiometer.run_on_each(afunc, task_gen(10), max_at_once=2, max_per_second=2))

# Success! output is:
# starting task 0
# starting task 1
# finished task 0
# starting task 2
# finished task 1
# starting task 3
# finished task 2
# starting task 4
# finished task 3
# starting task 5
# finished task 4
# starting task 6
# finished task 5
# starting task 7
# finished task 6
# starting task 8
# finished task 7
# exhausted generator
# starting task 9
# finished task 8
# finished task 9