import asyncio

def gen(n):
    i = 0
    while i < n:
        yield i
        i += 1
    print('generator exhausted')

async def afunc(i, semaphore):
    async with semaphore:
        print(f'Start {i}')
        await asyncio.sleep(2)
        print(f'End {i}')

async def main(generator):
    first_i = next(iter(generator))
    sem = asyncio.Semaphore(3)
    await afunc(first_i, sem)
    await asyncio.gather(*{afunc(i, sem) for i in generator})

asyncio.run(main(gen(10)))
