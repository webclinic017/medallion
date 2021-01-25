import asyncio

async def find_divisibles(inrange, div_by):
    print("Finding nums in range {} divisible by {}".format(inrange, div_by))
    located = []
    for i in range(inrange):
        if i % div_by == 0:
            located.append(i)
        if i % 50000 == 0:
            await asyncio.sleep(0.1)
    print("Done with nums in range {} divisible by {}".format(inrange, div_by))
    return located

async def main():
    divs1 = await find_divisibles(5080000, 34113)
    divs2 = await find_divisibles(100052, 3210)
    divs3 = await find_divisibles(500, 3)
    print(divs1)
    

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except Exception as e:
        pass
    finally:
        loop.close()

# Take away:
# Setup the loop execution for the app, then we can use async/await as in JS