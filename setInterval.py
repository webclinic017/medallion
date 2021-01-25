from random import random
import threading
import time
import asyncio

#calc_results = {}

def background_calculation(var): ## ib wrapper function
    # here goes some long calculation
    time.sleep(1)

    # when the calculation is done, the result is stored in a global variable
    print('background calculation var is', var)
    global calc_results
    calc_results[var] = var * 10
    # to "unset" it we use the "clear()" function. Also available: "is_set()"    
    result_available[var].set()
    time.sleep(1)

async def get_result(var): ## ib client function
    global calc_results
    try: calc_results
    except NameError: calc_results = {}
    result_available[var] = threading.Event()
    thread = threading.Thread(target=background_calculation, args=[var])
    thread.start()
    print('Making fake request...', var)
    # TODO: wait here for the result to be available before continuing!
    await asyncio.sleep(0.1)
    result_available[var].wait()
    # 
    print('In get_result. Result is', calc_results[var])
    return calc_results[var]


async def main():
  vars = [1,2,3]
  global result_available
  result_available = {}
  tasks = []
  for var in vars:
    tasks.append(get_result(var))
  final_results = await asyncio.gather(*tasks)
  print('In main. Result is:', final_results, '\n')

if __name__ == '__main__':
    try:
      #loop = asyncio.get_event_loop()
      #loop.run_until_complete(main())
      asyncio.run(main())
    except Exception as e:
      print('Error caught:', e)
