import time

def main(args):
    t1 = time.time()
    time.sleep(3)
    t2 = time.time()
    return {
        'measurement': {
            'lib_load_time': t1,
            'model_load_time': t2,
            'is_load?': 0,
        }
    }

print(main(1))
