import time


def time_counter(func):
    """
    Statistical function execution time
    Decorator version
    :param func: function
    :return:
    """
    def inner(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        print('{} cost {}s'.format(func.__name__, time.time() - start_time))
        return result
    return inner


@time_counter
def func_1(num):
    for i in range(num):
        print(i)


if __name__ == '__main__':
    func_1(1000)
