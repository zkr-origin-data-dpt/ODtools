import timeit


def timeit_counter(function_name: str = '', parameters: list = None, execute_num: int = 100):
    """
    Multiple execution functions
    Statistical function execution time
    :param function_name: function name
    :param parameters: operating environment
    :param execute_num: number of executions
    :return:
    """
    in_obj = timeit.Timer(function_name + "()",
                          "from __main__ import {}; {}".format(function_name, '; '.join(parameters))
                          if parameters else "from __main__ import {}".format(function_name))

    print("{} execute {} cost {}s:".format(function_name, execute_num, in_obj.timeit(number=execute_num)))


a = [i for i in range(10000)]


def func_1():
    set_a = set()
    for i in a:
        set_a.add(i)


def func_2():
    hash_a = {}
    for i in a:
        hash_a[i] = ""


if __name__ == '__main__':
    timeit_counter(function_name='func_1', parameters=['a', ], execute_num=1000)
