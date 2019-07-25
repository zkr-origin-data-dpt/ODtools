class Singleton(type):
    """
    Singleton mode(Metaclass)
    """
    _instances = dict()

    def __call__(cls, *args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cls._instances:
            cls._instances[key] = super(Singleton, cls).__call__(*args, **kwargs)
            print('Instantiation {class_name} success'.format(class_name=cls.__name__))
        return cls._instances[key]


class A(metaclass=Singleton):
    def __init__(self, parameter):
        print(parameter)


if __name__ == '__main__':
    a1 = A('1')
    a2 = A('2')
    print(id(a1), id(a2))
