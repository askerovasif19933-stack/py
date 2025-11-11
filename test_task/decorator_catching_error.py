import functools


def decorator_catching_errors(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f'Ошибка {e}')
            return False
    return wrapper