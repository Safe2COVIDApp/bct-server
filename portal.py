#import logging
from twisted.logger import Logger


# This file is intended to be run in a separate webserver from contacts.py
# So there is no data sharing,
# BUT Its currently kicked off by the twisted server in server.py
# To allow testing on a single server, all urls start /portal_

logger = Logger()

registry = {}

def register_method(_func=None, *, route):
    def decorator(func):
        registry[route] = func

        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)

class Portal:

    @register_method(route='/portal/user')
    def portal_user(self, data, args):
