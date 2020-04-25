# the module contains the client and server process to manage ids

import hashlib
import string
import random


# The hash_nonce and verify_nonce functions are a pair that may be changed as the function changes.
# verify(nonce, hashupdates(nonce)) == true;
def new_nonce(seed = None):
    if not seed:
        seed = random_ascii(8)
    return hash_nonce(seed)  # first hash_nonce is to get size same as updates

def hash_nonce(nonce):
    return hashlib.sha1(nonce.encode()).hexdigest()

def fold_hash(hash):
    return "%X" % (int(hash[:20], 16) ^ int(hash[20:],16))

def random_ascii(length):
    return ''.join([random.choice(string.ascii_letters + string.digits) for n in range(length)])