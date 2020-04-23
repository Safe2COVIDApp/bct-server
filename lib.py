# the module contains the client and server process to manage ids

import hashlib

def hash_nonce(nonce):
    return hashlib.sha1(nonce.encode()).hexdigest()

def fold_hash(hash):
    return "%X" % (int(hash[:20], 16) ^ int(hash[20:],16))

