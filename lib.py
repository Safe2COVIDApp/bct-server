# the module contains the client and server process to manage ids

import hashlib
import string
import random


# The hash_nonce and verify_nonce functions are a pair that may be changed as the function changes.
# verify(nonce, hashupdates(nonce)) == true;

def hash_nonce(nonce):
    return hashlib.sha1(nonce.encode()).hexdigest()

def fold_hash(hash40):
    return "%X" % (int(hash40[:20], 16) ^ int(hash40[20:],16))

def random_ascii(length):
    return ''.join([random.choice(string.ascii_letters + string.digits) for n in range(length)])

def new_nonce(seed = None):
    if not seed:
        seed = random_ascii(8)
    return hash_nonce(seed)  # first hash_nonce is to get size same as updates

# This group of functions centralize the process and cryptography for nonce -> replacement_token -> update_token

# Generate Replacement token from nonce + n (which should increment)
# Requirements - none reversable, cannot be used to find the nonce, or any other replacment_token
def replacement_token(nonce, n):
    return hash_nonce(nonce + str(n))

# Generate Update Token from Replacement Token
# Requirements: Not reversable, confirmable i.e. updateToken(rt) == ut shows that you possess the original rt used to create ut
def update_token(rt):
    return fold_hash(hash_nonce(nonce))

# Check that the rt is a correct rt for the ut.
def confirm_update_token(ut, rt):
    return update_token(rt) == ut

# TODO-55 need to change from fold_hash and hash_nonce to replacement_token and update_token in contacts.py
