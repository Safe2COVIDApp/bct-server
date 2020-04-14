import requests

import pdb

def test_red(server):
    server.reset()
    contact_id = "1234"
    data = {"id":contact_id}
    matches = server.get_data_from_id(contact_id) 
    assert 0 == len(matches)
    resp = server.red(data)
    matches = server.get_data_from_id(contact_id) 
    assert 1 == len(matches)
    assert matches[0] == data
    data_2 = {"id":contact_id, "other_stuff": True}
    resp = server.red(data_2)
    matches = server.get_data_from_id(contact_id) 
    assert 2 == len(matches)
    # ugh, comparison of list of dicts: https://stackoverflow.com/questions/9845369/comparing-2-lists-consisting-of-dictionaries-with-unique-keys-in-python
    assert set(tuple(sorted(d.items())) for d in matches) == set(tuple(sorted(d.items())) for d in [data, data_2])
    return
