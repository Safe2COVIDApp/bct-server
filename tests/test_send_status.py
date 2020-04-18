import requests
import time
import datetime

def test_send_status(server):
    server.reset()
    contact_id = server.valid_ids[0]
    data = [{"id":contact_id}]
    matches = server.get_data_from_id(contact_id) 
    assert 0 == len(matches)
    resp = server.send_status(contacts = data)
    matches = server.get_data_from_id(contact_id) 
    assert 1 == len(matches)
    assert matches == data
    data_2 = [{"id":contact_id, "other_stuff": True}]
    resp = server.send_status(contacts =data_2)
    # Check it made it to the file system where we expected.
    matches = server.get_data_from_id(contact_id)
    expected = [data[0], data_2[0]]
    assert 2 == len(matches)
    # ugh, comparison of list of dicts: https://stackoverflow.com/questions/9845369/comparing-2-lists-consisting-of-dictionaries-with-unique-keys-in-python
    assert set(tuple(sorted(d.items())) for d in matches) == set(tuple(sorted(d.items())) for d in expected)
    return

def test_send_status_with_geolocation(server):
    server.reset()
    updatetoken='hash' # TODO replace with hash function when ready
    replaces='nonce'
    locations = [server.locations_in[0], server.locations_out[0]]
    resp = server.send_status(locations = locations, updatetoken =  updatetoken, replaces = replaces)
    assert resp.status_code == 200
    # Now check it made it to the geo files
    matches = server.get_data_to_match_hash('hash')
    now = int(time.time())
    expected = [{"lat": d["lat"], "long": d["long"], "date": now, "updatetoken": updatetoken, "replaces": replaces } for d in locations]
    #assert 2 == len(matches)
    assert matches
    # ugh, comparison of list of dicts: https://stackoverflow.com/questions/9845369/comparing-2-lists-consisting-of-dictionaries-with-unique-keys-in-python
    assert set(tuple(sorted(d.items())) for d in matches) == set(tuple(sorted(d.items())) for d in expected)
    return

