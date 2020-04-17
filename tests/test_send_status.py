import requests


def test_send_status(server):
    server.reset()
    contact_id = "1234"
    data = [{"id":contact_id}]
    matches = server.get_data_from_id(contact_id) 
    assert 0 == len(matches)
    resp = server.send_status(contacts = data)
    matches = server.get_data_from_id(contact_id) 
    assert 1 == len(matches)
    assert matches == data
    data_2 = [{"id":contact_id, "other_stuff": True}]
    resp = server.send_status(contacts =data_2)
    matches = server.get_data_from_id(contact_id) 
    assert 2 == len(matches)
    # ugh, comparison of list of dicts: https://stackoverflow.com/questions/9845369/comparing-2-lists-consisting-of-dictionaries-with-unique-keys-in-python
    assert set(tuple(sorted(d.items())) for d in matches) == set(tuple(sorted(d.items())) for d in [data[0], data_2[0]])
    return

def test_send_status_with_geolocation(server):
    server.reset()
    locations = [{ "lat": 37.773972, "long": -122.431297 }, { "lat": 37.773972, "long": -122.431297 }]
    resp = server.send_status(locations = locations, updatetoken =  "hash",
                      replaces = "nonce")
    assert resp.status_code == 200
    matches = server.get_data_to_match_hash('hash')
    assert 2 == len(matches)
    assert matches
    return


