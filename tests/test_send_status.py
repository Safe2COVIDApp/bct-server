import time

def test_send_status(server, data):
    server.reset()
    contact_id = data.valid_ids[0]
    json_data = [{"id":contact_id}]
    matches = server.get_data_from_id(contact_id) 
    assert 0 == len(matches)
    server.send_status_json(contacts = json_data)
    matches = server.get_data_from_id(contact_id) 
    assert 1 == len(matches)
    assert matches == json_data
    json_data_2 = [{"id":contact_id, "other_stuff": True}]
    server.send_status_json(contacts = json_data_2)
    # Check it made it to the file system where we expected.
    matches = server.get_data_from_id(contact_id)
    expected = [json_data[0], json_data_2[0]]
    assert 2 == len(matches)
    # ugh, comparison of list of dicts: https://stackoverflow.com/questions/9845369/comparing-2-lists-consisting-of-dictionaries-with-unique-keys-in-python
    assert set(tuple(sorted(d.items())) for d in matches) == set(tuple(sorted(d.items())) for d in expected)
    return

def test_send_status_with_geolocation(server, data):
    server.reset()
    locations = [data.locations_in[0], data.locations_out[0]]
    server.send_status_json(locations = locations)
    # Now check it made it to the geo files
    matches = server.get_data_to_match_hash('hash') # [ [ { lat, long } ], [ { lat, long } ]]
    flattened_matches = [item for sublist in matches for item in sublist]
    now = int(time.time())
    expected = [{"lat": d["lat"], "long": d["long"], "date": now } for d in locations]
    assert 2 == len(flattened_matches)
    # ugh, comparison of list of dicts: https://stackoverflow.com/questions/9845369/comparing-2-lists-consisting-of-dictionaries-with-unique-keys-in-python
    assert set(tuple(sorted(d.items())) for d in flattened_matches) == set(tuple(sorted(d.items())) for d in expected)
    return

