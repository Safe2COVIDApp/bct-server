import requests

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
    resp = server.red(data)
    matches = server.get_data_from_id(contact_id) 
    assert 2 == len(matches)
    assert matches[0] == data
    return
