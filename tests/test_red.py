import requests

def test_red(server):
    server.reset()
    contact_id = "1234"
    data = {"id":contact_id}
    assert server.get_data_from_id(contact_id) == None
    resp = server.red(data)
    assert server.get_data_from_id(contact_id) == data
    return
