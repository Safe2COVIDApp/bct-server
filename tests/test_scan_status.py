import requests



def test_scan_status(server, data):
    server.reset()
    contact_id = data.valid_ids[0]
    prefix = contact_id[0:3]
    data = [{"id":contact_id}]
    server.send_status(contacts = data, status = 2)
    expected = [ { 'id': contact_id, 'status': 2 }]
    resp = server.scan_status(contact_prefixes = [prefix])
    assert resp.json()['ids'] == expected
    return



