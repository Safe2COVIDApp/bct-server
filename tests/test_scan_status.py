import time


def test_scan_status(server, data):
    server.reset()
    contact_id = data.valid_ids[0]
    prefix = contact_id[0:3]
    data = [{"id": contact_id}]
    server.send_status_json(contacts=data, status=2)
    time.sleep(1.0)  # Make sure its a new time slot
    expected = [{'id': contact_id, 'status': 2}]
    json_data = server.scan_status_json(contact_prefixes=[prefix], since="2007-04-05T14:30Z")
    assert json_data['contact_ids'] == expected
    return
