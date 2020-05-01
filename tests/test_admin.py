# noinspection PyUnusedLocal
def test_admin_config(server, data):
    server.reset()
    resp = server.admin_config()
    assert resp.status_code == 200
    json_data = resp.json()
    assert json_data.get('directory')
    return


def test_admin_status(server, data):
    server.reset()
    server.send_status_json(contacts=[{'id': data.valid_ids[0]}], locations=[data.locations_in[0]])
    resp = server.admin_status()
    assert resp.status_code == 200
    assert resp.json().get('contacts_count') == 1
    return
