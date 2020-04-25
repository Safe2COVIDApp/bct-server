import time
from lib import new_nonce


def location_match(here, there):
    return (here['long'] == there['long']) and (here['lat'] == there['lat'])

def test_send_seq1(server, data):
    server.reset()
    bob_id = data.valid_ids[0]
    bob_prefix = bob_id[:3]
    # Bob polls - should be nothing
    json_data = server.scan_status_json(contact_prefixes = [bob_prefix], locations = [ data.locations_box ])
    # json_data -> .ids -> filter for bob_id -> should not find
    bob_id_alerts = [ i for i in json_data['ids'] if (i.id == bob_id) ]
    assert len(bob_id_alerts) == 0
    # Alice sends in an alert for location
    server.send_status_json(locations = [data.locations_in[0], data.locations_out[0]], status = 2)
    # Bob should see it
    json_data = server.scan_status_json(contact_prefixes = [bob_prefix], locations = [ data.locations_box ])
    bob_location_alerts = [ i for i in json_data['locations'] if ( location_match(data.locations_in[0], i) and (i.get('status',0) > 0)) ]
    assert len(bob_location_alerts) == 1
    # Carol sends in an alert for Bob's id
    server.send_status_json(contacts = [{"id":bob_id}], status = 2)
    # Bob should see it
    json_data = server.scan_status_json(contact_prefixes = [bob_prefix], locations = [ data.locations_box ])
    bob_id_alerts = [ i for i in json_data['ids'] if (i.get('id') == bob_id) ]
    assert len(bob_id_alerts) == 1

def test_seq_update_replace(server, data):
    server.reset()
    bob_id = data.valid_ids[0]
    bob_prefix = bob_id[:3]
    # === Alice adds bob by id as POI/Orange
    nonce = new_nonce()
    server.send_status_json(contacts = [{"id":bob_id}], locations = [ data.locations_in[0]], status = 2, nonce = nonce)
    time.sleep(1.5) # Make sure its a new time slot
    # === Bob polls
    json_data = server.scan_status_json(contact_prefixes = [bob_prefix], locations = [ data.locations_box ])
    bob_id_alerts = [ i for i in json_data['ids'] if (i.get('id') == bob_id) ]
    bob_since = json_data.get('now')
    assert len(bob_id_alerts) == 1
    assert bob_id_alerts[0].get('status') == 2
    bob_location_alerts = [ i for i in json_data['locations'] if (i.get('lat') == data.locations_in[0].get('lat')) and (i.get('long') == data.locations_in[0].get('long')) ]
    assert len(bob_location_alerts) == 1
    assert bob_location_alerts[0].get('status') == 2
    time.sleep(1.5) # Make sure its a new time slot
    # === Alice updates new bob with wrong replaces
    nonce2 = new_nonce()
    nonce3 = new_nonce()
    server.status_update_json(status = 1, nonce = nonce3, replaces = nonce2, length = 2)
    # === Bob polls
    json_data = server.scan_status_json(contact_prefixes = [bob_prefix], locations = [ data.locations_box ], since = bob_since)
    bob_since = json_data.get('now')  # use for next query
    bob_id_alerts = [ i for i in json_data['ids'] if (i.get('id') == bob_id) ]
    assert len(bob_id_alerts) == 0  # Bob does not see the record from last time.
    bob_location_alerts = [ i for i in json_data['locations'] if (i.get('lat') == data.locations_in[0].get('lat')) and (i.get('long') == data.locations_in[0].get('long')) ]
    assert len(bob_location_alerts) == 0
    time.sleep(2.0)  # Make sure its a new time slot
    # === Alice updates bob with correct nonce
    server.status_update_json(status = 4, nonce = nonce3, replaces = nonce, length = 2)
    # === Bob polls
    json_data = server.scan_status_json(contact_prefixes = [bob_prefix], locations = [ data.locations_box ], since = bob_since)
    bob_id_alerts = [ i for i in json_data['ids'] if (i.get('id') == bob_id) ]
    assert len(bob_id_alerts) == 1
    assert bob_id_alerts[0].get('status') == 4
    bob_location_alerts = [ i for i in json_data['locations'] if (i.get('lat') == data.locations_in[0].get('lat')) and (i.get('long') == data.locations_in[0].get('long')) ]
    assert len(bob_location_alerts) == 1
    assert bob_location_alerts[0].get('status') == 4

    # TODO-33 Similar idea but using hospital style replacement