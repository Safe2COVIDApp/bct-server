import requests
import time
import datetime
from lib import new_nonce, hash_nonce


def location_match(here, there):
    return (here['long'] == there['long']) and (here['lat'] == there['lat'])

def test_send_seq1(server, data):
    server.reset()
    bob_id = data.valid_ids[0]
    bob_prefix = bob_id[:3]
    # Bob polls - should be nothing
    resp = server.scan_status(contact_prefixes = [bob_prefix], locations = [ data.locations_box ]).json()
    # resp -> .ids -> filter for bob_id -> shouldnt find
    bob_id_alerts = [ i for i in resp['ids'] if (i.id == bob_id) ]
    assert len(bob_id_alerts) == 0
    # Alice sends in an alert for location
    unusedResp = server.send_status(locations = [data.locations_in[0], data.locations_out[0]], status = 2)
    # Bob should see it
    resp = server.scan_status(contact_prefixes = [bob_prefix], locations = [ data.locations_box ]).json()
    bob_location_alerts = [ i for i in resp['locations'] if ( location_match(data.locations_in[0], i) and (i.get('status',0) > 0)) ]
    assert len(bob_location_alerts) == 1
    # Carol sends in an alert for Bob's id
    unusedResp = server.send_status(contacts = [{"id":bob_id}], status = 2)
    # Bob should see it
    resp = server.scan_status(contact_prefixes = [bob_prefix], locations = [ data.locations_box ]).json()
    bob_id_alerts = [ i for i in resp['ids'] if (i.get('id') == bob_id) ]
    assert len(bob_id_alerts) == 1

def test_seq_update_replace(server, data):
    server.reset()
    bob_id = data.valid_ids[0]
    bob_prefix = bob_id[:3]
    # === Alice adds bob by id as POI/Orange
    nonce = new_nonce()
    unusedResp = server.send_status(contacts = [{"id":bob_id}], status = 2, nonce = nonce)
    time.sleep(1.5) # Make sure its a new time slot
    # === Bob polls
    resp = server.scan_status(contact_prefixes = [bob_prefix], locations = [ data.locations_box ]).json()
    bob_id_alerts = [ i for i in resp['ids'] if (i.get('id') == bob_id) ]
    bob_since = resp.get('now')
    assert len(bob_id_alerts) == 1
    assert bob_id_alerts[0].get('status') == 2
    time.sleep(1.5) # Make sure its a new time slot
    # === Alice updates new bob with wrong replaces
    nonce2 = new_nonce()
    nonce3 = new_nonce()
    resp = server.status_update(status = 1, nonce = nonce3, replaces = nonce2, length = 1)
    assert resp.status_code == 200
    # === Bob polls
    r = server.scan_status(contact_prefixes = [bob_prefix], locations = [ data.locations_box ], since = bob_since)
    assert r.status_code == 200
    resp = r.json()
    bob_since = resp.get('now')
    bob_id_alerts = [ i for i in resp['ids'] if (i.get('id') == bob_id) ]
    assert len(bob_id_alerts) == 0 # Bob does not see the record from last time.
    time.sleep(2.0) # Make sure its a new time slot
    # === Alice updates bob with correct nonce
    resp = server.status_update(status = 4, nonce = nonce3, replaces = nonce, length = 1)
    assert resp.status_code == 200
    # === Bob polls
    r = server.scan_status(contact_prefixes = [bob_prefix], locations = [ data.locations_box ], since = bob_since)
    assert resp.status_code == 200
    resp = r.json()
    bob_id_alerts = [ i for i in resp['ids'] if (i.get('id') == bob_id) ]
    assert len(bob_id_alerts) == 1
    assert bob_id_alerts[0].get('status') == 4
    # TODO-33 Same sequence with locations - or add in above

    # TODO-33 Similar idea but using hospital style replacement