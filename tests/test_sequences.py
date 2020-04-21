import requests
import time
import datetime


def location_match(here, there):
    return (here['long'] == there['long']) and (here['lat'] == there['lat'])

def test_send_seq1(server, data):
    # TODO replace hash and nonce in here
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
    # TODO Alice adds bob by id as POI/Orange
    nonce = server.new_nonce()
    unusedResp = server.send_status(contacts = [{"id":bob_id}], status = 2, nonce = nonce)
    # TODO Bob polls
    resp = server.scan_status(contact_prefixes = [bob_prefix], locations = [ data.locations_box ]).json()
    bob_id_alerts = [ i for i in resp['ids'] if (i.get('id') == bob_id) ]
    bob_since = resp.get('now')
    assert len(bob_id_alerts) == 1
    time.sleep(2.0) # Make sure its a new time slot
    # TODO Alice updates new bob with wrong replaces
    nonce2 = server.new_nonce()
    unusedResp = server.send_status(contacts = [{"id":bob_id}], status = 3, replaces = nonce2)
    # TODO Bob polls
    r = server.scan_status(contact_prefixes = [bob_prefix], locations = [ data.locations_box ], since = bob_since)
    resp = r.json()
    bob_id_alerts = [ i for i in resp['ids'] if (i.get('id') == bob_id) ]
    bob_since = resp.get('now')
    assert len(bob_id_alerts) == 1
    time.sleep(2.0) # Make sure its a new time slot
    # TODO Alice updates bob
    nonce2 = server.new_nonce()
    unusedResp = server.send_status(contacts = [{"id":bob_id}], status = 1, nonce = nonce2)
    # TODO Bob polls
    resp = server.scan_status(contact_prefixes = [bob_prefix], locations = [ data.locations_box ]).json()
    bob_id_alerts = [ i for i in resp['ids'] if (i.get('id') == bob_id) ]
    bob_replaces = [ x for i in bob_id_alerts if i.get('replaces') ]
    assert len(bob_id_alerts) == 0 # This migth be wrong, it might be that its the deleted thing we want to catch

    # TODO Same sequence with locations - or add in above

    # TODO Similar idea but using hospital style replacement