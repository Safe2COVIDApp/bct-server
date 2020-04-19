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
    unusedResp = server.send_status(locations = [data.locations_in[0], data.locations_out[0]], status = 2, updateToken = "hash")
    # Bob should see it
    resp = server.scan_status(contact_prefixes = [bob_prefix], locations = [ data.locations_box ]).json()
    bob_location_alerts = [ i for i in resp['locations'] if ( location_match(data.locations_in[0], i) and (i.get('status',0) > 0)) ]
    assert len(bob_location_alerts) == 1
    # Carol sends in an alert for Bob's id
    unusedResp = server.send_status(contacts = [{'id': bob_id}], status = 2)
    # Bob should see it
    resp = server.scan_status(contact_prefixes=[bob_prefix], locations=[data.locations_box]).json()
    bob_id_alerts = [ i for i in resp['ids'] if (i.get('id') == bob_id) ]
    # TODO-MITRA this fails - check why
    assert len(bob_id_alerts) == 1
    # Alice updates her status to Green
    unusedResp = server.send_status(status = 0, replaces = "nonce")
    resp = server.scan_status(contact_prefixes = [bob_prefix], locations = [ data.locations_box ]).json()
    bob_location_alerts = [ i for i in resp['locations'] if ( location_match(data.locations_in[0], i) and (i.status > 0)) ]
    assert len(bob_location_alerts) == 1



