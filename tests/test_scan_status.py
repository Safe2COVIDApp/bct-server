import requests


def test_scan_status(server):
    server.reset()
    contact_id = "12345678"
    data = [{"id":contact_id}]
    server.send_status(contacts = data)
    resp = server.scan_status(contact_prefixes = ['123'])
    return



