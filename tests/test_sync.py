import time
import logging
logger = logging.getLogger(__name__)
from . import run_server_in_context, get_free_port, sort_list_of_dictionaries
import pdb


def broken_test_sync():
    port_1 = get_free_port()
    port_2 = get_free_port()
    server_urls = 'http://localhost:%d, http://localhost:%d' % (port_1, port_2)

    json_data_1 = [{"id":"123456789"}]
    json_data_2 = [{"id":"987654321"}]
    # gack, for some reason we assertion failures hang the tests when done within the 'with'
    # so for now we just check all the assertions after the with
    with run_server_in_context(server_urls = server_urls, port = port_1) as server_1:
        with run_server_in_context(server_urls = server_urls, port = port_2) as server_2:
            logger.info('server1 is: %s' % server_1)
            logger.info('server2 is: %s' % server_2)
            resp_1_1 = server_1.sync()
            resp_2_1 = server_2.sync()
            server_1.send_status_json(contacts = json_data_1)
            server_2.send_status_json(contacts = json_data_2)
            time.sleep(2)
            resp_1_2 = server_1.sync()
            resp_2_2 = server_2.sync()
    assert resp_1_1.status_code == 200
    assert 'contacts' not in resp_1_1.json()

    assert resp_2_1.status_code == 200
    assert 'contacts' not in resp_2_1.json()

    all_contacts = sort_list_of_dictionaries(json_data_1 + json_data_2)
    assert 'contacts' in resp_1_2.json()
    assert sort_list_of_dictionaries(resp_1_2.json()['contacts']) == all_contacts

    assert 'contacts' in resp_2_2.json()
    assert sort_list_of_dictionaries(resp_2_2.json()['contacts']) == all_contacts
    return

