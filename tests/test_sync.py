def test_sync(server):
    resp = server.sync()
    assert resp.status_code == 200
    return
