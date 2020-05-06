// TODO this will be the URL of the portal
//alert("XXX JS loading")

var urlPortalServer = "http://localhost:8080";
var urlPortalRegister = "/portal/register";
var urlPortalResult = "/portal/result";
var provider = null;
var testid = null;
var replaces = "PLACEHOLDER_FILL_IN_BY_CLIENT";

function hideOrShow(bool, x) {
  let el = document.getElementById(x)
  if (bool) {
    el.style.display = "block";
  } else {
    el.style.display = "none";
  }
}
function clientSetVisible()
{
  hideOrShow(provider === null, "provider_form");
  hideOrShow(provider !== null && testid === null, "testid_form");
  hideOrShow(provider !== null && testid !== null, "results");
}
function providerSetVisible() {
  hideOrShow(provider === null, "provider_form");
  hideOrShow(provider !== null && testid === null, "testid_form");
  hideOrShow(provider !== null && testid !== null, "results");
}
function clientProviderSelected(event)
{
    provider = document.getElementById("provider").value;
    clientSetVisible();
    return false;
}
// Return a promise that will return json
function submitToPortal(relUrl, body)
{
    return fetch(urlPortalServer + relUrl, {
        method: "POST",
        headers: {
             "Content-type": "application/json"
        },
        body: body,
        mode: 'cors',
        cache: 'no-store',
        redirect: 'follow',  // Chrome defaults to manual
        keepalive: false    // Keep alive - mostly we'll be going back to same places a lot
    })
}
function clientTestidEntered(event)
{
    testid = document.getElementById("testid").value;
    clientSetVisible();
    //TODO-114 the portal doesn't even need the testid, could hash on the client, but need to find library (see also clientTestidEntered)
    let body = JSON.stringify({
        "provider": encodeURIComponent(provider),
        "testid": encodeURIComponent(testid),
        "status": encodeURIComponent(status)
    });
    submitToPortal(urlPortalRegister, body)
    .then(resp => resp.json())
    .then(json =>
        alert("Should be using hashed key " + json["hashed_id"] + " for poll"))
    .catch(err =>
        alert("Failed to contact portal " + err.message));
    return false;
}
function providerProviderSelected(event)
{
    provider = document.getElementById("provider").value;
    providerSetVisible();
    return false;
}
function providerTestidEntered(event) {
    testid = document.getElementById("testid").value;
    providerSetVisible();
    let status = document.querySelectorAll("input[name=status]:checked")[0].value
    // TODO-114 portal doesnt need to see the Testid - hash here on the client (see also clientTestidEntered)
    let body = JSON.stringify({
        "provider": encodeURIComponent(provider),
        "testid": encodeURIComponent(testid),
        "status": encodeURIComponent(status)
    });
    // TODO-114 build server side then debug
    submitToPortal(urlPortalResult, body)
    .then(resp =>
        resp.json())
    .then(json =>
        alert("Should be using hashed key " + json["hashed_id"] + " for poll"))
    .catch(err =>
        alert("Failed to contact portal " + err.message));
    return false;
}