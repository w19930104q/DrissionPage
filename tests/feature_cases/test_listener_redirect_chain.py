"""Feature: opt-in redirect-chain collection for tab/frame listeners."""
from __future__ import annotations

from DrissionPage._units.listener import Listener, SSEPacket
from DrissionPage.items import RedirectHop
from support import assert_equal, assert_false, assert_not_in, assert_true, TestFailure


FEATURE_ID = "listener_redirect_chain"
REQUIRES_BROWSER = False
FEATURES = (
    "listener_redirect_chain_replay",
    "listener_redirect_chain_final_filter",
    "listener_redirect_chain_extra_info",
    "listener_redirect_chain_silence",
    "listener_redirect_chain_lifecycle",
)


class _Owner:
    def __init__(self):
        self.tab_id = "tab-chain"
        self.timeout = 0
        self._messenger_running = True
        self.listen = None
        self.browser = None
        self.callbacks = {}
        self.enabled = []
        self.disabled = []
        self.cdp_calls = []
        self.bodies = {}
        self.post_data = {}

    def _set_callback(self, event, callback):
        self.callbacks[event] = callback

    def _enable_domain(self, domain, **kwargs):
        self.enabled.append((domain, kwargs))

    def _disable_domain(self, domain):
        self.disabled.append(domain)

    def _run_cdp(self, method, **kwargs):
        self.cdp_calls.append((method, dict(kwargs)))
        request_id = kwargs.get("requestId")
        if method == "Network.getResponseBody":
            return self.bodies.get(request_id, {})
        if method == "Network.getRequestPostData":
            return {"postData": self.post_data.get(request_id)}
        raise AssertionError(f"unexpected CDP call: {method}")


def _make_listener(urls=None, *, redirect_chain=True, res_type=None, method=None):
    owner = _Owner()
    listener = Listener(owner)
    owner.listen = listener
    if res_type is not None:
        getattr(listener.set_res_type, res_type)(only=True)
    if method is not None:
        getattr(listener.set_method, method)(only=True)
    listener.start(urls, redirect_chain=redirect_chain)
    return owner, listener


def _request(rid, url, method="GET", headers=None, type_="Fetch", frame="frame-chain",
             timestamp=100.0, redirect=None, extra_info=True, redirect_extra_info=True,
             has_post_data=False):
    """Build one requestWillBeSent event. One call = one redirect hop."""
    kwargs = {
        "requestId": rid,
        "frameId": frame,
        "type": type_,
        "timestamp": timestamp,
        "request": {
            "url": url,
            "method": method,
            "headers": dict(headers or {}),
            "timestamp": timestamp,
            "hasPostData": has_post_data,
        },
        "hasExtraInfo": extra_info,
    }
    if redirect is not None:
        kwargs["redirectResponse"] = redirect
        kwargs["redirectHasExtraInfo"] = redirect_extra_info
    return kwargs


def _redirect_response(url, status, headers):
    return {
        "url": url,
        "status": status,
        "statusText": "Redirect",
        "headers": dict(headers),
        "mimeType": "text/plain",
    }


def _response(url, status=200, headers=None, mime_type="application/json"):
    return {
        "url": url,
        "status": status,
        "statusText": "OK",
        "headers": dict(headers or {}),
        "mimeType": mime_type,
    }


def run(ctx):
    _check_default_off_and_api()
    _check_basic_chain_order_and_headers()
    _check_final_hop_filtering()
    _check_extra_info_early_late_and_absent()
    _check_concurrent_chains_and_same_request_id()
    _check_failed_chain()
    _check_wait_silent_and_lifecycle()
    _check_non_http_and_preserved_configuration()


def _check_default_off_and_api():
    owner, listener = _make_listener("/final", redirect_chain=False)
    assert_false(listener.redirect_chain, "redirect-chain collection must default to off")
    listener._requestWillBeSent(**_request("off-1", "https://host.test/start"))
    listener._requestWillBeSent(**_request(
        "off-1", "https://host.test/final", "GET", {"X-Hop": "2"},
        redirect=_redirect_response("https://host.test/start", 302, {"X-Hop": "1"}),
    ))
    listener._response_received(requestId="off-1", type="Fetch", timestamp=101.0,
                                response=_response("https://host.test/final"))
    owner.bodies["off-1"] = {"body": '{"ok": true}', "base64Encoded": False}
    listener._loading_finished(requestId="off-1")
    packet = listener.wait(timeout=0.01)
    assert_equal(packet.url, "https://host.test/final",
                 "default listener mode should keep the classic final-request packet")
    assert_equal(packet.redirects, [], "default listener mode should not expose redirect hops")
    assert_equal(packet.redirect_count, 0, "default listener mode should report no redirects")

    listener.set_redirect_chain(True)
    assert_true(listener.redirect_chain, "set_redirect_chain(True) should enable chain mode")
    listener.set_redirects(False)
    assert_false(listener.redirect_chain, "set_redirects should alias set_redirect_chain")
    listener.set_collect_redirects(True)
    assert_true(listener.redirect_chain, "set_collect_redirects should alias set_redirect_chain")
    listener.redirect_chain = False
    assert_false(listener.redirect_chain, "redirect_chain property setter should update the mode")

    listener.set_urls("/keep")
    listener.start(None, redirect_chain=True)
    assert_equal(listener.urls, {"/keep"},
                 "start without urls should preserve existing URL filters")


def _check_basic_chain_order_and_headers():
    owner, listener = _make_listener("/final")
    listener._requestWillBeSent(**_request(
        "chain-1", "https://host.test/start", "GET",
        {"X-Hop": "1", "X-Origin": "start"}, timestamp=100.0,
    ))
    listener._requestWillBeSent(
        **{
            **_request(
                "chain-1", "https://host.test/middle", "POST",
                {"X-Hop": "2", "X-Origin": "middle"}, timestamp=102.0,
                redirect=_redirect_response("https://host.test/start", 301,
                                            {"X-Hop": "1", "X-Origin": "start"}),
            ),
        }
    )
    # Response extra for the first hop arrives after the second request event.
    listener._responseReceivedExtraInfo(
        requestId="chain-1",
        statusCode=301,
        headers={"X-Hop": "1", "X-Resp-Extra": "first"},
    )
    listener._requestWillBeSent(**_request(
        "chain-1", "https://host.test/final?from=redirect", "POST",
        {"X-Hop": "3", "X-Origin": "final"}, timestamp=104.0,
        redirect=_redirect_response("https://host.test/middle", 307,
                                    {"X-Hop": "2", "X-Origin": "middle"}),
    ))
    listener._requestWillBeSentExtraInfo(
        requestId="chain-1",
        headers={"X-Hop": "3", "X-Req-Extra": "final"},
        associatedCookies=[],
    )
    listener._response_received(
        requestId="chain-1", type="Fetch", timestamp=106.0, hasExtraInfo=True,
        response=_response("https://host.test/final?from=redirect", 200,
                           {"X-Hop": "3", "X-Origin": "final"}),
    )
    # The final response extra is deliberately held until after completion.
    owner.bodies["chain-1"] = {"body": '{"result": "final"}', "base64Encoded": False}
    listener._loading_finished(requestId="chain-1", timestamp=107.0)

    assert_equal(listener._caught.qsize(), 1,
                 "a three-hop chain should enqueue exactly one final packet")
    packet = listener.wait(timeout=0.01)
    assert_true(packet is not None and packet is not False, "the final packet should be enqueued")
    assert_equal(packet.url, "https://host.test/final?from=redirect",
                 "DataPacket.url should describe the final hop")
    assert_equal(packet.method, "POST", "DataPacket.method should describe the final hop")
    assert_equal(packet.target, ("/final", "POST", "Fetch"),
                 "the packet target should reflect the matched final filter")
    assert_equal(packet.redirect_count, 2, "the packet should expose two prior hops")
    assert_equal(len(packet.hops), 3, "hops should include prior hops plus the final hop")
    assert_true(all(isinstance(hop, RedirectHop) for hop in packet.hops),
                "redirect chains should expose RedirectHop objects")

    first, middle, final = packet.hops
    assert_equal([hop.url for hop in packet.hops],
                 ["https://host.test/start", "https://host.test/middle",
                  "https://host.test/final?from=redirect"],
                 "redirect hops should remain in request order")
    assert_equal([hop.order for hop in packet.hops], [0, 1, 2],
                 "redirect hops should retain their chain order")
    assert_equal(first.status, 301, "the first hop should expose its redirect status")
    assert_equal(first.response_headers["X-Hop"], "1",
                 "the first hop should expose its redirect response headers")
    assert_equal(first.response_headers["X-Resp-Extra"], "first",
                 "the first hop should receive its own late response ExtraInfo")
    assert_equal(first.request_headers["X-Origin"], "start",
                 "each hop should expose its own request headers")
    assert_equal(middle.status, 307, "the middle hop should expose its redirect status")
    assert_equal(middle.response_headers["X-Hop"], "2",
                 "the middle hop should expose its redirect response headers")
    assert_equal(middle.request_headers["X-Origin"], "middle",
                 "the middle hop should expose its own request headers")
    assert_equal(final.status, 200, "the final hop should expose the response status")
    assert_equal(final.response_headers["X-Hop"], "3",
                 "the final hop should expose the final response headers")
    assert_equal(final.request_headers["X-Req-Extra"], "final",
                 "the final packet should expose the final hop's request ExtraInfo")

    assert_equal(packet.request.headers["X-Hop"], "3",
                 "DataPacket.request should represent the final hop only")
    assert_equal(packet.request.headers["X-Req-Extra"], "final",
                 "final request headers should include final-hop ExtraInfo")
    assert_equal(packet.request.headers["X-Origin"], "final",
                 "the final packet request must not inherit intermediate headers")
    assert_equal(packet.response.headers["X-Hop"], "3",
                 "DataPacket.response should represent the final hop only")
    assert_equal(packet.response.body, {"result": "final"},
                 "only the final hop should expose a parsed response body")
    assert_equal(first.body, None, "intermediate hops must not fetch a response body")
    assert_equal(middle.body, None, "intermediate hops must not fetch a response body")

    body_calls = [call for call in owner.cdp_calls if call[0] == "Network.getResponseBody"]
    assert_equal(body_calls, [("Network.getResponseBody", {"requestId": "chain-1", "_ignore": True})],
                 "only the final hop should request the response body")

    # The response extra arrives after the packet was already delivered. It must
    # still attach to the final hop and refresh cached headers.
    second_before = packet.response.headers["X-Hop"]
    listener._responseReceivedExtraInfo(
        requestId="chain-1", statusCode=200,
        headers={"X-Hop": "3", "X-Resp-Extra": "final"},
    )
    assert_equal(packet.response.headers["X-Hop"], second_before,
                 "late final ExtraInfo must not change the authoritative final headers")
    assert_equal(packet.response.headers["X-Resp-Extra"], "final",
                 "late final ExtraInfo should attach to the final hop")
    assert_true(packet.wait_extra_info(timeout=0),
                "wait_extra_info should see the late final response ExtraInfo")
    assert_equal(listener._caught.qsize(), 0, "late ExtraInfo must not enqueue another packet")


def _check_final_hop_filtering():
    # Intermediate URL matches, final URL does not: the chain must not enqueue.
    owner, listener = _make_listener("/api")
    listener._requestWillBeSent(**_request("filter-1", "https://host.test/api/start",
                                           "GET", {"X-Hop": "1"}))
    listener._requestWillBeSent(**_request(
        "filter-1", "https://host.test/login", "POST", {"X-Hop": "2"},
        redirect=_redirect_response("https://host.test/api/start", 302, {"X-Hop": "1"}),
    ))
    listener._loading_failed(requestId="filter-1", type="Fetch", timestamp=101.0,
                             errorText="net::ERR_ABORTED")
    assert_equal(listener._caught.qsize(), 0,
                 "matching only an intermediate hop must not enqueue a packet")
    assert_equal(listener._running_requests, 0,
                 "filtered-out chains should still release their request accounting")

    # Final URL matches, intermediate does not: exactly one packet is enqueued.
    listener._requestWillBeSent(**_request("filter-2", "https://host.test/start",
                                           "GET", {"X-Hop": "1"}))
    listener._requestWillBeSent(**_request(
        "filter-2", "https://host.test/api/final", "POST", {"X-Hop": "2"},
        redirect=_redirect_response("https://host.test/start", 302, {"X-Hop": "1"}),
    ))
    listener._response_received(
        requestId="filter-2", type="Fetch", timestamp=103.0,
        response=_response("https://host.test/api/final", 200, {"X-Hop": "2"}),
    )
    owner.bodies["filter-2"] = {"body": '{"ok": true}', "base64Encoded": False}
    listener._loading_finished(requestId="filter-2")
    assert_equal(listener._caught.qsize(), 1,
                 "a matching final hop must enqueue exactly one packet")
    packet = listener.wait(timeout=0.01)
    assert_equal(packet.url, "https://host.test/api/final",
                 "the enqueued packet should describe the matching final hop")
    assert_equal(packet.redirects[0].url, "https://host.test/start",
                 "unmatched intermediate hops should still be retained")

    # Method and resource-type filters apply to the final hop.
    listener.set_method.GET(only=True)
    listener._requestWillBeSent(**_request("filter-3", "https://host.test/api/one", "GET"))
    listener._requestWillBeSent(**_request(
        "filter-3", "https://host.test/api/two", "POST",
        redirect=_redirect_response("https://host.test/api/one", 302, {}),
    ))
    owner.bodies["filter-3"] = {"body": "{}", "base64Encoded": False}
    listener._response_received(requestId="filter-3", type="Fetch", timestamp=105.0,
                                response=_response("https://host.test/api/two"))
    listener._loading_finished(requestId="filter-3")
    assert_equal(listener._caught.qsize(), 0,
                 "the method filter should be evaluated against the final hop")
    listener.set_method.all()

    listener.set_res_type.XHR(only=True)
    listener._requestWillBeSent(**_request("filter-4", "https://host.test/api/three", "GET"))
    listener._requestWillBeSent(**_request(
        "filter-4", "https://host.test/api/four", "GET",
        redirect=_redirect_response("https://host.test/api/three", 302, {}),
    ))
    owner.bodies["filter-4"] = {"body": "{}", "base64Encoded": False}
    listener._response_received(requestId="filter-4", type="Fetch", timestamp=107.0,
                                response=_response("https://host.test/api/four"))
    listener._loading_finished(requestId="filter-4")
    assert_equal(listener._caught.qsize(), 0,
                 "the resource-type filter should be evaluated against the final hop")
    listener.set_res_type.all()


def _check_extra_info_early_late_and_absent():
    owner, listener = _make_listener("/extra")

    # Early request and response ExtraInfo arrive before any request event.
    listener._requestWillBeSentExtraInfo(
        requestId="early-1", headers={"X-Hop": "1", "X-Req": "early"}, associatedCookies=[]
    )
    listener._responseReceivedExtraInfo(
        requestId="early-1", statusCode=302, headers={"X-Hop": "1", "X-Resp": "early"}
    )
    listener._requestWillBeSent(**_request(
        "early-1", "https://host.test/extra/start", "GET", {"X-Hop": "1"},
        extra_info=True,
    ))
    listener._requestWillBeSent(**_request(
        "early-1", "https://host.test/extra/final", "GET", {"X-Hop": "2"},
        redirect=_redirect_response("https://host.test/extra/start", 302, {"X-Hop": "1"}),
    ))
    listener._response_received(
        requestId="early-1", type="Fetch", timestamp=103.0,
        response=_response("https://host.test/extra/final", 200, {"X-Hop": "2"}),
    )
    owner.bodies["early-1"] = {"body": '{"early": true}', "base64Encoded": False}
    listener._loading_finished(requestId="early-1")
    packet = listener.wait(timeout=0.01)
    assert_equal(packet.hops[0].request_headers["X-Req"], "early",
                 "early request ExtraInfo should attach to the first hop")
    assert_equal(packet.hops[0].response_headers["X-Resp"], "early",
                 "early response ExtraInfo should attach to the first hop")
    assert_false("X-Req" in packet.request.headers,
                 "first-hop request ExtraInfo must not leak into the final packet")

    # Early ExtraInfo for the *final* hop can arrive before its request or
    # response event. It must wait for the final hop and not attach to hop 1.
    owner, listener = _make_listener("/early-final")
    listener._requestWillBeSent(**_request(
        "early-final-1", "https://host.test/early-final/start", "GET",
        {"X-Hop": "1"}, extra_info=False,
    ))
    listener._requestWillBeSentExtraInfo(
        requestId="early-final-1",
        headers={"X-Hop": "2", "X-Early-Req": "final"},
        associatedCookies=[],
    )
    listener._responseReceivedExtraInfo(
        requestId="early-final-1", statusCode=200,
        headers={"X-Hop": "2", "X-Early-Resp": "final"},
    )
    listener._requestWillBeSent(**_request(
        "early-final-1", "https://host.test/early-final/final", "GET",
        {"X-Hop": "2"}, extra_info=True, redirect_extra_info=False,
        redirect=_redirect_response("https://host.test/early-final/start", 302,
                                    {"X-Hop": "1"}),
    ))
    listener._response_received(
        requestId="early-final-1", type="Fetch", timestamp=107.0, hasExtraInfo=True,
        response=_response("https://host.test/early-final/final", 200, {"X-Hop": "2"}),
    )
    owner.bodies["early-final-1"] = {"body": '{"early_final": true}', "base64Encoded": False}
    listener._loading_finished(requestId="early-final-1")
    packet = listener.wait(timeout=0.01)
    assert_equal(packet.request.headers["X-Early-Req"], "final",
                 "early final request ExtraInfo should attach to the final hop")
    assert_equal(packet.response.headers["X-Early-Resp"], "final",
                 "early final response ExtraInfo should attach to the final hop")
    assert_equal(packet.redirects[0].request_headers["X-Hop"], "1",
                 "early final ExtraInfo must not attach to the first hop")
    assert_equal(packet.redirects[0].response_headers["X-Hop"], "1",
                 "early final ExtraInfo must not attach to the first hop response")
    assert_false("X-Early-Req" in packet.redirects[0].request_headers,
                 "early final request ExtraInfo must not leak into the first hop")

    # Absent ExtraInfo must not delay enqueueing or wait_extra_info forever.
    owner, listener = _make_listener("/absent")
    listener._requestWillBeSent(**_request(
        "absent-1", "https://host.test/absent/start", "GET", {"X-Hop": "1"},
        extra_info=False,
    ))
    listener._requestWillBeSent(**_request(
        "absent-1", "https://host.test/absent/final", "GET", {"X-Hop": "2"},
        redirect=_redirect_response("https://host.test/absent/start", 302, {"X-Hop": "1"}),
        extra_info=False, redirect_extra_info=False,
    ))
    owner.bodies["absent-1"] = {"body": '{"absent": true}', "base64Encoded": False}
    listener._response_received(
        requestId="absent-1", type="Fetch", timestamp=103.0, hasExtraInfo=False,
        response=_response("https://host.test/absent/final", 200, {"X-Hop": "2"}),
    )
    assert_equal(listener._running_requests, 1,
                 "request accounting should not depend on ExtraInfo events")
    listener._loading_finished(requestId="absent-1")
    assert_equal(listener._caught.qsize(), 1,
                 "absent ExtraInfo must not block final-packet enqueueing")
    packet = listener.wait(timeout=0.01)
    assert_equal(packet.response.body, {"absent": True},
                 "the final body should still be available without ExtraInfo")
    assert_false(packet.wait_extra_info(timeout=0.01),
                 "wait_extra_info should return False rather than hang on absent ExtraInfo")
    assert_equal(packet.hops[0].request_extra_info.all_info, {},
                 "missing intermediate request ExtraInfo should expose an empty mapping")
    assert_equal(packet.hops[0].response_extra_info.all_info, {},
                 "missing intermediate response ExtraInfo should expose an empty mapping")

    # Request extra after the final packet, plus a late previously-missing hop extra.
    listener._requestWillBeSentExtraInfo(
        requestId="absent-1", headers={"X-Hop": "2", "X-Late": "final"}, associatedCookies=[]
    )
    listener._responseReceivedExtraInfo(
        requestId="absent-1", statusCode=200,
        headers={"X-Hop": "2", "X-Late-Resp": "final"},
    )
    assert_equal(packet.request.headers["X-Late"], "final",
                 "late request ExtraInfo should attach to the final packet")
    assert_equal(packet.response.headers["X-Late-Resp"], "final",
                 "late response ExtraInfo should attach to the final packet")
    assert_true(packet.wait_extra_info(timeout=0),
                "wait_extra_info should see a late response ExtraInfo after absence")


def _check_concurrent_chains_and_same_request_id():
    owner, listener = _make_listener("/concurrent")
    owner.bodies["conc-1"] = {"body": '{"chain": 1}', "base64Encoded": False}
    owner.bodies["conc-2"] = {"body": '{"chain": 2}', "base64Encoded": False}

    listener._requestWillBeSent(**_request("conc-1", "https://host.test/concurrent/start-1",
                                           "GET", {"X-Chain": "1", "X-Hop": "1"}))
    listener._requestWillBeSent(**_request("conc-2", "https://host.test/concurrent/start-2",
                                           "GET", {"X-Chain": "2", "X-Hop": "1"}))
    listener._requestWillBeSentExtraInfo(
        requestId="conc-1", headers={"X-Chain": "1", "X-Req": "1-a"}, associatedCookies=[]
    )
    listener._requestWillBeSentExtraInfo(
        requestId="conc-2", headers={"X-Chain": "2", "X-Req": "2-a"}, associatedCookies=[]
    )
    listener._requestWillBeSent(**_request(
        "conc-1", "https://host.test/concurrent/final-1", "POST",
        {"X-Chain": "1", "X-Hop": "2"},
        redirect=_redirect_response("https://host.test/concurrent/start-1", 302,
                                    {"X-Chain": "1", "X-Hop": "1"}),
    ))
    listener._requestWillBeSent(**_request(
        "conc-2", "https://host.test/concurrent/final-2", "POST",
        {"X-Chain": "2", "X-Hop": "2"},
        redirect=_redirect_response("https://host.test/concurrent/start-2", 302,
                                    {"X-Chain": "2", "X-Hop": "1"}),
    ))
    listener._response_received(
        requestId="conc-1", type="Fetch", timestamp=105.0,
        response=_response("https://host.test/concurrent/final-1", 200, {"X-Chain": "1"}),
    )
    listener._response_received(
        requestId="conc-2", type="Fetch", timestamp=106.0,
        response=_response("https://host.test/concurrent/final-2", 200, {"X-Chain": "2"}),
    )
    listener._responseReceivedExtraInfo(
        requestId="conc-2", statusCode=200, headers={"X-Chain": "2", "X-Resp": "2-b"}
    )
    listener._responseReceivedExtraInfo(
        requestId="conc-1", statusCode=200, headers={"X-Chain": "1", "X-Resp": "1-b"}
    )
    listener._loading_finished(requestId="conc-1")
    listener._loading_finished(requestId="conc-2")

    assert_equal(listener._caught.qsize(), 2,
                 "two concurrent chains should enqueue exactly two final packets")
    p1, p2 = listener.wait(count=2, timeout=0.01)
    assert_equal(p1.url, "https://host.test/concurrent/final-1",
                 "the first concurrent chain should keep its own final URL")
    assert_equal(p2.url, "https://host.test/concurrent/final-2",
                 "the second concurrent chain should keep its own final URL")
    assert_equal(p1.request.headers["X-Chain"], "1",
                 "concurrent request headers must not cross chains")
    assert_equal(p2.request.headers["X-Chain"], "2",
                 "concurrent request headers must not cross chains")
    assert_equal(p1.redirects[0].request_headers["X-Req"], "1-a",
                 "concurrent ExtraInfo must attach by requestId")
    assert_equal(p2.redirects[0].request_headers["X-Req"], "2-a",
                 "concurrent ExtraInfo must attach by requestId")
    assert_not_in("X-Req", p1.request.headers,
                  "first-hop request ExtraInfo must not leak into the final packet")
    assert_not_in("X-Req", p2.request.headers,
                  "first-hop request ExtraInfo must not leak into the final packet")
    assert_equal(p1.response.headers["X-Resp"], "1-b",
                 "concurrent response ExtraInfo must attach by requestId")
    assert_equal(p2.response.headers["X-Resp"], "2-b",
                 "concurrent response ExtraInfo must attach by requestId")
    assert_equal(p1.response.body, {"chain": 1}, "each concurrent chain should fetch its own body")
    assert_equal(p2.response.body, {"chain": 2}, "each concurrent chain should fetch its own body")

    body_ids = [call[1].get("requestId") for call in owner.cdp_calls
                if call[0] == "Network.getResponseBody"]
    assert_equal(sorted(body_ids), ["conc-1", "conc-2"],
                 "each chain should fetch exactly its own final body")


def _check_failed_chain():
    owner, listener = _make_listener("/fail")
    listener._requestWillBeSent(**_request(
        "fail-1", "https://host.test/fail/start", "GET", {"X-Hop": "1"},
    ))
    listener._requestWillBeSent(**_request(
        "fail-1", "https://host.test/fail/final", "POST", {"X-Hop": "2"},
        redirect=_redirect_response("https://host.test/fail/start", 302, {"X-Hop": "1"}),
    ))
    listener._loading_failed(
        requestId="fail-1", type="Fetch", timestamp=102.0,
        errorText="net::ERR_CONNECTION_RESET", canceled=False,
    )
    assert_equal(listener._caught.qsize(), 1,
                 "a failed matching chain should still enqueue its final packet")
    packet = listener.wait(timeout=0.01)
    assert_true(packet.is_failed, "failed chains should mark the final packet")
    assert_equal(packet.fail_info.errorText, "net::ERR_CONNECTION_RESET",
                 "failed chains should retain the failure reason")
    assert_equal(packet.url, "https://host.test/fail/final",
                 "failed chains should retain the final hop URL")
    assert_equal(packet.redirect_count, 1,
                 "failed chains should retain hops collected before the failure")
    assert_equal(packet.redirects[0].status, 302,
                 "failed chains should retain prior redirect responses")
    assert_equal(packet.response.body, None,
                 "failed final hops should not expose a body")
    assert_equal([call for call in owner.cdp_calls if call[0] == "Network.getResponseBody"], [],
                 "failed chains must not try to fetch a response body")


def _check_wait_silent_and_lifecycle():
    owner, listener = _make_listener("/silent")
    listener._requestWillBeSent(**_request("silent-1", "https://host.test/silent/start",
                                           "GET", {"X-Hop": "1"}))
    listener._requestWillBeSent(**_request(
        "silent-1", "https://host.test/silent/final", "GET", {"X-Hop": "2"},
        redirect=_redirect_response("https://host.test/silent/start", 302, {"X-Hop": "1"}),
    ))
    assert_equal(listener._running_requests, 1,
                 "an unterminated chain should count as one active request")
    assert_false(listener.wait_silent(timeout=0.001),
                 "wait_silent must not report silence while a chain is still open")
    # ExtraInfo absence must not keep the chain alive.
    owner.bodies["silent-1"] = {"body": "{}", "base64Encoded": False}
    listener._loading_finished(requestId="silent-1")
    assert_true(listener.wait_silent(timeout=0.01),
                "wait_silent should recover once the final hop finishes")
    assert_equal(listener._running_requests, 0, "finished chains should release request accounting")
    assert_equal(listener._running_targets, 0, "finished chains should release target accounting")
    assert_equal(listener._caught.qsize(), 1, "silent completion should still enqueue exactly one packet")

    # Old-round events must not attach to a new round, even with a reused id.
    owner, listener = _make_listener("/round")
    listener._requestWillBeSent(**_request("round-1", "https://host.test/round/start",
                                           "GET", {"X-Round": "old", "X-Hop": "1"}))
    listener._requestWillBeSentExtraInfo(requestId="round-1",
                                         headers={"X-Round": "old"}, associatedCookies=[])
    listener.clear()
    listener._requestWillBeSentExtraInfo(requestId="round-1",
                                        headers={"X-Round": "stale"}, associatedCookies=[])
    listener._responseReceivedExtraInfo(requestId="round-1", statusCode=302,
                                        headers={"X-Round": "stale"})
    assert_equal(listener._chains, {}, "clear should discard in-flight chain state")
    assert_equal(listener._running_requests, 0, "clear should reset request accounting")

    listener._requestWillBeSent(**_request("round-1", "https://host.test/round/start",
                                           "GET", {"X-Round": "new", "X-Hop": "1"}))
    listener._requestWillBeSent(**_request(
        "round-1", "https://host.test/round/final", "GET",
        {"X-Round": "new", "X-Hop": "2", "X-Req-Hint": "final"},
        redirect=_redirect_response("https://host.test/round/start", 302,
                                    {"X-Round": "new", "X-Hop": "1"}),
    ))
    listener._requestWillBeSentExtraInfo(requestId="round-1",
                                         headers={"X-Round": "new", "X-Hop": "2",
                                                  "X-New": "yes"},
                                         associatedCookies=[])
    owner.bodies["round-1"] = {"body": '{"round": "new"}', "base64Encoded": False}
    listener._response_received(
        requestId="round-1", type="Fetch", timestamp=103.0,
        response=_response("https://host.test/round/final", 200, {"X-Round": "new"}),
    )
    listener._loading_finished(requestId="round-1")
    packet = listener.wait(timeout=0.01)
    assert_equal(packet.hops[0].request_headers["X-Round"], "new",
                 "stale ExtraInfo from a cleared round must not attach to the new chain")
    assert_equal(packet.request.headers["X-Round"], "new",
                 "the new round should only expose its own final-hop data")
    assert_equal(packet.request.headers["X-New"], "yes",
                 "fresh ExtraInfo should attach to the new round")
    assert_not_in("stale", packet.request.headers.values(),
                  "stale ExtraInfo values must not leak into a new round")

    listener._requestWillBeSent(**_request("round-open", "https://host.test/round/open"))
    listener.stop()
    assert_false(listener.listening, "stop should leave listening state")
    assert_equal(listener._chains, {}, "stop should clear chain state")
    assert_equal(listener._running_requests, 0, "stop should reset request accounting")
    assert_true(listener._caught.empty(), "stop should clear queued packets")


def _check_non_http_and_preserved_configuration():
    owner, listener = _make_listener("/events", redirect_chain=True,
                                     res_type="EventSource")
    listener._requestWillBeSent(**_request(
        "sse-1", "https://host.test/events", "GET", type_="EventSource",
    ))
    listener._eventSourceMessageReceived(
        requestId="sse-1", timestamp=101.0, eventName="message", eventId="1", data="ready"
    )
    assert_equal(listener._chains, {}, "non-HTTP listeners must not create redirect chains")
    event = listener.wait(timeout=0.01)
    assert_true(isinstance(event, SSEPacket),
                "EventSource packets should keep their normal listener behavior")
    assert_equal(event.url, "https://host.test/events",
                 "non-HTTP packets should retain their normal request metadata")
    assert_equal(event.data, "ready", "non-HTTP packets should retain their normal payload")
    assert_equal(listener._running_requests, 1,
                 "an unfinished EventSource request should keep normal request accounting")
    listener._loading_finished_sse(requestId="sse-1")
    assert_equal(listener._running_requests, 0,
                 "SSE completion should release normal request accounting")

    # Non-target non-HTTP events remain silent for wait_silent(targets_only=True).
    owner, listener = _make_listener("/only-target", redirect_chain=True,
                                     res_type="WebSocket")
    listener._webSocketCreated(requestId="ws-1", url="wss://host.test/socket")
    assert_equal(listener._chains, {}, "WebSocket listeners must not create redirect chains")
    assert_true(listener.wait_silent(timeout=0.001, targets_only=True),
                "WebSocket-only listeners should retain their silence behavior")
    listener._webSocketClosed(requestId="ws-1")
    assert_true(listener.wait_silent(timeout=0.001),
                "closing a WebSocket should retain silence")

    # wait()/steps() continue to work with chain mode and identity filters.
    owner, listener = _make_listener("/strict", redirect_chain=True, method="POST",
                                     res_type="Fetch")
    listener.set_urls(["/strict"], is_regex=False)
    listener._requestWillBeSent(**_request("strict-1", "https://host.test/strict/one",
                                           "GET", {"X-Hop": "1"}))
    listener._requestWillBeSent(**_request(
        "strict-1", "https://host.test/strict/two", "POST", {"X-Hop": "2"},
        redirect=_redirect_response("https://host.test/strict/one", 302, {"X-Hop": "1"}),
    ))
    owner.bodies["strict-1"] = {"body": '{"strict": true}', "base64Encoded": False}
    listener._response_received(requestId="strict-1", type="Fetch", timestamp=102.0,
                                response=_response("https://host.test/strict/two"))
    listener._loading_finished(requestId="strict-1")
    assert_true(listener.wait_silent(timeout=0.01),
                "filtered chain completion should restore silence")
    packet = next(listener.steps(count=1, timeout=0.1))
    assert_equal(packet.url, "https://host.test/strict/two",
                 "steps() should return the chained final packet")
    assert_equal(packet.method, "POST",
                 "method filters should still apply to the final hop")


if __name__ == "__main__":
    from support import TestContext
    try:
        run(TestContext(include_browser=False))
    except TestFailure as exc:
        print(f"FAIL: {exc}")
        raise SystemExit(1)
    print("listener_redirect_chain PASS")
