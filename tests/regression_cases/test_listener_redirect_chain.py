# -*- coding: utf-8 -*-
"""Regression: redirect-chain collection in a real Chromium tab and frame."""
from __future__ import annotations

import json

from support import (
    TestCase,
    TestContext,
    TestFailure,
    assert_equal,
    assert_false,
    assert_true,
    chromium,
    html,
    local_server,
    wait_for_packet,
)
import time


def _wait_listener_packet(listener, wanted, *, timeout, desc):
    end = time.perf_counter() + timeout
    seen = []
    while time.perf_counter() < end:
        remain = max(0.05, min(0.5, end - time.perf_counter()))
        packet = listener.wait(timeout=remain, fit_count=False)
        if packet is False:
            continue
        packets = packet if isinstance(packet, list) else [packet]
        for item in packets:
            seen.append(getattr(item, "type", type(item).__name__))
            if wanted(item):
                return item
    raise TestFailure(f"No {desc} before timeout; seen={seen}")


def _redirect(location, status=302, hop="redirect"):
    return status, "text/plain; charset=utf-8", "", {
        "Location": location,
        "X-Redirect-Hop": hop,
    }


def _json_final(data, hop="final"):
    return 200, "application/json", json.dumps(data), {
        "X-Redirect-Hop": hop,
    }


def run(ctx: TestContext) -> None:
    routes = {
        "/page/chain": lambda req: html("""
            <body>
              <iframe id="chain-frame" src="/frame/page"></iframe>
            </body>
            """, title="redirect chain"),
        "/redirect/start": lambda req: _redirect("/redirect/middle", 302, "one"),
        "/redirect/middle": lambda req: _redirect("/redirect/final", 307, "two"),
        "/redirect/final": lambda req: _json_final({"final": True, "via": "document"}, "three"),
        "/page/fetch": lambda req: html("""
            <body><script>
              fetch('/redirect/api-start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json', 'X-Client': 'chrome'},
                body: JSON.stringify({from: 'browser'})
              }).then(r => r.text()).catch(() => {});
            </script></body>
            """, title="redirect fetch"),
        "/redirect/api-start": lambda req: _redirect("/redirect/api-final", 307, "post-one"),
        "/redirect/api-final": lambda req: _json_final({"api": True}, "post-final"),
        "/frame/page": lambda req: html("<body><h1 id='frame-title'>frame</h1></body>",
                                        title="frame page"),
        "/frame/start": lambda req: _redirect("/frame/final", 302, "frame-one"),
        "/frame/final": lambda req: _json_final({"frame": True}, "frame-final"),
    }

    with local_server(routes) as base, chromium(ctx) as browser:
        tab = browser.latest_tab

        # --- tab.listen: document navigation through a 302 -> 307 chain ---
        tab.listen.set_urls("/redirect/final")
        tab.listen.start(redirect_chain=True)
        assert_true(tab.listen.redirect_chain,
                    "tab.listen.start(redirect_chain=True) should enable chain mode")
        tab.get(base + "/redirect/start")
        packet = wait_for_packet(tab, lambda p: getattr(p, "url", "").endswith("/redirect/final"),
                                 timeout=ctx.timeout, desc="tab redirect-chain packet")
        try:
            assert_equal(packet.redirect_count, 2,
                         "tab listener should retain the two prior redirect hops")
            assert_equal([hop.url for hop in packet.hops],
                         [base + "/redirect/start", base + "/redirect/middle",
                          base + "/redirect/final"],
                         "tab listener redirect hops should remain in order")
            assert_equal([hop.status for hop in packet.hops], [302, 307, 200],
                         "tab listener should expose per-hop status codes")
            assert_equal([hop.response_headers["x-redirect-hop"] for hop in packet.hops],
                         ["one", "two", "three"],
                         "tab listener should expose per-hop response headers")
            assert_equal([hop.method for hop in packet.hops], ["GET", "GET", "GET"],
                         "tab listener should expose each redirect hop method")
            assert_equal(packet.request.headers.get("X-Redirect-Hop"), None,
                         "the final document request should not inherit redirect headers")
            assert_equal(packet.response.body, {"final": True, "via": "document"},
                         "only the final redirect hop should expose the document body")
            assert_equal(packet.redirects[0].body, None,
                         "prior redirect hops should not expose bodies")
            assert_true(tab.listen.wait_silent(timeout=ctx.timeout),
                        "tab wait_silent should not hang on redirects or ExtraInfo")
            assert_equal(tab.listen._caught.qsize(), 0,
                         "one chain should enqueue exactly one final packet")
        finally:
            tab.listen.stop()

        # --- tab.listen: 307 preserves POST, final method/resource filters ---
        tab.listen.set_urls("/redirect/api-final")
        tab.listen.set_method.POST(only=True)
        tab.listen.set_res_type.Fetch(only=True)
        tab.listen.start(redirect_chain=True)
        try:
            tab.get(base + "/page/fetch")
            api_packet = wait_for_packet(
                tab, lambda p: getattr(p, "url", "").endswith("/redirect/api-final"),
                timeout=ctx.timeout, desc="POST redirect-chain packet")
            assert_equal(api_packet.method, "POST",
                         "the final packet should use the final hop's preserved POST method")
            assert_equal(api_packet.redirect_count, 1,
                         "the POST chain should retain its intermediate 307 hop")
            assert_equal(api_packet.redirects[0].status, 307,
                         "the intermediate POST hop should expose its 307 status")
            assert_equal(api_packet.redirects[0].request_headers["X-Client"], "chrome",
                         "each POST hop should expose its own request headers")
            assert_equal(api_packet.request.headers["X-Client"], "chrome",
                         "final request headers should include the browser request header")
            assert_equal(api_packet.response.body, {"api": True},
                         "the POST chain should expose only the final JSON body")
            assert_true(tab.listen.wait_silent(timeout=ctx.timeout),
                        "POST redirect chain should reach silence")
        finally:
            tab.listen.set_method.all()
            tab.listen.set_res_type.all()
            tab.listen.stop()

        # --- frame.listen: redirect chain triggered inside an iframe ---
        tab.get(base + "/page/chain")
        frame = tab.get_frame("#chain-frame", timeout=ctx.timeout)
        assert_true(frame, "the redirect-chain test iframe should be obtainable")
        frame.listen.set_urls("/frame/final")
        frame.listen.start(redirect_chain=True)
        try:
            frame.run_js("fetch('/frame/start').then(r => r.text()).catch(() => {})")
            frame_packet = _wait_listener_packet(
                frame.listen, lambda p: getattr(p, "url", "").endswith("/frame/final"),
                timeout=ctx.timeout, desc="frame redirect-chain packet")
            assert_equal(frame_packet.redirect_count, 1,
                         "frame listener should retain the prior redirect hop")
            assert_equal(frame_packet.redirects[0].url, base + "/frame/start",
                         "frame listener redirect hop URL mismatch")
            assert_equal(frame_packet.redirects[0].status, 302,
                         "frame listener redirect hop status mismatch")
            assert_equal(frame_packet.redirects[0].response_headers["x-redirect-hop"],
                         "frame-one",
                         "frame listener redirect hop headers mismatch")
            assert_equal(frame_packet.response.body, {"frame": True},
                         "frame listener should expose only the final body")
            assert_true(frame.listen.wait_silent(timeout=ctx.timeout),
                        "frame wait_silent should not hang on redirects or ExtraInfo")
            assert_equal(frame.listen._caught.qsize(), 0,
                         "the frame chain should enqueue exactly one final packet")
        finally:
            frame.listen.stop()

        # --- default-off compatibility in a real browser ---
        tab.listen.set_urls("/redirect/final")
        tab.listen.start(redirect_chain=False)
        try:
            tab.get(base + "/redirect/start")
            classic = wait_for_packet(
                tab, lambda p: getattr(p, "url", "").endswith("/redirect/final"),
                timeout=ctx.timeout, desc="classic redirect packet")
            assert_equal(classic.redirects, [],
                         "default-off tab.listen should not expose redirect hops")
            assert_false(tab.listen.redirect_chain,
                         "default-off tab.listen should keep chain mode disabled")
            assert_equal(classic.response.body, {"final": True, "via": "document"},
                         "default-off listener should keep the classic final body")
        finally:
            tab.listen.stop()


TEST_CASE = TestCase(
    name="listener_redirect_chain",
    title="Redirect-chain collection in real Chromium tab.listen and frame.listen",
    requires_browser=True,
    features=("listener_redirect_chain_browser",),
    run=run,
)


if __name__ == "__main__":
    import os
    path = os.environ.get("DRISSIONPAGE_BROWSER_PATH")
    run(TestContext(browser_path=path, include_browser=True, timeout=10.0))
    print("listener_redirect_chain browser PASS")
