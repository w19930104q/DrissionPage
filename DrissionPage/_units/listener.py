# -*- coding:utf-8 -*-
"""
@Author   : g1879
@Contact  : g1879@qq.com
@Website  : https://DrissionPage.cn
@Copyright: (c) 2020 by g1879, Inc. All Rights Reserved.
"""
from base64 import b64decode
from json import JSONDecodeError, loads
from queue import Queue
from re import search
from threading import RLock
from time import perf_counter, sleep

from ftfy import fix_text
from requests.structures import CaseInsensitiveDict

from .._functions.settings import Settings as _S
from .._functions.tools import wait_until
from ..errors import PageDisconnectedError, WaitTimeoutError

# CDP resource types that are not regular HTTP responses. They keep the
# original listener behaviour even when redirect-chain collection is enabled.
_NON_HTTP_TYPES = frozenset(('EventSource', 'WebSocket'))


class BaseListener(object):
    def __init__(self, owner):
        self._owner = owner
        self.listening = False

        self._urls = True
        self._is_regex = False
        self._method = {'GET', 'POST'}
        self._res_type = True

        self._method_setter = None
        self._res_type_setter = None

    @property
    def urls(self):
        return self._urls

    @property
    def set_method(self):
        if self._method_setter is None:
            self._method_setter = MethodSetter(self)
        return self._method_setter

    @property
    def set_res_type(self):
        if self._res_type_setter is None:
            self._res_type_setter = ResTypeSetter(self)
        return self._res_type_setter

    def set_urls(self, urls=True, is_regex=False):
        if urls is None:
            self._is_regex = is_regex
            return
        if not isinstance(urls, (str, list, tuple, set)) and urls is not True:
            raise ValueError(_S._lang.joinn(_S._lang.INCORRECT_TYPE_, 'urls',
                                            ALLOW_TYPE='str, list, tuple, set, True', CURR_TYPE=type(urls)))
        if urls is True:
            self._urls = True
        else:
            self._urls = {urls} if isinstance(urls, str) else set(urls)

        self._is_regex = is_regex

    def resume(self):
        self._init_callback()
        self.listening = True


class Listener(BaseListener):
    def __init__(self, owner):
        super().__init__(owner)
        self._caught = None
        self._running_requests = 0
        self._running_targets = 0

        self._request_ids = {}
        self._extra_info_ids = {}
        self._ws_info = {}

        # Redirect-chain collection is opt-in per listener. State is kept
        # separately from the classic single-packet dictionaries so a redirect
        # chain can never be confused with a plain request sharing a requestId.
        self._redirect_chain = False
        self._chains = {}
        self._active_request_ids = set()
        self._active_target_ids = set()
        self._stale_request_ids = set()
        self._orphan_request_extras = {}
        self._orphan_response_extras = {}
        self._chain_lock = RLock()

        self.tab_id = None

    def start(self, urls=None, is_regex=False, redirect_chain=None, redirects=None):
        if redirect_chain is None and redirects is not None:
            redirect_chain = redirects
        if redirect_chain is not None:
            self.set_redirect_chain(redirect_chain)
        if urls is not None:
            self.set_urls(urls, is_regex)
        self.clear()
        if not self.listening:
            self.resume()
            self._owner._enable_domain('Network')

    @property
    def redirect_chain(self):
        """是否采集重定向链（默认关闭）。

        :return: bool
        """
        return self._redirect_chain

    @redirect_chain.setter
    def redirect_chain(self, value):
        self.set_redirect_chain(value)

    def set_redirect_chain(self, on=True):
        """开启或关闭重定向链采集模式。

        该设置可在 start() 前调用。开启后，每条重定向链只入队一个终包，
        终包的 url/method/request/response 等字段表示最后一次跳转；前面的
        跳转保存在 packet.redirects 中，按发生顺序排列。

        :param on: True 开启，False 关闭
        :return: 监听对象本身，便于链式调用
        """
        self._redirect_chain = True if on is True else bool(on)
        return self

    def set_redirects(self, on=True):
        """set_redirect_chain() 的别名。"""
        return self.set_redirect_chain(on)

    def set_collect_redirects(self, on=True):
        """set_redirect_chain() 的别名。"""
        return self.set_redirect_chain(on)

    def wait(self, count=1, timeout=None, fit_count=True, raise_err=None):
        if not self.listening:
            self.start()
        success = False
        if not timeout:
            while self._owner._messenger_running and self.listening and self._caught.qsize() < count:
                sleep(.01)
            if not self._owner._messenger_running:
                raise PageDisconnectedError
            success = True

        else:
            end = perf_counter() + timeout
            while self._owner._messenger_running and self.listening and perf_counter() < end:
                if self._caught.qsize() >= count:
                    success = True
                    break
                sleep(.01)
            if not self._owner._messenger_running:
                raise PageDisconnectedError

        if success:
            if count == 1:
                return self._caught.get_nowait()
            return [self._caught.get_nowait() for _ in range(count)]

        if fit_count or not self._caught.qsize():
            if raise_err is True or (_S.raise_when_wait_failed is True and raise_err is None):
                raise WaitTimeoutError(_S._lang.WAITING_FAILED_, _S._lang.DATA_PACKET, timeout)
            else:
                return False
        else:
            return [self._caught.get_nowait() for _ in range(self._caught.qsize())]

    def browser_wait(self, count=1, timeout=None, fit_count=True, raise_err=None):
        if not self._owner.browser.listen.listening:
            self._owner.browser.listen.start()
        success = False

        if not timeout:
            while (self._owner.browser.listen.listening and self._owner._messenger_running and
                   (self._owner.tab_id not in self._owner.browser.listen._caught or
                    self._owner.browser.listen._caught[self._owner.tab_id].qsize() < count)):
                sleep(.01)
            if not self._owner._messenger_running:
                raise PageDisconnectedError
            success = True

        else:
            end = perf_counter() + timeout
            while self._owner.browser.listen.listening and self._owner._messenger_running and perf_counter() < end:
                if (self._owner.tab_id in self._owner.browser.listen._caught and
                        self._owner.browser.listen._caught[self._owner.tab_id].qsize() >= count):
                    success = True
                    break
                sleep(.01)
            if not self._owner._messenger_running:
                raise PageDisconnectedError

        if success:
            if count == 1:
                return self._owner.browser.listen._caught[self._owner.tab_id].get_nowait()
            return [self._owner.browser.listen._caught[self._owner.tab_id].get_nowait() for _ in range(count)]

        if fit_count or not self._owner.browser.listen._caught[self._owner.tab_id].qsize():
            if raise_err is True or (_S.raise_when_wait_failed is True and raise_err is None):
                raise WaitTimeoutError(_S._lang.WAITING_FAILED_, _S._lang.DATA_PACKET, timeout)
            else:
                return False
        else:
            return [self._owner.browser.listen._caught[self._owner.tab_id].get_nowait()
                    for _ in range(self._owner.browser.listen._caught[self._owner.tab_id].qsize())]

    def steps(self, count=None, timeout=None, gap=1):
        if not self.listening:
            self.start()
        caught = 0
        if timeout is None:
            while self._owner._messenger_running and self.listening:
                if self._caught.qsize() >= gap:
                    yield self._caught.get_nowait() if gap == 1 else [self._caught.get_nowait() for _ in range(gap)]
                    if count:
                        caught += gap
                        if caught >= count:
                            return None
                sleep(.01)
            if not self._owner._messenger_running:
                raise PageDisconnectedError

        else:
            end = perf_counter() + timeout
            while self._owner._messenger_running and self.listening and perf_counter() < end:
                if self._caught.qsize() >= gap:
                    yield self._caught.get_nowait() if gap == 1 else [self._caught.get_nowait() for _ in range(gap)]
                    end = perf_counter() + timeout
                    if count:
                        caught += gap
                        if caught >= count:
                            return None
                sleep(.01)
            if not self._owner._messenger_running:
                raise PageDisconnectedError
            return False

    def browser_steps(self, count=None, timeout=None, gap=1):
        if not self._owner.browser.listen.listening:
            self._owner.browser.listen.start()
        caught = 0
        _caught = self._owner.browser.listen._caught
        tid = self._owner.tab_id
        if timeout is None:
            while self._owner.browser.listen.listening and self._owner._messenger_running:
                if tid in _caught and _caught[tid].qsize() >= gap:
                    yield _caught[tid].get_nowait() if gap == 1 else [_caught[tid].get_nowait() for _ in range(gap)]
                    if count:
                        caught += gap
                        if caught >= count:
                            return None
                sleep(.01)
            if not self._owner._messenger_running:
                raise PageDisconnectedError

        else:
            end = perf_counter() + timeout
            while self._owner.browser.listen.listening and self._owner._messenger_running and perf_counter() < end:
                if tid in _caught and _caught[tid].qsize() >= gap:
                    yield _caught[tid].get_nowait() if gap == 1 else [_caught[tid].get_nowait() for _ in range(gap)]
                    end = perf_counter() + timeout
                    if count:
                        caught += gap
                        if caught >= count:
                            return None
                sleep(.01)
            if not self._owner._messenger_running:
                raise PageDisconnectedError
            return False

    def stop(self):
        if self.listening:
            self.pause(clear=True)
        self._owner._disable_domain('Network')

    def pause(self, clear=True):
        if self.listening:
            self._owner._set_callback('Network.requestWillBeSent', None)
            self._owner._set_callback('Network.responseReceived', None)
            self._owner._set_callback('Network.loadingFinished', None)
            self._owner._set_callback('Network.loadingFailed', None)
            self._owner._set_callback('Network.requestWillBeSentExtraInfo', None)
            self._owner._set_callback('Network.responseReceivedExtraInfo', None)
            self._owner._set_callback('Network.webSocketFrameSent', None)
            self._owner._set_callback('Network.webSocketFrameReceived', None)
            self._owner._set_callback('Network.webSocketCreated', None)
            self._owner._set_callback('Network.webSocketHandshakeResponseReceived', None)
            self._owner._set_callback('Network.webSocketWillSendHandshakeRequest', None)
            self._owner._set_callback('Network.webSocketClosed', None)
            self._owner._set_callback('Network.eventSourceMessageReceived', None)
            self.listening = False
        if clear:
            self.clear()

    def clear(self):
        with self._chain_lock:
            # Remember which requestIds belonged to the previous round. Late
            # ExtraInfo events from that round must not attach to a new chain
            # that happens to reuse the same requestId.
            stale = set(self._active_request_ids)
            stale.update(self._chains)
            stale.update(self._orphan_request_extras)
            stale.update(self._orphan_response_extras)
            if self._request_ids:
                stale.update(self._request_ids)
            if stale:
                self._stale_request_ids.update(stale)
                if len(self._stale_request_ids) > 4096:
                    self._stale_request_ids = set()

            self._request_ids = {}
            self._extra_info_ids = {}
            self._ws_info = {}
            self._chains = {}
            self._active_request_ids = set()
            self._active_target_ids = set()
            self._orphan_request_extras = {}
            self._orphan_response_extras = {}
            self._caught = Queue(maxsize=0)
            self._running_requests = 0
            self._running_targets = 0

    def wait_silent(self, timeout=None, targets_only=False, limit=0):
        if not self.listening:
            raise RuntimeError(_S._lang.joinn(_S._lang.NOT_LISTENING))
        if timeout is None:
            while ((not targets_only and self._running_requests > limit)
                   or (targets_only and self._running_targets > limit)):
                sleep(.01)
            return True

        end_time = perf_counter() + timeout
        while perf_counter() < end_time:
            if ((not targets_only and self._running_requests <= limit)
                    or (targets_only and self._running_targets <= limit)):
                return True
            sleep(.01)
        else:
            return False

    def _to_target(self, target_id, owner):
        self._target_id = target_id
        self._owner = owner
        if self.listening:
            self.stop()
            self.start()

    def _init_callback(self):
        if self._res_type is True or self._res_type - {'WebSocket'}:
            self._owner._set_callback('Network.requestWillBeSent', self._requestWillBeSent)
            self._owner._set_callback('Network.responseReceived', self._response_received)
            self._owner._set_callback('Network.requestWillBeSentExtraInfo', self._requestWillBeSentExtraInfo)
            self._owner._set_callback('Network.responseReceivedExtraInfo', self._responseReceivedExtraInfo)
            if self._res_type in ({'EventSource'}, {'EventSource', 'WebSocket'}):
                self._owner._set_callback('Network.loadingFinished', self._loading_finished_sse)
                self._owner._set_callback('Network.loadingFailed', self._loading_failed_sse)
            else:
                self._owner._set_callback('Network.loadingFinished', self._loading_finished)
                self._owner._set_callback('Network.loadingFailed', self._loading_failed)
        if self._res_type is True or 'WebSocket' in self._res_type:
            self._owner._set_callback('Network.webSocketFrameSent', self._webSocketFrameSent)
            self._owner._set_callback('Network.webSocketFrameReceived', self._webSocketFrameReceived)
            self._owner._set_callback('Network.webSocketCreated', self._webSocketCreated)
            self._owner._set_callback('Network.webSocketHandshakeResponseReceived',
                                      self._webSocketHandshakeResponseReceived)
            self._owner._set_callback('Network.webSocketWillSendHandshakeRequest',
                                      self._webSocketWillSendHandshakeRequest)
            self._owner._set_callback('Network.webSocketClosed', self._webSocketClosed)
        if self._res_type is True or 'EventSource' in self._res_type:
            self._owner._set_callback('Network.eventSourceMessageReceived', self._eventSourceMessageReceived)

    def _eventSourceMessageReceived(self, **kwargs):
        i = self._request_ids.get(kwargs['requestId'])
        if self._urls is True or i:
            p = SSEPacket(self._owner, i.target if i else None, kwargs)
            if i:
                p._connect_info = i
            self._caught.put(p)

    def _webSocketFrameSent(self, **kwargs):
        i = self._ws_info.get(kwargs['requestId'])
        if self._urls is True or i:
            p = WebSocketPacket(self._owner, i.target if i else None, kwargs, True)
            if i:
                p._connect_info = i
            self._caught.put(p)

    def _webSocketFrameReceived(self, **kwargs):
        i = self._ws_info.get(kwargs['requestId'])
        if self._urls is True or i:
            p = WebSocketPacket(self._owner, i.target if i else None, kwargs, False)
            if i:
                p._connect_info = i
            self._caught.put(p)

    def _webSocketCreated(self, **kwargs):
        target = in_targets(self, kwargs['url'], 'GET', 'WebSocket')
        if target:
            self._ws_info[kwargs['requestId']] = WebSocketConnectInfo(self._owner, target, kwargs['requestId'],
                                                                      kwargs['url'], kwargs.get('initiator'))

    def _webSocketClosed(self, **kwargs):
        self._ws_info.pop(kwargs['requestId'], None)

    def _webSocketHandshakeResponseReceived(self, **kwargs):
        i = self._ws_info.get(kwargs['requestId'])
        if i:
            i.response = kwargs

    def _webSocketWillSendHandshakeRequest(self, **kwargs):
        i = self._ws_info.get(kwargs['requestId'])
        if i:
            i.request = kwargs

    # ---------- 请求计数 ----------
    def _start_request(self, request_id):
        self._stale_request_ids.discard(request_id)
        if request_id not in self._active_request_ids:
            self._active_request_ids.add(request_id)
            self._running_requests += 1

    def _mark_target(self, request_id):
        if request_id not in self._active_target_ids:
            self._active_target_ids.add(request_id)
            self._running_targets += 1

    def _finish_request(self, request_id):
        active = getattr(self, '_active_request_ids', None)
        if active is not None and request_id in active:
            active.discard(request_id)
            self._running_requests = max(0, self._running_requests - 1)
        targets = getattr(self, '_active_target_ids', None)
        if targets is not None and request_id in targets:
            targets.discard(request_id)
            self._running_targets = max(0, getattr(self, '_running_targets', 0) - 1)

    def _http_chain_active(self):
        if not self._redirect_chain:
            return False
        if self._res_type is True:
            return True
        return bool(self._res_type - _NON_HTTP_TYPES)

    def _requestWillBeSent(self, **kwargs):
        rid = kwargs['requestId']
        request = kwargs['request']
        target = in_targets(self, request['url'], request['method'], kwargs.get('type', ''))

        with self._chain_lock:
            self._start_request(rid)
            if target:
                self._mark_target(rid)

            if self._http_chain_active() and kwargs.get('type', '') not in _NON_HTTP_TYPES:
                self._chain_requestWillBeSent(kwargs)
            else:
                self._normal_requestWillBeSent(kwargs, target)

    def _normal_requestWillBeSent(self, kwargs, target):
        rid = kwargs['requestId']
        p = False
        if target:
            p = self._request_ids.setdefault(rid, DataPacket(self._owner, target))
            p._raw_request = kwargs
            if kwargs['request'].get('hasPostData') and not kwargs['request'].get('postDataEntries'):
                p._raw_post_data = self._owner._run_cdp('Network.getRequestPostData',
                                                        requestId=rid, _ignore=True).get('postData')
        entry = self._extra_info_ids.setdefault(rid, {})
        entry['obj'] = p

        # ExtraInfo can arrive before requestWillBeSent. Reuse it when it does.
        for extra in self._orphan_request_extras.pop(rid, ()):
            entry['request'] = extra
        for extra in self._orphan_response_extras.pop(rid, ()):
            entry['response'] = extra
            if isinstance(p, DataPacket):
                p._responseExtraInfo = extra

    def _requestWillBeSentExtraInfo(self, **kwargs):
        rid = kwargs['requestId']
        with self._chain_lock:
            chain = self._chains.get(rid)
            if chain is not None:
                self._chain_add_request_extra(chain, kwargs)
                return
            if rid in self._stale_request_ids and rid not in self._request_ids:
                return
            if self._http_chain_active() and rid not in self._request_ids:
                self._orphan_request_extras.setdefault(rid, []).append(kwargs)
                self._prune_orphans(self._orphan_request_extras)
                return
            self._extra_info_ids.setdefault(rid, {})['request'] = kwargs

    def _response_received(self, **kwargs):
        rid = kwargs['requestId']
        with self._chain_lock:
            chain = self._chains.get(rid)
            if chain is not None and rid not in self._request_ids:
                self._chain_response_received(chain, kwargs)
                return
            request = self._request_ids.get(rid)
            if request:
                request._raw_response = kwargs['response']
                request._resource_type = kwargs['type']
                request.timestamp = kwargs['timestamp']

    def _responseReceivedExtraInfo(self, **kwargs):
        rid = kwargs['requestId']
        with self._chain_lock:
            chain = self._chains.get(rid)
            if chain is not None and rid not in self._request_ids:
                self._chain_add_response_extra(chain, kwargs)
                return
            if rid in self._stale_request_ids and rid not in self._request_ids:
                return
            if self._http_chain_active() and rid not in self._request_ids:
                self._orphan_response_extras.setdefault(rid, []).append(kwargs)
                self._prune_orphans(self._orphan_response_extras)
                return
            r = self._extra_info_ids.get(rid)
            if r:
                obj = r.get('obj')
                if isinstance(obj, DataPacket):
                    obj._responseExtraInfo = kwargs
                    r['response'] = kwargs
                else:
                    r['response'] = kwargs

    # ---------- 重定向链 ----------
    def _chain_requestWillBeSent(self, kwargs):
        rid = kwargs['requestId']
        chain = self._chains.get(rid)
        if chain is not None and chain.done:
            self._evict_chains()
            chain = None
        if chain is None:
            chain = _RedirectChain(rid)
            self._chains[rid] = chain
            # ExtraInfo may have arrived before this first request event.
            for extra in self._orphan_request_extras.pop(rid, ()):
                chain.pending_request_extras.append(extra)
            for extra in self._orphan_response_extras.pop(rid, ()):
                chain.pending_response_extras.append(extra)

        index = len(chain.hops)
        hop = RedirectHop(rid, kwargs, index, kwargs.get('type'))
        chain.hops.append(hop)

        if index:
            prev = chain.hops[-2]
            redirect_response = kwargs.get('redirectResponse')
            if redirect_response is not None:
                prev.set_response(redirect_response)
            if 'redirectHasExtraInfo' in kwargs:
                prev._response_extra_expected = kwargs['redirectHasExtraInfo']
                if kwargs['redirectHasExtraInfo'] is False:
                    prev._response_extra_absent = True
            self._add_response_extra_waiting(chain, prev)

        if 'hasExtraInfo' in kwargs:
            hop._request_extra_expected = kwargs['hasExtraInfo']
            if kwargs['hasExtraInfo'] is False:
                hop._request_extra_absent = True

        # Keep absent hops available for late ExtraInfo. "Absent" only means
        # the final packet must not wait for it; it does not mean a genuinely
        # late event must be discarded.
        if not hop._request_extra_paired:
            chain.waiting_request_hops.append(hop)
        if hop._raw_response is not None:
            self._add_response_extra_waiting(chain, hop)

        self._pair_request_extras(chain)
        self._pair_response_extras(chain)

    def _chain_response_received(self, chain, kwargs):
        response = kwargs.get('response') or {}
        url = response.get('url')
        hop = None
        for h in reversed(chain.hops):
            if h._raw_response is None and (url is None or h.url == url):
                hop = h
                break
        if hop is None:
            for h in reversed(chain.hops):
                if url is not None and h.url == url:
                    hop = h
                    break
        if hop is None and chain.hops:
            hop = chain.hops[-1]
        if hop is None:
            return

        hop.set_response(response, kwargs.get('timestamp'))
        if 'type' in kwargs:
            hop._resource_type = kwargs['type']
        if 'hasExtraInfo' in kwargs:
            hop._response_extra_expected = kwargs['hasExtraInfo']
            if kwargs['hasExtraInfo'] is False:
                hop._response_extra_absent = True
        self._add_response_extra_waiting(chain, hop)
        self._pair_response_extras(chain)

    def _chain_add_request_extra(self, chain, extra):
        chain.pending_request_extras.append(extra)
        self._pair_request_extras(chain)

    def _chain_add_response_extra(self, chain, extra):
        chain.pending_response_extras.append(extra)
        self._pair_response_extras(chain)

    def _add_response_extra_waiting(self, chain, hop):
        if hop._response_extra_paired:
            return
        if hop._response_extra_expected is False:
            hop._response_extra_absent = True
        # As with request ExtraInfo, an absent flag only prevents the final
        # packet from waiting; the hop stays eligible for a late event.
        if hop not in chain.waiting_response_hops:
            chain.waiting_response_hops.append(hop)

    def _pair_request_extras(self, chain, final=False):
        while chain.pending_request_extras and chain.waiting_request_hops:
            best = None
            best_score = -1
            for extra in chain.pending_request_extras:
                for hop in chain.waiting_request_hops:
                    score = _request_extra_score(extra, hop)
                    if score > best_score:
                        best_score = score
                        best = (extra, hop)
            if best_score < 0:
                break
            if (best_score == 0 and not final
                    and not (len(chain.pending_request_extras) == 1
                             and len(chain.waiting_request_hops) == 1)):
                break
            if best is None:
                break
            extra, hop = best
            hop.set_request_extra(extra)
            chain.pending_request_extras.remove(extra)
            chain.waiting_request_hops.remove(hop)

        if final:
            # Do not wait for missing ExtraInfo. Unmatched waiting hops are
            # simply reported as absent; a genuinely late event can still be
            # attached because the hop stays in the waiting list.
            for hop in chain.waiting_request_hops:
                hop._request_extra_absent = True

    def _pair_response_extras(self, chain, final=False):
        while chain.pending_response_extras and chain.waiting_response_hops:
            best = None
            best_score = -1
            for extra in chain.pending_response_extras:
                for hop in chain.waiting_response_hops:
                    score = _response_extra_score(extra, hop)
                    if score > best_score:
                        best_score = score
                        best = (extra, hop)
            if best_score < 0:
                break
            if (best_score == 0 and not final
                    and not (len(chain.pending_response_extras) == 1
                             and len(chain.waiting_response_hops) == 1)):
                break
            if best is None:
                break
            extra, hop = best
            hop.set_response_extra(extra)
            chain.pending_response_extras.remove(extra)
            chain.waiting_response_hops.remove(hop)

        if final:
            for hop in chain.waiting_response_hops:
                hop._response_extra_absent = True

    def _loading_finished_sse(self, **kwargs):
        rid = kwargs['requestId']
        self._finish_request(rid)
        packet = self._request_ids.get(rid)
        if packet:
            r = self._owner._run_cdp('Network.getResponseBody', requestId=rid, _ignore=True)
            if 'body' in r:
                packet._raw_body = r['body']
                packet._base64_body = r.get('base64Encoded', False)
            else:
                packet._raw_body = ''
                packet._base64_body = False

            if (packet._raw_request['request'].get('hasPostData')
                    and not packet._raw_request['request'].get('postDataEntries')):
                r = self._owner._run_cdp('Network.getRequestPostData', requestId=rid, _timeout=1, _ignore=True)
                packet._raw_post_data = r.get('postData')

        self._merge_normal_extra(rid)
        self._request_ids.pop(rid, None)
        return packet

    def _loading_failed_sse(self, **kwargs):
        rid = kwargs['requestId']
        self._finish_request(rid)
        packet = self._request_ids.get(rid)
        if packet:
            packet._raw_fail_info = kwargs
            packet._resource_type = kwargs['type']
            packet.is_failed = True

        self._merge_normal_extra(rid)
        self._request_ids.pop(rid, None)
        return packet

    def _merge_normal_extra(self, rid):
        r = self._extra_info_ids.get(rid)
        if not r:
            return
        obj = r.get('obj')
        if isinstance(obj, DataPacket):
            request_extra = r.get('request')
            response_extra = r.get('response')
            if request_extra is not None:
                obj._requestExtraInfo = request_extra
            if response_extra is not None:
                obj._responseExtraInfo = response_extra
        self._extra_info_ids.pop(rid, None)

    def _loading_finished(self, **kwargs):
        rid = kwargs['requestId']
        with self._chain_lock:
            chain = self._chains.get(rid)
            if chain is not None and rid not in self._request_ids:
                self._finish_request(rid)
                packet = self._finish_chain(chain, kwargs, failed=False)
            else:
                packet = self._loading_finished_sse(**kwargs)
            if packet:
                self._caught.put(packet)

    def _loading_failed(self, **kwargs):
        rid = kwargs['requestId']
        with self._chain_lock:
            chain = self._chains.get(rid)
            if chain is not None and rid not in self._request_ids:
                self._finish_request(rid)
                packet = self._finish_chain(chain, kwargs, failed=True)
            else:
                packet = self._loading_failed_sse(**kwargs)
            if packet:
                self._caught.put(packet)

    def _finish_chain(self, chain, kwargs, failed):
        if chain.done:
            return None
        chain.done = True
        if not chain.hops:
            return None

        hop = chain.hops[-1]
        if failed:
            hop.is_failed = True
            hop._raw_fail_info = kwargs
        if kwargs.get('type'):
            hop._resource_type = kwargs['type']

        # Do not wait for missing ExtraInfo. Whatever arrived is already
        # attached; the rest is treated as absent.
        self._pair_request_extras(chain, final=True)
        self._pair_response_extras(chain, final=True)

        target = in_targets(self, hop.url, hop.method, hop._resource_type or '')
        if not target:
            return None

        packet = DataPacket(self._owner, target, redirects=chain.hops[:-1])
        packet._raw_request = hop._raw_request
        packet._raw_post_data = hop._raw_post_data
        packet._raw_response = hop._raw_response
        packet._raw_body = hop._raw_body
        packet._base64_body = hop._base64_body
        packet._raw_fail_info = hop._raw_fail_info
        packet._resource_type = hop._resource_type
        packet.timestamp = hop.timestamp
        packet.is_failed = failed
        packet._final_hop = hop
        hop._packet = packet

        if not failed:
            self._fetch_final_body(rid=chain.request_id, hop=hop, packet=packet)

        chain.final_packet = packet
        return packet

    def _fetch_final_body(self, rid, hop, packet):
        """Only the final hop of a chain owns a response body."""
        result = self._owner._run_cdp('Network.getResponseBody', requestId=rid, _ignore=True)
        if 'body' in result:
            hop._raw_body = result['body']
            hop._base64_body = result.get('base64Encoded', False)
        else:
            hop._raw_body = ''
            hop._base64_body = False
        packet._raw_body = hop._raw_body
        packet._base64_body = hop._base64_body

        request = hop._raw_request.get('request', {})
        if request.get('hasPostData') and not request.get('postDataEntries'):
            result = self._owner._run_cdp('Network.getRequestPostData', requestId=rid, _timeout=1, _ignore=True)
            hop._raw_post_data = result.get('postData')
            packet._raw_post_data = hop._raw_post_data

    def _evict_chains(self):
        if len(self._chains) <= 512:
            return
        for rid in list(self._chains):
            if self._chains[rid].done:
                self._chains.pop(rid, None)
                if len(self._chains) <= 256:
                    return

    def _prune_orphans(self, store):
        while len(store) > 256:
            store.pop(next(iter(store)), None)


class BrowserListener(BaseListener):
    def __init__(self, owner):
        super().__init__(owner)
        self._caught = {}
        self._request_ids = {}

    def start(self, urls=None, is_regex=False):
        if urls is not None:
            self.set_urls(urls, is_regex)
        self.clear()
        if not self.listening:
            self.resume()
            if self._urls is True or self._is_regex:
                if self._res_type is True:
                    patterns = [{'requestStage': 'Response'}]
                else:
                    patterns = []
                    for m in self._res_type:
                        patterns.append({'resourceType': m, 'requestStage': 'Response'})
            else:
                patterns = []
                if self._res_type is True:
                    for t in self._urls:
                        patterns.append({'urlPattern': f'*{t}*', 'requestStage': 'Response'})
                else:
                    for t in self._urls:
                        p = {'urlPattern': f'*{t}*', 'requestStage': 'Response'}
                        for m in self._res_type:
                            p['resourceType'] = m
                            patterns.append(p)

            self._owner._enable_domain('Fetch', patterns=patterns)

    def stop(self):
        if self.listening:
            self.pause()
        self._owner._disable_domain('Fetch')

    def pause(self, clear=True):
        if self.listening:
            self._owner._set_callback('Fetch.requestPaused', None)
            self.listening = False
        if clear:
            self.clear()

    def clear(self):
        self._caught = {}
        self._request_ids = {}

    def _init_callback(self):
        self._owner._set_callback('Fetch.requestPaused', self._onRequestPaused)

    def _onRequestPaused(self, **kwargs):
        target = is_target(self, kwargs['request']['url'], kwargs['request']['method'], kwargs['resourceType'])
        if not target:
            self._owner._run_cdp('Fetch.continueResponse', requestId=kwargs['requestId'], _ignore=True, _timeout=0)
            return

        tab_id = self._owner._tabs._frames.get(kwargs['frameId'])
        times = 0
        while not tab_id and times < 50:
            tab_id = self._owner._tabs._frames.get(kwargs['frameId'])
            sleep(.001)
            times += 1

        if tab_id:
            body = self._owner._run_cdp('Fetch.getResponseBody', requestId=kwargs['requestId'], _ignore=True)
            kwargs['body'] = None if 'error' in body else body
            self._owner._run_cdp('Fetch.continueResponse', requestId=kwargs['requestId'], _ignore=True, _timeout=0)
            self._caught.setdefault(tab_id, Queue(maxsize=0)).put(BrowserDataPacket(tab_id, target, kwargs))


def is_target(listener, url, method, res_type):
    if listener._urls is True:
        if ((listener._method is True or method in listener._method)
                and (listener._res_type is True or res_type in listener._res_type)):
            return listener._urls, listener._method, listener._res_type
    else:
        for u in listener._urls:
            if (((listener._is_regex and search(u, url)) or (not listener._is_regex and u in url))
                    and (listener._method is True or method in listener._method)
                    and (listener._res_type is True or res_type in listener._res_type)):
                return listener._urls, listener._method, listener._res_type
    return False


def in_targets(listener, url, method, res_type):
    """返回url是否命中监听目标，命中时返回 (匹配的url目标, method, res_type)。

    与内部 is_target() 不同，这里返回真正命中的单个 url 特征，便于外部判断
    是哪一个过滤条件生效。
    """
    if listener._method is not True and method not in listener._method:
        return False
    if listener._res_type is not True and res_type not in listener._res_type:
        return False
    if listener._urls is True:
        return True, method, res_type
    for u in listener._urls:
        if ((listener._is_regex and search(u, url)) or (not listener._is_regex and u in url)):
            return u, method, res_type
    return False


def _normalised_headers(headers):
    if not headers:
        return {}
    return {str(k).lower(): str(v) for k, v in headers.items()}


def _headers_similarity(left, right):
    a = _normalised_headers(left)
    b = _normalised_headers(right)
    if not a or not b:
        return 0
    score = 0
    for name in set(a) & set(b):
        # A differing value for the same header usually means the extra info
        # belongs to a different hop (e.g. a hop-specific marker header).
        score += 2 if a[name] == b[name] else -1
    return score


def _request_extra_score(extra, hop):
    raw_request = hop._raw_request.get('request', {}) if hop._raw_request else {}
    return _headers_similarity(extra.get('headers'), raw_request.get('headers'))


def _response_extra_score(extra, hop):
    score = _headers_similarity(extra.get('headers'), hop._raw_response.get('headers')
                                if hop._raw_response else None)
    extra_status = extra.get('statusCode')
    hop_status = hop.status
    if extra_status is not None and hop_status is not None:
        try:
            score += 20 if int(extra_status) == int(hop_status) else -20
        except (TypeError, ValueError):
            pass
    return score


class _RedirectChain(object):
    """Internal state for one requestId's redirect chain."""

    def __init__(self, request_id):
        self.request_id = request_id
        self.hops = []
        self.done = False
        self.final_packet = None
        self.pending_request_extras = []
        self.pending_response_extras = []
        self.waiting_request_hops = []
        self.waiting_response_hops = []


class FrameListener(Listener):
    def _requestWillBeSent(self, **kwargs):
        if not self._owner._is_diff_domain and kwargs.get('frameId') != self._owner._frame_id:
            return
        super()._requestWillBeSent(**kwargs)

    def _response_received(self, **kwargs):
        if (not self._owner._is_diff_domain and kwargs.get('frameId') != self._owner._frame_id
                and kwargs.get('requestId') not in self._chains):
            return
        super()._response_received(**kwargs)


class DataPacket(object):
    type = 'DataPacket'

    def __init__(self, tab, target, redirects=None):
        self.tab = tab
        self.tab_id = tab.tab_id
        self.target = target
        self.is_failed = False
        self.timestamp = None

        self._raw_request = None
        self._raw_post_data = None
        self._raw_response = None
        self._raw_body = None
        self._raw_fail_info = None

        self._request = None
        self._response = None
        self._fail_info = None

        self._base64_body = False
        self._requestExtraInfo = None
        self._responseExtraInfo = None
        self._resource_type = None

        # When redirect-chain collection is enabled, earlier hops are kept in
        # order. The final hop is represented by the normal DataPacket fields.
        self._redirects = list(redirects) if redirects else []
        self._final_hop = None

    def __repr__(self):
        t = f'"{self.target}"' if self.target is not True else True
        return f'<DataPacket target={t} url="{self.url}">'

    @property
    def _request_extra_info(self):
        if self._final_hop is not None:
            return self._final_hop._requestExtraInfo
        return self._requestExtraInfo

    @property
    def _response_extra_info(self):
        if self._final_hop is not None:
            return self._final_hop._responseExtraInfo
        return self._responseExtraInfo

    @property
    def redirects(self):
        """重定向链中位于终跳之前的各跳，按发生顺序排列。"""
        return self._redirects

    @property
    def redirect_count(self):
        """终跳之前的跳转数量。"""
        return len(self._redirects)

    @property
    def hops(self):
        """整条链的所有跳，最后一项为终跳（若已终结）。"""
        return self._redirects + ([self._final_hop] if self._final_hop is not None else [])

    @property
    def url(self):
        return self.request.url

    @property
    def method(self):
        return self.request.method

    @property
    def frameId(self):
        return self._raw_request.get('frameId')

    @property
    def resourceType(self):
        return self._resource_type

    @property
    def request(self):
        if self._request is None:
            self._request = Request(self, self._raw_request['request'], self._raw_post_data)
        return self._request

    @property
    def response(self):
        if self._response is None:
            self._response = Response(self, self._raw_response, self._raw_body, self._base64_body, self.timestamp)
        return self._response

    @property
    def data(self):
        return self.response.body

    @property
    def fail_info(self):
        if self._fail_info is None:
            self._fail_info = FailInfo(self, self._raw_fail_info)
        return self._fail_info

    def wait_extra_info(self, timeout=None):
        def do():
            if not self.tab._messenger_running or not self.tab.listen.listening:
                return False
            return None if self._response_extra_info is None else True

        if timeout is None:
            timeout = self.tab.timeout
        return wait_until(do, timeout=timeout)


class RedirectHop(object):
    """重定向链中的一跳。

    request/response/status/extra_info 等访问方式与 DataPacket 保持一致。
    只有链的终跳会取响应正文；中间跳的 response.body 为 None。
    """
    type = 'RedirectHop'

    def __init__(self, request_id, raw_request, order=0, resource_type=None):
        self.request_id = request_id
        self.order = order
        self._raw_request = raw_request
        self._raw_post_data = None
        self._raw_response = None
        self._raw_body = None
        self._raw_fail_info = None
        self._base64_body = False
        self._requestExtraInfo = None
        self._responseExtraInfo = None
        self._resource_type = resource_type
        self.timestamp = raw_request.get('timestamp')

        self._request = None
        self._response = None
        self._fail_info = None
        self._packet = None

        self.is_failed = False
        self._request_extra_paired = False
        self._response_extra_paired = False
        self._request_extra_expected = None
        self._response_extra_expected = None
        self._request_extra_absent = False
        self._response_extra_absent = False

    def __repr__(self):
        return f'<RedirectHop {self.order} url="{self.url}" status={self.status}>'

    def __getitem__(self, item):
        if item in ('status_code', 'statusCode'):
            item = 'status'
        return getattr(self, item)

    def set_response(self, raw_response, timestamp=None):
        if raw_response is None:
            return
        self._raw_response = raw_response
        if timestamp is not None:
            self.timestamp = timestamp
        self._response = None
        if self._packet is not None and self._packet._response is not None:
            self._packet._response = None

    def set_request_extra(self, extra):
        self._requestExtraInfo = extra
        self._request_extra_paired = True
        self._request_extra_absent = False
        if self._request is not None:
            self._request._headers = None
        if self._packet is not None and self._packet._request is not None:
            self._packet._request._headers = None

    def set_response_extra(self, extra):
        self._responseExtraInfo = extra
        self._response_extra_paired = True
        self._response_extra_absent = False
        if self._response is not None:
            self._response._headers = None
        if self._packet is not None and self._packet._response is not None:
            self._packet._response._headers = None

    def as_dict(self):
        return {
            'request_id': self.request_id,
            'order': self.order,
            'url': self.url,
            'method': self.method,
            'status': self.status,
            'resource_type': self._resource_type,
            'request': self._raw_request,
            'response': self._raw_response,
            'request_extra_info': self._requestExtraInfo,
            'response_extra_info': self._responseExtraInfo,
            'is_failed': self.is_failed,
        }

    @property
    def url(self):
        return self._raw_request['request'].get('url')

    @property
    def method(self):
        return self._raw_request['request'].get('method')

    @property
    def frameId(self):
        return (self._raw_request or {}).get('frameId')

    @property
    def resourceType(self):
        return self._resource_type

    @property
    def resource_type(self):
        return self._resource_type

    @property
    def status(self):
        return self._raw_response.get('status') if self._raw_response else None

    @property
    def status_code(self):
        return self.status

    @property
    def statusCode(self):
        return self.status

    @property
    def request(self):
        if self._request is None:
            self._request = Request(self, self._raw_request['request'], self._raw_post_data)
        return self._request

    @property
    def response(self):
        if self._response is None:
            self._response = Response(self, self._raw_response, self._raw_body,
                                      self._base64_body, self.timestamp)
        return self._response

    @property
    def request_extra_info(self):
        return RequestExtraInfo(self._requestExtraInfo or {})

    @property
    def response_extra_info(self):
        return ResponseExtraInfo(self._responseExtraInfo or {})

    @property
    def requestExtraInfo(self):
        return self.request_extra_info

    @property
    def responseExtraInfo(self):
        return self.response_extra_info

    @property
    def request_headers(self):
        return self.request.headers

    @property
    def response_headers(self):
        return self.response.headers

    @property
    def headers(self):
        return self.response.headers if self._raw_response is not None else self.request.headers

    @property
    def body(self):
        return self.response.body

    @property
    def data(self):
        return self.response.body

    @property
    def fail_info(self):
        if self._fail_info is None:
            self._fail_info = FailInfo(self, self._raw_fail_info)
        return self._fail_info

    @property
    def raw_request(self):
        return self._raw_request

    @property
    def raw_response(self):
        return self._raw_response

    @property
    def _request_extra_info(self):
        return self._requestExtraInfo

    @property
    def _response_extra_info(self):
        return self._responseExtraInfo


class Request(object):
    def __init__(self, data_packet, raw_request, post_data):
        self._data_packet = data_packet
        self._request = raw_request
        self._raw_post_data = post_data
        self._postData = None
        self._headers = None

    def __getattr__(self, item):
        return self._request.get(item)

    @property
    def headers(self):
        if self._headers is None:
            self._headers = CaseInsensitiveDict(self._request['headers'])
            if self.extra_info.headers:
                h = CaseInsensitiveDict(self.extra_info.headers)
                for k, v in h.items():
                    if k not in self._headers:
                        self._headers[k] = v
        return self._headers

    @property
    def params(self):
        from urllib.parse import parse_qsl, urlparse
        return dict(parse_qsl(urlparse(self.url).query, keep_blank_values=True))

    @property
    def postData(self):
        if self._postData is None:
            if self._raw_post_data:
                try:
                    self._postData = loads(self._raw_post_data)
                except (JSONDecodeError, TypeError):
                    self._postData = self._raw_post_data
            elif self._request.get('postDataEntries'):
                if len(self._request['postDataEntries']) == 1:
                    self._postData = loads(b64decode(self._request['postDataEntries'][0]['bytes']).decode())
                else:
                    self._postData = [loads(b64decode(d['bytes']).decode()) for d in self._request['postDataEntries']]
            else:
                self._postData = False
        return self._postData

    @property
    def cookies(self):
        return [c['cookie'] for c in self.extra_info.associatedCookies if not c['blockedReasons']]

    @property
    def extra_info(self):
        return RequestExtraInfo(self._data_packet._request_extra_info or {})

    @property
    def timestamp(self):
        raw_request = self._data_packet._raw_request
        request = raw_request.get('request', {}) if isinstance(raw_request, dict) else {}
        return request.get('timestamp', raw_request.get('timestamp'))


class Response(object):
    def __init__(self, data_packet, raw_response, raw_body, base64_body, timestamp):
        self._data_packet = data_packet
        self._response = raw_response
        self._raw_body = raw_body
        self._is_base64_body = base64_body
        self._body = None
        self._headers = None
        self._timestamp = timestamp

    def __getattr__(self, item):
        return self._response.get(item) if self._response else None

    @property
    def headers(self):
        if self._headers is None:
            self._headers = CaseInsensitiveDict(self._response['headers'])
            if self.extra_info.headers:
                h = CaseInsensitiveDict(self.extra_info.headers)
                for k, v in h.items():
                    if k not in self._headers:
                        self._headers[k] = v
        return self._headers

    @property
    def raw_body(self):
        return self._raw_body

    @property
    def body(self):
        if self._body is None and self._raw_body:
            if self._is_base64_body:
                self._body = b64decode(self._raw_body)
            else:
                try:
                    self._body = loads(self._raw_body)
                except (JSONDecodeError, TypeError):
                    self._body = fix_text(self._raw_body)
                    if self._body and 'event-stream' in self.headers.get('content-type', ''):
                        self._body = sse2list(self._body)

        return self._body

    @property
    def extra_info(self):
        return ResponseExtraInfo(self._data_packet._response_extra_info or {})

    @property
    def timestamp(self):
        return self._timestamp


class ExtraInfo(object):
    def __init__(self, extra_info):
        self._extra_info = extra_info

    @property
    def all_info(self):
        return self._extra_info

    def __getattr__(self, item):
        return self._extra_info.get(item)


class RequestExtraInfo(ExtraInfo):
    pass


class ResponseExtraInfo(ExtraInfo):
    pass


class FailInfo(object):
    def __init__(self, data_packet, fail_info):
        self._data_packet = data_packet
        self._fail_info = fail_info

    def __getattr__(self, item):
        return self._fail_info.get(item) if self._fail_info else None


class WebSocketConnectInfo(object):
    def __init__(self, tab, target, request_id, url, initiator=None):
        self.tab = tab
        self.tab_id = tab.tab_id
        self.target = target
        self.request_id = request_id
        self.url = url
        self.initiator = initiator
        self.request = None
        self.response = None


class WebSocketPacket(object):
    type = 'WebSocketPacket'
    resourceType = 'WebSocket'

    def __init__(self, tab, target, raw_data, is_sent):
        self.tab = tab
        self.tab_id = tab.tab_id
        self.target = target
        self.is_sent = is_sent
        self._raw_data = raw_data
        self._payload = None
        self._connect_info = None
        self._request_id = raw_data['requestId']

    def __repr__(self):
        direction = 'sent' if self.is_sent else 'received'
        return f'<WebSocketPacket {direction} requestId={self._request_id} timestamp={self.timestamp}>'

    @property
    def timestamp(self):
        return self._raw_data['timestamp']

    @property
    def data(self):
        if self._payload is None:
            payload = self._raw_data['response']
            if payload['opcode'] == 1:
                try:
                    self._payload = loads(payload['payloadData'])
                except (JSONDecodeError, TypeError):
                    self._payload = payload['payloadData']
            else:
                self._payload = b64decode(payload['payloadData'])
        return self._payload

    @property
    def url(self):
        return self._connect_info.url if self._connect_info else None

    @property
    def method(self):
        return None

    @property
    def frameId(self):
        return self.tab._frame_id

    @property
    def request(self):
        return self._connect_info.request if self._connect_info else None

    @property
    def response(self):
        return self._connect_info.response if self._connect_info else None

    @property
    def is_failed(self):
        return False


class SSEPacket(object):
    type = 'SSEPacket'
    resourceType = 'EventSource'

    def __init__(self, tab, target, raw_data):
        self.tab = tab
        self.tab_id = tab.tab_id
        self.target = target
        self._raw_data = raw_data
        self._connect_info = None
        self._request_id = raw_data['requestId']

    def __repr__(self):
        return f'<SSEPacket requestId={self._request_id} timestamp={self.timestamp}>'

    @property
    def timestamp(self):
        return self._raw_data['timestamp']

    @property
    def name(self):
        return self._raw_data['eventName']

    @property
    def id(self):
        return self._raw_data['eventId']

    @property
    def data(self):
        return self._raw_data['data']

    @property
    def url(self):
        return self._connect_info.url if self._connect_info else None

    @property
    def method(self):
        return self._connect_info.method if self._connect_info else None

    @property
    def frameId(self):
        return self._connect_info.frameId if self._connect_info else self.tab._frame_id

    @property
    def request(self):
        return self._connect_info.request if self._connect_info else None

    @property
    def response(self):
        return self._connect_info.response if self._connect_info else None

    @property
    def is_failed(self):
        return False


class MethodSetter(object):
    _ALLOW = {'GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'OPTIONS', 'PATCH', 'TRACE', 'CONNECT'}
    _REMOVE = {'remove_GET', 'remove_POST', 'remove_PUT', 'remove_DELETE', 'remove_HEAD', 'remove_OPTIONS',
               'remove_PATCH', 'remove_TRACE', 'remove_CONNECT'}
    _ATTR = '_method'

    def __init__(self, listener):
        self._listener = listener

    def all(self):
        setattr(self._listener, self._ATTR, True)
        return self

    def __getattr__(self, item):
        def _func(only=False):
            if item in self._ALLOW:
                if only:
                    setattr(self._listener, self._ATTR, {item})
                elif getattr(self._listener, self._ATTR) is not True:
                    getattr(self._listener, self._ATTR).add(item)
            elif item in self._REMOVE:
                if getattr(self._listener, self._ATTR) is True:
                    setattr(self._listener, self._ATTR, {i for i in self._ALLOW})
                getattr(self._listener, self._ATTR).discard(item[7:])
            else:
                raise ValueError()

            if self._listener.listening:
                self._listener._init_callback()

            return self

        return _func


class ResTypeSetter(MethodSetter):
    _ALLOW = {'Document', 'Stylesheet', 'Image', 'Media', 'Font', 'Script', 'TextTrack', 'XHR', 'Fetch',
              'Prefetch', 'EventSource', 'WebSocket', 'Manifest', 'SignedExchange', 'Ping', 'CSPViolationReport',
              'Preflight', 'Other'}
    _REMOVE = {'remove_Document', 'remove_Stylesheet', 'remove_Image', 'remove_Media',
               'remove_Font', 'remove_Script', 'remove_TextTrack', 'remove_XHR', 'remove_Fetch', 'remove_Prefetch',
               'remove_EventSource', 'remove_WebSocket', 'remove_Manifest', 'remove_SignedExchange', 'remove_Ping',
               'remove_CSPViolationReport', 'remove_Preflight', 'remove_Other'}
    _ATTR = '_res_type'

    def ws(self, only=False):
        return self.__getattr__('WebSocket')(only=only)

    def remove_ws(self):
        return self.__getattr__('remove_WebSocket')()

    def sse(self, only=False):
        return self.__getattr__('EventSource')(only=only)

    def remove_sse(self):
        return self.__getattr__('remove_EventSource')()


class BrowserDataPacket(object):
    def __init__(self, tab_id, target, raw_data):
        self.tab_id = tab_id
        self.target = target
        self._raw_data = raw_data
        self._request = None
        self._response = None
        self._data = None

    def __repr__(self):
        return f'<BrowserDataRacket url={self.url} target={self.target}>'

    @property
    def url(self):
        return self._raw_data['request']['url']

    @property
    def method(self):
        return self.request.method

    @property
    def frameId(self):
        return self._raw_data['frameId']

    @property
    def request(self):
        if self._request is None:
            self._request = BrowserRequest(self._raw_data['request'])
        return self._request

    @property
    def response(self):
        if self._response is None:
            self._response = BrowserResponse(self._raw_data)
        return self._response

    @property
    def data(self):
        return self.response.body

    @property
    def resourceType(self):
        return self._raw_data.get('resourceType')

    @property
    def is_failed(self):
        return 'responseErrorReason' in self._raw_data

    @property
    def timestamp(self):
        return None


class BrowserRequest(object):
    def __init__(self, request):
        self._request = request
        self._headers = None
        self._postData = None

    def __getattr__(self, item):
        return self._request.get(item)

    @property
    def headers(self):
        if self._headers is None:
            self._headers = CaseInsensitiveDict(self._info['headers'])
        return self._headers

    @property
    def params(self):
        from urllib.parse import parse_qsl, urlparse
        return dict(parse_qsl(urlparse(self.url).query, keep_blank_values=True))

    @property
    def postData(self):
        if self._postData is None:
            if self._request.get('postDataEntries'):
                if len(self._request['postDataEntries']) == 1:
                    self._postData = loads(b64decode(self._request['postDataEntries'][0]['bytes']).decode())
                else:
                    self._postData = [loads(b64decode(d['bytes']).decode()) for d in self._request['postDataEntries']]
            else:
                self._postData = False
        return self._postData


class BrowserResponse(object):
    def __init__(self, response):
        self.headers = CaseInsensitiveDict({i['name']: i['value'] for i in response.get('responseHeaders', {})})
        self.statusCode = response.get('responseStatusCode')
        self.statusText = response.get('responseStatusText')
        self.errorReason = response.get('responseErrorReason')
        self._raw_body = response.get('body')
        self._body = None

    @property
    def body(self):
        if self._raw_body and self._body is None:
            body = None
            try:
                body = b64decode(self._raw_body['body']) if self._raw_body['base64Encoded'] else self._raw_body['body']
                body = body.decode('utf-8')
                body = loads(body)
            except:
                pass
            if isinstance(body, str) and 'event-stream' in self.headers.get('content-type', ''):
                body = sse2list(body)
            self._body = body
        return self._body


def sse2list(raw):
    body = []
    for data in raw.split('\n\n'):
        row = {}
        for d in data.split('\n'):
            if not d:
                continue
            name, value = d.split(':', 1)
            try:
                row[name] = loads(value.strip())
            except JSONDecodeError:
                row[name] = value.strip()
        body.append(row)
    return body
