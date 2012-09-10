import tornado.httpclient
import urllib

from formatters import _utf8, _utf8_params

try:
    import ujson as json
except ImportError:
    import json

_HTTPCLIENT = None
def get_http_client():
    global _HTTPCLIENT
    if _HTTPCLIENT is None:
        _HTTPCLIENT = tornado.httpclient.HTTPClient()
    return _HTTPCLIENT

def http_fetch(url, params={}, headers=None, method='GET', body=None, timeout=5.0, client_options=None, connect_timeout=None, request_timeout=None):
    headers = headers or {}
    client_options = client_options or {}

    for key, value in params.items():
        if isinstance(value, (int, long, float)):
            params[key] = str(value)
        if value is None:
            del params[key]

    params = _utf8_params(params)
    if method in ['GET', 'HEAD'] and params:
        url += '?' + urllib.urlencode(params, doseq=1)
        body = None
    elif params:
        body = urllib.urlencode(params, doseq=1)
        headers['Content-type'] = 'application/x-www-form-urlencoded'
    if body:
        body = _utf8(body)
    if 'follow_redirects' not in client_options:
        client_options['follow_redirects'] = False
    req = tornado.httpclient.HTTPRequest(url=_utf8(url), \
                    method=method,
                    body=body,
                    headers=headers,
                    connect_timeout=connect_timeout or timeout,
                    request_timeout=request_timeout or timeout,
                    validate_cert=False,
                    **client_options)

    # sync client raises errors on non-200 responses
    http = get_http_client()
    response = http.fetch(req)
    if method == 'HEAD':
        return True
    return response.body


def pubsub_write(endpoint, data):
    if isinstance(data, dict):
        data = json.dumps(data)
    return http_fetch(endpoint + '/pub', body=data, method='POST', timeout=1.5)

def simplequeue_write(endpoint, data):
    assert isinstance(data, dict)
    data = json.dumps(data)
    result = http_fetch(endpoint + '/put', dict(data=data))
    # simplequeue success is a 200 response w/ an empty response body
    return result == ''
