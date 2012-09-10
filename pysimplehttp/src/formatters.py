import re
import binascii
import calendar


def _crc(key):
    """crc32 hash a string"""
    return binascii.crc32(_utf8(key)) & 0xffffffff


def _b32(number):
    """convert positive integer to a base32 string"""
    assert isinstance(number, (int, long))
    alphabet = '0123456789abcdefghijklmnopqrstuv'
    alphabet_len = 32

    if number == 0:
        return alphabet[0]

    base32 = ''

    sign = ''
    if number < 0:
        sign = '-'
        number = -number

    while number != 0:
        number, i = divmod(number, alphabet_len)
        base32 = alphabet[i] + base32

    return sign + base32


def _idn(domain):
    """idn encode a domain name"""
    if not domain:
        return domain
    if 'xn--' in domain:
        return domain.decode('idna')
    return _unicode(domain)


def _punycode(domain):
    """idna encode (punycode) a domain name"""
    if not domain:
        return domain
    domain = _unicode(domain)
    if re.findall(r'[^-_a-zA-Z0-9\.]', domain):
        return domain.encode('idna')
    return _utf8(domain)


def _unicode(value):
    """decode a utf-8 string as unicode"""
    if isinstance(value, str):
        return value.decode("utf-8")
    assert isinstance(value, unicode)
    return value


def _utf8(s):
    """encode a unicode string as utf-8"""
    if isinstance(s, unicode):
        return s.encode("utf-8")
    assert isinstance(s, str)
    return s


def _utc_ts(dt):
    """convert a datetime object into a UNIX epoch timestamp"""
    return calendar.timegm(dt.utctimetuple())


def _utf8_params(params):
    """encode a dictionary of URL parameters (including iterables) as utf-8"""
    isinstance(params, dict)
    encoded_params = []
    for k, v in params.items():
        if isinstance(v, (list, tuple)):
            v = [_utf8(x) for x in v]
        else:
            v = _utf8(v)
        encoded_params.append((k, v))
    return dict(encoded_params)


class _O(dict):
    """Makes a dictionary behave like an object."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            # raise AttributeError(name)
            return None

    def __setattr__(self, name, value):
        self[name] = value
