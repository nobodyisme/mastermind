import contextlib
import cStringIO
import gzip
import time


def gzip_compress(data, compression_level=1):
    with contextlib.closing(cStringIO.StringIO()) as buf:
        gz = gzip.GzipFile(
            fileobj=buf,
            mode='wb',
            compresslevel=compression_level
        )
        with gz:
            gz.write(data)
        return buf.getvalue()


def encode(s, encoding='utf-8', errors='strict'):
    """Returns an encoded version of string

    NB: s is encoded only if it was a unicode string, otherwise
    it is returned as is.
    """
    if isinstance(s, unicode):
        return s.encode(encoding, errors=errors)
    return s


def unidirectional_value_map(old_result, old_value, new_value, func):
    if new_value < old_value:
        return old_result
    return func(old_value, new_value)


def ts_to_str(ts):
    return time.asctime(time.localtime(ts))
