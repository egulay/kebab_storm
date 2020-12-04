import base64

from Crypto import Random
from Crypto.Cipher import AES

from util.constants import BS


def pad(s):
    return s + (BS - len(s) % BS) * \
           chr(BS - len(s) % BS).encode('utf8')


def un_pad(s):
    return s[:-ord(s[len(s) - 1:])]


def encrypt(key: str, raw: str) -> bytes:
    m_key = bytes(pad(key) if len(key) < BS else key[0:BS], 'utf-8')
    raw = str(raw).encode('utf8')
    raw = pad(raw)
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(m_key[:BS], AES.MODE_CBC, iv)
    return base64.b64encode(iv + cipher.encrypt(raw))


def decrypt(key: str, enc: str) -> bytes:
    m_key = bytes(pad(key) if len(key) < BS else key[0:BS], 'utf-8')
    enc = base64.b64decode(enc)
    iv = enc[:16]
    cipher = AES.new(m_key[:BS], AES.MODE_CBC, iv)
    return un_pad(cipher.decrypt(enc[16:]))
