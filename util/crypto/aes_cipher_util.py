from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

AES_256_KEY_BYTES = 32
GCM_NONCE_BYTES = 12
GCM_TAG_BYTES = 16
PAYLOAD_VERSION = b'\x01'


def _aes_256_key(key: str) -> bytes:
    try:
        encoded = bytes.fromhex(key)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError('AES-256 keys must contain exactly 64 hexadecimal characters') from error
    if len(key) != 64 or len(encoded) != AES_256_KEY_BYTES:
        raise ValueError('AES-256 keys must contain exactly 64 hexadecimal characters')
    return encoded


def encrypt(key: str, raw: str) -> bytes:
    cipher = AES.new(
        _aes_256_key(key), AES.MODE_GCM, nonce=get_random_bytes(GCM_NONCE_BYTES), mac_len=GCM_TAG_BYTES)
    ciphertext, tag = cipher.encrypt_and_digest(str(raw).encode('utf-8'))
    return PAYLOAD_VERSION + cipher.nonce + tag + ciphertext


def decrypt(key: str, enc: bytes) -> bytes:
    payload = bytes(enc)
    minimum_length = len(PAYLOAD_VERSION) + GCM_NONCE_BYTES + GCM_TAG_BYTES
    if len(payload) < minimum_length or payload[:1] != PAYLOAD_VERSION:
        raise ValueError('Unsupported or malformed encrypted payload')
    nonce_start = len(PAYLOAD_VERSION)
    tag_start = nonce_start + GCM_NONCE_BYTES
    ciphertext_start = tag_start + GCM_TAG_BYTES
    nonce = payload[nonce_start:tag_start]
    tag = payload[tag_start:ciphertext_start]
    ciphertext = payload[ciphertext_start:]
    cipher = AES.new(_aes_256_key(key), AES.MODE_GCM, nonce=nonce, mac_len=GCM_TAG_BYTES)
    return cipher.decrypt_and_verify(ciphertext, tag)
