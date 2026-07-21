import unittest

from util.crypto.aes_cipher_util import decrypt, encrypt, PAYLOAD_VERSION


class AesCipherUtilTest(unittest.TestCase):
    KEY = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'

    def test_aes_256_gcm_round_trip_uses_versioned_raw_binary(self):
        first = encrypt(self.KEY, 'highly sensitive')
        second = encrypt(self.KEY, 'highly sensitive')

        self.assertTrue(first.startswith(PAYLOAD_VERSION))
        self.assertNotEqual(first, second)
        self.assertNotIn(b'highly sensitive', first)
        self.assertEqual(b'highly sensitive', decrypt(self.KEY, first))

    def test_tampering_fails_authentication(self):
        encrypted = bytearray(encrypt(self.KEY, 'protected'))
        encrypted[-1] ^= 1

        with self.assertRaises(ValueError):
            decrypt(self.KEY, encrypted)

    def test_requires_an_exact_32_byte_ascii_key(self):
        for invalid_key in ('short', 'a' * 65, 'z' * 64, '\N{LOCK}' * 64):
            with self.subTest(invalid_key=invalid_key):
                with self.assertRaisesRegex(ValueError, 'exactly 64 hexadecimal'):
                    encrypt(invalid_key, 'protected')


if __name__ == '__main__':
    unittest.main()
