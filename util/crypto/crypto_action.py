from enum import Enum


class CryptoAction(Enum):
    encrypt = "Encryption"
    decrypt = "Decryption"

    def __str__(self):
        return '%s' % self.value