import base64
from hashlib import pbkdf2_hmac

from Crypto.Cipher import AES


class EventhubStreamConnectionStringEncrypt:
    """
    For connecting to an eventhub stream using the library

    "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22"

    This class replaces the need to call the protected method

    >>> spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt

    You need to supply the SparkConnectorVersion
    (last part of the library that you installed)
    """

    def __init__(self, SparkConnectorVersion: str = "2.3.22"):
        self.key = self._derive_key(SparkConnectorVersion)
        self.mode: AES.MODE_ECB = AES.MODE_ECB
        self.block_size: int = 16

    def _pad(self, byte_array: bytearray):
        """
        pkcs5 padding
        """
        pad_len = self.block_size - len(byte_array) % self.block_size
        return byte_array + (bytes([pad_len]) * pad_len)

    def _derive_key(self, SparkConnectorVersion: str) -> bytes:
        return pbkdf2_hmac(
            hash_name="sha256",
            password=SparkConnectorVersion.encode(),
            salt=SparkConnectorVersion.encode(),
            iterations=1000,
            dklen=32,
        )

    def encrypt(self, message: str) -> str:
        # convert to bytes
        byte_array = message.encode("UTF-8")
        # pad the message - with pkcs5 style
        padded = self._pad(byte_array)
        # new instance of AES with encoded key
        cipher = AES.new(self.key, AES.MODE_ECB)
        # now encrypt the padded bytes
        encrypted = cipher.encrypt(padded)
        # base64 encode and convert back to string
        return base64.b64encode(encrypted).decode("utf-8")
