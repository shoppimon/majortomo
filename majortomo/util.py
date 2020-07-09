"""Utilities and helpers useful in other modules
"""

from typing import Text, Union

TextOrBytes = Union[Text, bytes]


def text_to_ascii_bytes(text):
    # type: (TextOrBytes) -> bytes
    """Convert a text-or-bytes value to ASCII-encoded bytes

    If the input is already `bytes`, we simply return it as is
    """
    if hasattr('encode', text):
        return text.encode('ascii')
    return text
