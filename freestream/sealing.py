"""What FreeStream writes to disk, sealed: AES-256-GCM under one key the server holds (#450).

I folded sealing in on 2026-09-26. The segments hold every page of every return in flight, and a
deleted segment lingers on the disk until something overwrites it; this VPS has no disk-at-rest
encryption. I weighed doing it in the host instead, with dm-crypt, and chose the code, because a host
setting would end up being something I forget about over time. So the server seals each record's
contents as it writes them and opens them as it reads them, and a journal entry the same way. Producers
and consumers never see a cipher; what crosses the socket is plaintext, and what lands on the disk is not.

★ THE LIBRARY KEEPS THE GIL OFF. The standard library has no AES, so this is the one third-party import
in FreeStream, and the stream must stay fully multithreaded (2026-09-25). `cryptography` ships
a free-threaded wheel (cp314t) and was measured on 2026-09-26: the GIL stayed off after import and use,
four threads encrypted 3.8 times what one did, and one AESGCM object served eight threads with no error.
`__main__` refuses to start if a free-threaded build finds the GIL on after its imports.

★ WHAT IS SEALED, AND WHAT IS NOT. A record's key, value and headers are sealed together, bound to the
record's offset, so a sealed record copied to another offset fails to open. Its frame header — offset,
checksum, lengths, timestamp — stays readable, which is what lets a segment find its footing past a damaged
record (`partition_log`). The key lives in a file of its own (`--key-file`), not in any product's settings,
which FreeStream never reads.
"""
from __future__ import annotations

import os
from pathlib import Path

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

KEY_BYTES = 32
NONCE_BYTES = 12


class UnopenableError(ValueError):
    """Sealed bytes that do not open under this key: damaged, moved, or sealed under another key."""


class Seal:
    """Seals and opens bytes under one key. Safe to share between threads (measured; see above)."""

    def __init__(self, key: bytes) -> None:
        if len(key) != KEY_BYTES:
            raise ValueError(f"a FreeStream key is {KEY_BYTES} bytes, not {len(key)}")
        self._aead = AESGCM(key)

    @classmethod
    def with_a_new_key(cls) -> Seal:
        """A seal under a key nobody else has — a server a test builds without a key file."""
        return cls(AESGCM.generate_key(bit_length=KEY_BYTES * 8))

    def seal(self, plaintext: bytes, bound_to: bytes) -> bytes:
        """The plaintext sealed: a fresh nonce, then the ciphertext and its tag."""
        nonce = os.urandom(NONCE_BYTES)
        return nonce + self._aead.encrypt(nonce, plaintext, bound_to)

    def open(self, sealed: bytes, bound_to: bytes) -> bytes:
        if len(sealed) < NONCE_BYTES:
            raise UnopenableError("too short to be sealed")
        try:
            return self._aead.decrypt(sealed[:NONCE_BYTES], sealed[NONCE_BYTES:], bound_to)
        except InvalidTag as refused:
            raise UnopenableError("does not open under this key") from refused


def load_key(path: str | Path) -> bytes:
    """The key in a key file: 64 hex characters, as `new_key_file` writes them."""
    text = Path(path).read_text().strip()
    try:
        key = bytes.fromhex(text)
    except ValueError as unreadable:
        raise ValueError(f"{path} does not hold a hex key") from unreadable
    if len(key) != KEY_BYTES:
        raise ValueError(f"{path} holds {len(key)} bytes; a FreeStream key is {KEY_BYTES}")
    return key


def new_key_file(path: str | Path) -> None:
    """Write a new random key, readable by its owner alone. Refuses to replace one that exists."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(AESGCM.generate_key(bit_length=KEY_BYTES * 8).hex() + "\n")


if __name__ == "__main__":
    import sys

    new_key_file(sys.argv[1])
