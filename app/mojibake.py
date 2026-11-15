"""Repair of UTF-8 mojibake spans (misdecoded as Latin-1 / Windows-1252).

The crawler stored UTF-8 bytes after decoding them with the wrong
single-byte charset, so a span like "cafÃ©" ended up in the corpus instead
of "café". Reversing the mistake for one span means: encode the mojibake
string back to the byte string it came from (using the mis-decoding
charset), then decode those bytes as UTF-8.

Only whole-span inverse conversions are offered, and only when they
actually change the text — normal (uncorrupted) text either fails the
round-trip (e.g. "é" is not valid UTF-8 when re-encoded as Latin-1) or
would be left unchanged, so genuine text is never "repaired".
"""

# The two inverse conversions, in stable presentation order.
CONVERSIONS = ("latin-1", "windows-1252")

_CODEC = {"latin-1": "latin-1", "windows-1252": "cp1252"}
_LABEL = {"latin-1": "Latin-1", "windows-1252": "Windows-1252"}


def inverse_convert(segment: str, conversion: str):
    """Attempt one inverse mis-decoding on *segment*.

    Returns ``(replacement, None)`` when the span is valid mojibake of
    this kind, otherwise ``(None, reason)`` explaining why no candidate
    can be offered. A round-trip that would leave the text unchanged is
    not a valid repair candidate (there is nothing to fix).
    """
    codec = _CODEC[conversion]
    label = _LABEL[conversion]
    try:
        raw = segment.encode(codec)
    except UnicodeEncodeError as exc:
        bad = exc.object[exc.start]
        return None, (
            f"contains {bad!r} (U+{ord(bad):04X}) which {label} cannot "
            f"represent, so the span cannot be UTF-8-misdecoded-as-{label} "
            f"mojibake"
        )
    try:
        fixed = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        return None, (
            f"re-encoding as {label} yields bytes that are not valid UTF-8 "
            f"({exc.reason} at byte {exc.start}), so the span is not this "
            f"kind of mojibake"
        )
    if fixed == segment:
        return None, (
            "inverse conversion leaves the text unchanged "
            "(no mojibake signature in this span)"
        )
    return fixed, None
