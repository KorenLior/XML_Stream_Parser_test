
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Incremental XML Stream Parser with async generators.

Features:
- Handles fragmented XML chunks.
- Supports multiple concurrent generators (each with its own incremental parser).
- Maintains a global "state" dict (last-write-wins, deep-merge on nested dicts).
- Converts XML -> JSON-like Python dict (attributes at "@attr", text at "#text",
  numbers auto-parsed to int/float, including scientific notation).
"""

import asyncio
import random
from typing import AsyncIterator, Dict, Any, Iterable, Optional
from xml.etree.ElementTree import XMLPullParser, ParseError

# -----------------------
# Fragment generators
# -----------------------

async def create_generator(stream_str: str,
                           chunk_sizes: Optional[Iterable[int]] = None,
                           min_delay: float = 0.0,
                           max_delay: float = 0.0) -> AsyncIterator[str]:
    """
    Async generator yielding chunks of stream_str with random (or fixed) delays.
    If chunk_sizes is provided, uses those exact sizes deterministically.
    Otherwise, uses random chunk sizes (1..4 chars) to stress incremental parsing.
    """
    idx = 0
    n = len(stream_str)
    if chunk_sizes is None:
        while idx < n:
            sz = random.randint(1, 4)
            await asyncio.sleep(random.uniform(min_delay, max_delay))
            yield stream_str[idx: idx+sz]
            idx += sz
    else:
        for sz in chunk_sizes:
            if idx >= n:
                break
            await asyncio.sleep(random.uniform(min_delay, max_delay))
            yield stream_str[idx: idx+sz]
            idx += sz
        if idx < n:
            await asyncio.sleep(random.uniform(min_delay, max_delay))
            yield stream_str[idx:]

# -----------------------
# XML -> Python helpers
# -----------------------

def _to_number_if_possible(text: Optional[str]):
    """Convert text to int/float when sensible; otherwise return as-is."""
    if text is None:
        return None
    s = text.strip()
    if s == "":
        return ""
    # Try int first
    try:
        return int(s)
    except ValueError:
        pass
    # Then float (handles 2.5, -3.0, 1e-3, etc.)
    try:
        return float(s)
    except ValueError:
        return text

def _elem_to_obj(elem) -> Any:
    """
    Convert an ElementTree element to a JSON-like object.
    - Attributes as "@attr"
    - If element has attributes or children, text is placed under "#text"
    - If element is a pure leaf (no attrs, no children), become a scalar
    """
    children = list(elem)
    has_children = len(children) > 0
    has_attrs = bool(elem.attrib)

    # Leaf scalar
    if not has_children and not has_attrs:
        return _to_number_if_possible(elem.text)

    node: Dict[str, Any] = {}

    # Attributes
    if has_attrs:
        for k, v in elem.attrib.items():
            node[f"@{k}"] = v

    # Children (last-write-wins for repeated child tags)
    for c in children:
        node[c.tag] = _elem_to_obj(c)

    # Text (when also attrs/children)
    if elem.text is not None:
        txt = elem.text.strip()
        if txt != "":
            node["#text"] = _to_number_if_possible(txt)

    return node

def _deep_update(dst: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge: dicts merge recursively; non-dicts overwrite (last write wins)."""
    for k, v in src.items():
        if k in dst and isinstance(dst[k], dict) and isinstance(v, dict):
            _deep_update(dst[k], v)
        else:
            dst[k] = v
    return dst

# -----------------------
# StateParser
# -----------------------




def _make_decl_stripper():
    """
    Streaming sanitizer for XML declarations.
    Removes any '<?xml ...?>' tokens from the stream safely, even when
    the token is split across chunks and even if it appears mid-stream.
    Case-insensitive for '<?xml'.
    """
    carry = ""  # un-emitted buffer that may contain a partial declaration

    def remove_decls(s: str) -> str:
        nonlocal carry
        if not s:
            return s
        carry += s
        out_parts = []
        i = 0
        while True:
            low = carry.lower()
            start = low.find("<?xml", i)
            if start == -1:
                # no declaration start from position i; emit tail
                out_parts.append(carry[i:])
                carry = ""  # everything emitted
                break
            # emit content before declaration
            out_parts.append(carry[i:start])
            end = low.find("?>", start)
            if end == -1:
                # incomplete declaration at end -> retain from 'start'
                carry = carry[start:]
                break
            else:
                # drop the whole declaration and continue scanning after it
                i = end + 2
                if i >= len(carry):
                    carry = ""
                    break
                continue
        return "".join(out_parts)

    return remove_decls
class StateParser:
    """
    Consumes one or more async generators of XML fragments.
    Each generator is parsed with its own XMLPullParser instance, wrapped in a
    synthetic <root> so multiple top-level siblings are allowed. We merge only
    when a top-level element completes (depth==1).
    """
    def __init__(self) -> None:
        self._generators: list[AsyncIterator[str]] = []
        self._state: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self._done = False

    def add_generator(self, gen: AsyncIterator[str]) -> None:
        if self._done:
            raise RuntimeError("Cannot add generators after run() completed")
        self._generators.append(gen)

    
    async def _consume_generator(self, gen: AsyncIterator[str]) -> None:
        parser = XMLPullParser(events=["start", "end"])
        depth = 0
        strip_decl = _make_decl_stripper()
        root_fed = False

        async def feed_root_once():
            nonlocal root_fed
            if not root_fed:
                parser.feed("<root>")
                root_fed = True

        async for chunk in gen:
            chunk = strip_decl(chunk)
            if not chunk:
                continue
            # ensure root is fed only after declaration stripping
            await feed_root_once()
            try:
                parser.feed(chunk)
            except ParseError:
                raise

            for event, elem in parser.read_events():
                if event == "start":
                    if elem.tag != "root":
                        depth += 1
                elif event == "end":
                    if elem.tag == "root":
                        continue
                    if depth == 1:
                        obj = _elem_to_obj(elem)
                        async with self._lock:
                            _deep_update(self._state, {elem.tag: obj})
                        elem.clear()
                    if depth > 0:
                        depth -= 1

        # Finalize this stream (allow partial state on truncation)
        try:
            # If no content ever arrived (but maybe declaration did), still need the root
            if not root_fed:
                parser.feed("<root>")
            parser.feed("</root>")
            for event, elem in parser.read_events():
                if event == "start":
                    if elem.tag != "root":
                        depth += 1
                elif event == "end":
                    if elem.tag == "root":
                        continue
                    if depth == 1:
                        obj = _elem_to_obj(elem)
                        async with self._lock:
                            _deep_update(self._state, {elem.tag: obj})
                        elem.clear()
                    if depth > 0:
                        depth -= 1
        except Exception:
            pass

    async def run(self, timeout: Optional[float] = None) -> Dict[str, Any]:
        if not self._generators:
            self._done = True
            return {}
        tasks = [asyncio.create_task(self._consume_generator(g)) for g in self._generators]
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
        except asyncio.TimeoutError:
            for t in tasks:
                if not t.done():
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass
        self._done = True
        return self._state

# -----------------------
# Demos (safe to run as a script)
# -----------------------

async def run_basic_test():
    stream_str = "<counter>1</counter><counter>2</counter><counter>3</counter>"
    expected_state = {"counter": 3}
    generator = create_generator(stream_str, min_delay=0.0, max_delay=0.0)
    state_parser = StateParser()
    state_parser.add_generator(generator)
    actual_state = await state_parser.run(timeout=5)
    print("Basic test state:", actual_state)
    assert expected_state == actual_state, f"Expected {expected_state}, got {actual_state}"
    print("✔ Basic test passed.")

async def run_nested_test():
    # Example from prompt: <foo><bar>1</bar></foo> arrives fragmented
    async def gen():
        for p in ["<foo><", "bar>1</ba", "r></fo", "o>"]:
            yield p
    expected = {"foo": {"bar": 1}}
    sp = StateParser()
    sp.add_generator(gen())
    state = await sp.run(timeout=5)
    print("Nested test state:", state)
    assert state == expected, f"Expected {expected}, got {state}"
    print("✔ Nested test passed.")

async def run_multi_generator_test_deterministic():
    # Two fixed-timing generators -> truly deterministic
    async def gen1():
        yield "<a>10</a>"
        await asyncio.sleep(0.01)
        yield "<b>20</b>"

    async def gen2():
        await asyncio.sleep(0.02)  # arrives after gen1's <b>20</b>
        yield "<b>30</b>"
        await asyncio.sleep(0.005)
        yield "<c><d>4.5</d></c>"

    expected = {"a": 10, "b": 30, "c": {"d": 4.5}}
    sp = StateParser()
    sp.add_generator(gen1())
    sp.add_generator(gen2())
    state = await sp.run(timeout=2)
    print("Deterministic multi-gen state:", state)
    assert state == expected, f"Expected {expected}, got {state}"
    print("✔ Deterministic multi-generator test passed.")

async def demo():
    await run_basic_test()
    await run_nested_test()
    await run_multi_generator_test_deterministic()

if __name__ == "__main__":
    asyncio.run(demo())
