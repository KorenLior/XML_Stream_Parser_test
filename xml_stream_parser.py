#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Incremental XML Stream Parser with async generators.

Fixes:
- Robust XML declaration stripping (handles fragmented '<?xml ...?>').
- Delay synthetic <root> until after declaration handling.
- Safe-timeout helpers for deterministic tests with random delays/chunking.
- Last-write-wins deep merge at top-level closes (depth == 1).
- Attributes as '@attr', text as '#text'; numeric parsing incl. scientific notation.
"""

import asyncio
import random
from typing import AsyncIterator, Dict, Any, Iterable, Optional
from xml.etree.ElementTree import XMLPullParser, ParseError


# -----------------------
# Fragment generator
# -----------------------

async def create_generator(stream_str: str,
                           chunk_sizes: Optional[Iterable[int]] = None,
                           min_delay: float = 0.0,
                           max_delay: float = 0.0) -> AsyncIterator[str]:
    """
    Async generator yielding chunks of stream_str with delays.
    - If chunk_sizes provided: yields exactly those sizes in order (deterministic).
    - Else: yields random chunks of size 1..4 (stress incremental parsing).
    """
    random.seed(42)
    idx = 0
    n = len(stream_str)
    if chunk_sizes is None:
        while idx < n:
            sz = random.randint(1, 4)
            await asyncio.sleep(random.uniform(min_delay, max_delay))
            yield stream_str[idx: idx + sz]
            idx += sz
    else:
        for sz in chunk_sizes:
            if idx >= n:
                break
            await asyncio.sleep(random.uniform(min_delay, max_delay))
            yield stream_str[idx: idx + sz]
            idx += sz
        if idx < n:
            await asyncio.sleep(random.uniform(min_delay, max_delay))
            yield stream_str[idx:]


# -----------------------
# XML -> Python helpers
# -----------------------

def _to_number_if_possible(text: Optional[str]) -> Any:
    """Convert text to int/float when sensible; otherwise return as-is."""
    if text is None:
        return None
    s = text.strip()
    if s == "":
        return ""
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)  # handles 2.5, -3.0, 1e-3, etc.
    except ValueError:
        return text


def _elem_to_obj(elem) -> Any:
    """
    Convert an ElementTree element to a JSON-like object.
    - Attributes as "@attr"
    - If element has attributes or children, text is placed under "#text"
    - If element is a pure leaf (no attrs, no children), become a scalar value
    """
    children = list(elem)
    has_children = len(children) > 0
    has_attrs = bool(elem.attrib)

    if not has_children and not has_attrs:
        return _to_number_if_possible(elem.text)

    node: Dict[str, Any] = {}

    # attributes
    if has_attrs:
        for k, v in elem.attrib.items():
            node[f"@{k}"] = v

    # children (last-write-wins per key)
    for c in children:
        node[c.tag] = _elem_to_obj(c)

    # text (only when there are attrs/children)
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
# Safe-timeout helpers
# -----------------------

def _estimate_worst_case_chunks(stream_str: str,
                                chunk_sizes: Optional[Iterable[int]]) -> int:
    """
    Upper-bound the number of chunks the generator will yield.
    - If chunk_sizes provided: count them (+1 if remainder).
    - Else: worst case is 1-char chunks => len(stream_str).
    """
    if chunk_sizes is None:
        return len(stream_str)
    total = 0
    count = 0
    for sz in chunk_sizes:
        if total >= len(stream_str):
            break
        total += sz
        count += 1
    if total < len(stream_str):
        count += 1
    return count


def compute_safe_timeout_for_stream(stream_str: str,
                                    min_delay: float,
                                    max_delay: float,
                                    chunk_sizes: Optional[Iterable[int]] = None,
                                    margin: float = 2.0) -> Optional[float]:
    """
    Worst-case timeout for a *single* generator:
      worst_chunks * max_delay + margin

    If max_delay == 0 (or both delays are 0), returns None (no timeout needed).
    """
    if max_delay <= 0.0:
        return None
    worst_chunks = _estimate_worst_case_chunks(stream_str, chunk_sizes)
    return worst_chunks * max_delay + margin


def compute_safe_timeout_for_generators(specs: list[dict],
                                        margin: float = 2.0) -> Optional[float]:
    """
    Given multiple generators (run concurrently), return a safe timeout that
    lets the *slowest* one finish in the worst case.

    Each spec is: {"stream_str": str, "min_delay": float, "max_delay": float, "chunk_sizes": Optional[Iterable[int]]}
    """
    per_gen = []
    for sp in specs:
        t = compute_safe_timeout_for_stream(
            sp["stream_str"],
            sp.get("min_delay", 0.0),
            sp.get("max_delay", 0.0),
            sp.get("chunk_sizes")
        )
        if t is not None:
            per_gen.append(t)
    if not per_gen:
        return None
    # generators run concurrently → take max, not sum
    return max(per_gen) + margin


# -----------------------
# Declaration stripper
# -----------------------

def _make_decl_stripper():
    """
    Robustly strips a single leading XML declaration **at the very start** of a stream,
    even if it is fragmented across tiny chunks (e.g., "<", "?x", "ml", " ... ?>").
    Also strips any stray '<?xml ...?>' tokens that may appear later (defensive).
    """
    import re

    buf: list[str] = []
    active = True
    decl_re = re.compile(r"<\?xml[^?]*\?>", re.IGNORECASE)

    def process(s: str) -> str:
        nonlocal buf, active
        if not s:
            return s
        if not active:
            # pass-through, but defensively strip any stray declarations in later chunks
            if "<?xml" in s:
                return decl_re.sub("", s)
            return s

        # Buffer until we can decide if the *leading* non-space chars are '<?xml'
        buf.append(s)
        joined = "".join(buf)
        ls = joined.lstrip()

        # Need at least 2 chars after stripping spaces to know if it's '<' followed by something
        if len(ls) < 2 and ls.startswith("<"):
            return ""  # more data needed

        # Need at least 5 chars to fully see '<?xml'
        if len(ls) < 5 and ls.startswith("<?"):
            return ""  # might be '<?xml', keep buffering

        if ls.startswith("<?xml"):
            # It's an XML declaration; wait for '?>'
            if "?>" not in ls:
                return ""  # keep buffering
            # Strip the declaration from the joined string
            offset = len(joined) - len(ls)  # position shift due to lstrip
            start = offset + ls.find("<?xml")
            end = offset + ls.find("?>", ls.find("<?xml"))
            out = joined[:start] + joined[end + 2:]
            active = False
            buf = []
            # also remove any stray mid-stream declarations
            if "<?xml" in out:
                out = decl_re.sub("", out)
            return out
        else:
            # Not a declaration at the start; finalize buffering and pass through
            active = False
            out = joined
            if "<?xml" in out:
                out = decl_re.sub("", out)
            buf = []
            return out

    return process


# -----------------------
# StateParser
# -----------------------

class StateParser:
    """
    Consumes one or more async generators of XML fragments.
    - Each generator is parsed with its own XMLPullParser.
    - We add a synthetic <root> so multiple top-level siblings are allowed.
    - We merge an element into state only when a **top-level** element closes (depth == 1).
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

        # Stream in chunks
        async for chunk in gen:
            chunk = strip_decl(chunk)
            if not chunk:
                continue
            # ensure root fed only after declaration (if any) is handled
            await feed_root_once()

            try:
                parser.feed(chunk)
            except ParseError:
                # Malformed mid-stream (e.g., mismatched tags) -> bubble up
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
            # Keep partial state if truncated
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
# Demos (safe-timeout based)
# -----------------------
async def debug_generator(gen):
    async for frag in gen:
        print("GEN FRAGMENT:", repr(frag))


async def run_basic_test():
    stream_str = "<counter>1</counter><counter>2</counter><counter>3</counter>"
    # stream_str = "<counter>3</counter>"
    expected_state = {"counter": 3}

    # Realistic streaming (random 1..4 char chunks, 1..3s delays)
    min_delay, max_delay = 0.1, 0.3
    generator = create_generator(stream_str, min_delay=min_delay, max_delay=max_delay)
    # print generator
    # await debug_generator(generator)
    # generator2 = create_generator(stream_str, min_delay=min_delay, max_delay=max_delay)
    # await debug_generator(generator2)
    # Compute a safe timeout that accounts for worst-case chunking+delays
    timeout = compute_safe_timeout_for_stream(
        stream_str, min_delay, max_delay, chunk_sizes=None, margin=2.0
    )

    sp = StateParser()
    sp.add_generator(generator)
    actual_state = await sp.run(timeout=timeout)
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
    # Two fixed-timing generators -> deterministic last-write-wins on 'b'
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
	

async def run_multi_generator_test_deterministic2():
    # Two fixed-timing generators -> deterministic last-write-wins on 'b'
    expected_state = {"counter": 3}

    # Realistic streaming (random 1..4 char chunks, 1..3s delays)
    min_delay, max_delay = 0.01, 0.02
    stream_str = "<foo><bar>1</bar></foo><foo><bar>2</bar></foo><foo><bar>3</bar></foo>"
    gen1 = create_generator(stream_str, min_delay=min_delay, max_delay=max_delay)
    stream_str2 = "<foo><bar2>1</bar2></foo><foo><bar2>2</bar2></foo>"
    gen2 = create_generator(stream_str2, min_delay=min_delay, max_delay=max_delay)

    expected = {"foo": ({"bar": 3},{"bar2": 2})}
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
