#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xml_stream_parser_fast.py — thin wrapper
Imports your existing xml_stream_parser.py and runs the same demos but with tiny delays
so the script completes quickly and deterministically.
"""
import asyncio
import xml_stream_parser as base  # uses your existing parser implementation

# --- FAST DEMOS (no code duplication of the parser) ---

async def run_basic_test_fast():
    s = "<counter>1</counter><counter>2</counter><counter>3</counter>"
    exp = {"counter": 3}
    gen = base.create_generator(s, min_delay=0.005, max_delay=0.02)
    # Small fixed timeout is fine here; or None to avoid any flakiness
    sp = base.StateParser(); sp.add_generator(gen)
    state = await sp.run(timeout=2)  # generous for tiny delays
    print("Basic (fast) state:", state)
    assert state == exp
    print("✔ Basic fast test passed.")

async def run_nested_test_fast():
    async def gen():
        for p in ["<foo><", "bar>1</ba", "r></fo", "o>"]:
            # mimic tiny streaming delays
            await asyncio.sleep(0.01)
            yield p
    exp = {"foo": {"bar": 1}}
    sp = base.StateParser(); sp.add_generator(gen())
    state = await sp.run(timeout=2)
    print("Nested (fast) state:", state)
    assert state == exp
    print("✔ Nested fast test passed.")

async def run_multi_generator_test_fast():
    # Deterministic ordering: ensure <b>30</b> lands after <b>20</b>
    async def gen1():
        yield "<a>10</a>"
        await asyncio.sleep(0.01)
        yield "<b>20</b>"
    async def gen2():
        await asyncio.sleep(0.02)
        yield "<b>30</b>"
        await asyncio.sleep(0.005)
        yield "<c><d>4.5</d></c>"
    exp = {"a": 10, "b": 30, "c": {"d": 4.5}}
    sp = base.StateParser(); sp.add_generator(gen1()); sp.add_generator(gen2())
    state = await sp.run(timeout=2)
    print("Deterministic multi-gen (fast) state:", state)
    assert state == exp
    print("✔ Deterministic multi-generator fast test passed.")

async def demo():
    await run_basic_test_fast()
    await run_nested_test_fast()
    await run_multi_generator_test_fast()

if __name__ == "__main__":
    asyncio.run(demo())
