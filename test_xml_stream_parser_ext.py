import unittest, asyncio
from xml.etree.ElementTree import ParseError
import xml_stream_parser as xmlsp

class TestXMLStreamParserExtended(unittest.IsolatedAsyncioTestCase):
    async def test_attributes(self):
        gen = xmlsp.create_generator('<item id="42" kind="x">7</item>', min_delay=0, max_delay=0)
        sp = xmlsp.StateParser(); sp.add_generator(gen)
        state = await sp.run(timeout=2)
        self.assertEqual(state, {"item": {"@id": "42", "@kind": "x", "#text": 7}})

    async def test_repeated_tags_last_wins(self):
        gen = xmlsp.create_generator("<val>1</val><val>2</val><val>9</val>", min_delay=0, max_delay=0)
        sp = xmlsp.StateParser(); sp.add_generator(gen)
        state = await sp.run(timeout=2)
        self.assertEqual(state, {"val": 9})

    async def test_nested_repeated_children_last_wins(self):
        gen = xmlsp.create_generator("<parent><a>1</a><a>5</a></parent>", min_delay=0, max_delay=0)
        sp = xmlsp.StateParser(); sp.add_generator(gen)
        state = await sp.run(timeout=2)
        self.assertEqual(state, {"parent": {"a": 5}})

    async def test_numbers_and_whitespace(self):
        gen = xmlsp.create_generator("<n1>  10 </n1><n2>\n-3</n2><n3>2.5</n3><n4>1e-3</n4>", min_delay=0, max_delay=0)
        sp = xmlsp.StateParser(); sp.add_generator(gen)
        s = await sp.run(timeout=2)
        self.assertEqual(s, {"n1": 10, "n2": -3, "n3": 2.5, "n4": 0.001})

    async def test_heavy_fragmentation(self):
        xml = "<foo><bar>1</bar><baz>2</baz></foo>"
        sizes = [1] * len(xml)
        gen = xmlsp.create_generator(xml, chunk_sizes=sizes, min_delay=0, max_delay=0)
        sp = xmlsp.StateParser(); sp.add_generator(gen)
        s = await sp.run(timeout=2)
        self.assertEqual(s, {"foo": {"bar": 1, "baz": 2}})

    async def test_unicode_text(self):
        gen = xmlsp.create_generator("<msg>שלום 🌟</msg>", min_delay=0, max_delay=0)
        sp = xmlsp.StateParser(); sp.add_generator(gen)
        s = await sp.run(timeout=2)
        self.assertEqual(s, {"msg": "שלום 🌟"})

    async def test_cdata_and_entities(self):
        gen = xmlsp.create_generator("<t><![CDATA[raw <&> text]]></t><e>&lt;&amp;&gt;</e>", min_delay=0, max_delay=0)
        sp = xmlsp.StateParser(); sp.add_generator(gen)
        s = await sp.run(timeout=2)
        self.assertEqual(s, {"t": "raw <&> text", "e": "<&>"})

    async def test_namespaces(self):
        gen = xmlsp.create_generator('<ns0:foo xmlns:ns0="urn:x"><ns0:bar>1</ns0:bar></ns0:foo>', min_delay=0, max_delay=0)
        sp = xmlsp.StateParser(); sp.add_generator(gen)
        s = await sp.run(timeout=2)
        self.assertEqual(s, {"{urn:x}foo": {"{urn:x}bar": 1}})

    async def test_comments_and_pi_ignored_with_decl(self):
        gen = xmlsp.create_generator("<?xml version='1.0'?><a>1</a><!--comment--><b>2</b>", min_delay=0, max_delay=0)
        sp = xmlsp.StateParser(); sp.add_generator(gen)
        s = await sp.run(timeout=2)
        self.assertEqual(s, {"a": 1, "b": 2})

    async def test_incomplete_parent_no_merge(self):
        gen = xmlsp.create_generator("<foo><bar>1</bar>", min_delay=0, max_delay=0)
        sp = xmlsp.StateParser(); sp.add_generator(gen)
        s = await sp.run(timeout=2)
        self.assertEqual(s, {})

    async def test_mismatched_tags_raises_parseerror(self):
        gen = xmlsp.create_generator("<foo><bar>1</baz></foo>", min_delay=0, max_delay=0)
        sp = xmlsp.StateParser(); sp.add_generator(gen)
        with self.assertRaises(ParseError):
            await sp.run(timeout=2)

    async def test_timeout_partial_state(self):
        async def slow_gen():
            yield "<x>1</x>"
            await asyncio.sleep(0.8)
            yield "<y>2</y>"
        sp = xmlsp.StateParser(); sp.add_generator(slow_gen())
        s = await sp.run(timeout=0.2)
        self.assertIn(s, ({"x": 1}, {"x": 1, "y": 2}))

    async def test_concurrency_many_generators(self):
        async def make_g(val, delay):
            async def g():
                await asyncio.sleep(delay)
                yield f"<k>{val}</k>"
            return g()
        gens = [await make_g(i, 0.01*i) for i in range(10)]
        sp = xmlsp.StateParser()
        for g in gens: sp.add_generator(g)
        s = await sp.run(timeout=5)
        self.assertEqual(s, {"k": 9})

    async def test_random_chunk_branch(self):
        gen = xmlsp.create_generator("<a>1</a><a>2</a><a>5</a>", min_delay=0, max_delay=0)
        sp = xmlsp.StateParser(); sp.add_generator(gen)
        s = await sp.run(timeout=2)
        self.assertEqual(s, {"a": 5})

    async def test_clean_eof_vs_mismatch_vs_truncation(self):
        sp1 = xmlsp.StateParser(); sp1.add_generator(xmlsp.create_generator("<foo>test</foo>", min_delay=0, max_delay=0))
        s1 = await sp1.run(timeout=2)
        self.assertEqual(s1, {"foo": "test"})
        sp2 = xmlsp.StateParser(); sp2.add_generator(xmlsp.create_generator("<foo>test</foo></bar>", min_delay=0, max_delay=0))
        with self.assertRaises(ParseError):
            await sp2.run(timeout=2)
        sp3 = xmlsp.StateParser(); sp3.add_generator(xmlsp.create_generator("<foo>test", min_delay=0, max_delay=0))
        s3 = await sp3.run(timeout=2)
        self.assertEqual(s3, {})

if __name__ == "__main__":
    unittest.main(verbosity=2)
