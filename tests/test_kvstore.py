from unittest import TestCase

from nidus.kvstore import KVStore


class KVStoreTestCases(TestCase):
    def test_set(self):
        store = KVStore()

        self.assertEqual(store.set("foo_bucket", "foo", "bar"), "OK")
        self.assertEqual(store.set("foo_bucket", "hello", "world"), "OK")
        self.assertEqual(store.set("stuff", "thing", "item"), "OK")

        self.assertEqual(
            store.buckets,
            {
                "foo_bucket": {"foo": "bar", "hello": "world"},
                "stuff": {"thing": "item"},
            },
        )

    def test_get(self):
        store = KVStore()
        store.set("foo_bucket", "foo", "bar")
        store.set("foo_bucket", "hello", "world")
        store.set("stuff", "thing", "item")

        self.assertEqual(store.get("foo_bucket", "foo"), "bar")
        self.assertEqual(store.get("foo_bucket", "hello"), "world")
        self.assertEqual(store.get("stuff", "thing"), "item")
        self.assertEqual(store.get("doesnt_exist", "key"), None)

    def test_delete(self):
        store = KVStore()
        store.set("foo_bucket", "foo", "bar")
        store.set("foo_bucket", "hello", "world")
        store.set("stuff", "thing", "item")

        self.assertEqual(store.delete("foo_bucket", "foo"), "OK")
        self.assertEqual(
            store.buckets,
            {"foo_bucket": {"hello": "world"}, "stuff": {"thing": "item"}},
        )
        self.assertEqual(store.delete("stuff", "thing"), "OK")
        self.assertEqual(store.buckets, {"foo_bucket": {"hello": "world"}, "stuff": {}})
        self.assertEqual(store.delete("foo_bucket", "hello"), "OK")
        self.assertEqual(store.buckets, {"foo_bucket": {}, "stuff": {}})
        self.assertEqual(store.delete("foo_bucket", "hello"), "NO_KEY")

    def test_delete_bucket(self):
        store = KVStore()
        store.set("foo_bucket", "foo", "bar")
        store.set("foo_bucket", "hello", "world")
        store.set("stuff", "thing", "item")

        self.assertEqual(store.delete_bucket("foo_bucket"), "OK")
        self.assertEqual(
            store.buckets,
            {"stuff": {"thing": "item"}},
        )
        self.assertEqual(store.delete_bucket("stuff"), "OK")
        self.assertEqual(store.buckets, {})
        self.assertEqual(store.delete_bucket("foo_bucket"), "NO_BUCKET")

    def test_keys(self):
        store = KVStore()
        store.set("foo_bucket", "foo", "bar")
        store.set("foo_bucket", "hello", "world")
        store.set("stuff", "thing", "item")

        self.assertEqual(store.keys("foo_bucket"), ["foo", "hello"])
        self.assertEqual(store.keys("stuff"), ["thing"])
        self.assertEqual(store.keys("other"), [])

    def test_buckets(self):
        store = KVStore()
        store.set("foo_bucket", "foo", "bar")
        store.set("foo_bucket", "hello", "world")
        store.set("stuff", "thing", "item")

        self.assertEqual(store.list_buckets(), ["foo_bucket", "stuff"])

    def test_apply(self):
        store = KVStore()

        self.assertEqual(store.apply(["SET", "foo_bucket", "foo", "bar"]), "OK")
        self.assertEqual(store.apply(["GET", "foo_bucket", "foo"]), "bar")
        self.assertEqual(store.apply(["GET", "foo_bucket", "baz"]), None)
        self.assertEqual(store.apply(["KEYS", "foo_bucket"]), ["foo"])
        self.assertEqual(store.apply(["BUCKETS"]), ["foo_bucket"])
        self.assertEqual(store.apply(["DEL", "foo_bucket", "foo"]), "OK")
        self.assertEqual(store.apply(["DEL", "foo_bucket", "baz"]), "NO_KEY")
        self.assertEqual(store.apply(["DELBUCKET", "foo_bucket"]), "OK")
        self.assertEqual(store.apply(["DELBUCKET", "foo_bucket"]), "NO_BUCKET")
        self.assertEqual(store.buckets, {})

        self.assertEqual(store.apply(["FAKE", "foo_bucket"]), "NO_CMD")
        self.assertEqual(store.apply(["GET", "foo_bucket"]), "BAD_ARGS")
        self.assertEqual(store.apply(["SET", "foo_bucket", "baz"]), "BAD_ARGS")
        self.assertEqual(store.apply(["DEL"]), "BAD_ARGS")
        self.assertEqual(store.apply([]), "BAD_ARGS")
