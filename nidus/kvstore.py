from collections import defaultdict


class KVStore:
    def __init__(self):
        self.buckets = defaultdict(dict)

    def get(self, bucket, key):
        return self.buckets[bucket].get(key)

    def set(self, bucket, key, value):
        self.buckets[bucket][key] = value
        return "OK"

    def delete(self, bucket, key):
        try:
            del self.buckets[bucket][key]
        except KeyError:
            return "NO_KEY"
        return "OK"

    def delete_bucket(self, bucket):
        if bucket not in self.buckets:
            return "NO_BUCKET"
        del self.buckets[bucket]
        return "OK"

    def keys(self, bucket):
        return list(self.buckets[bucket].keys())

    def list_buckets(self):
        return list(self.buckets.keys())

    def apply(self, item):
        try:
            assert len(item) > 0
            command = item[0].upper()

            if command == "GET":
                assert len(item[1:]) == 2
                bucket, key = item[1:]
                return self.get(bucket, key)
            elif command == "SET":
                assert len(item[1:]) == 3
                bucket, key, value = item[1:]
                return self.set(bucket, key, value)
            elif command == "DEL":
                assert len(item[1:]) == 2
                bucket, key = item[1:]
                return self.delete(bucket, key)
            elif command == "DELBUCKET":
                assert len(item[1:]) == 1
                return self.delete_bucket(item[1])
            elif command == "KEYS":
                assert len(item[1:]) == 1
                return self.keys(item[1])
            elif command == "BUCKETS":
                assert len(item[1:]) == 0
                return self.list_buckets()
            else:
                return "NO_CMD"
        except AssertionError:
            return "BAD_ARGS"
