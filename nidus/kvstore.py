class KVStore:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data[key]

    def set(self, key, value):
        self.data[key] = value
        return "OK"

    def delete(self, key):
        del self.data[key]
        return "OK"

    def apply(self, item):
        command = item[0].upper()
        if command == "GET":
            return self.get(item[1])
        elif command == "DEL":
            return self.delete(item[1])
        elif command == "SET":
            return self.set(item[1], item[2])
