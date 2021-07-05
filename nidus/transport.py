import struct


# TODO: SSL
class MsgTransport:
    def __init__(self, sock):
        self.sock = sock
        self.buff = b""
        self.size = None
        self.recvd_msg = None

    def send_message(self, msg: bytes):
        size_prefix = struct.pack(">L", len(msg))
        self.sock.sendall(size_prefix + msg)

    def recv_message(self):
        while not self.recvd_msg:
            data = self.sock.recv(1024)
            if not data:
                return b""
            self.buff += data

            if self.size is None and len(self.buff) >= 4:
                self.size = struct.unpack(">L", self.buff[:4])[0]
                self.buff = self.buff[4:]

            if self.size and len(self.buff) >= self.size:
                self.recvd_msg = self.buff[: self.size]
                self.buff = self.buff[self.size :]
                self.size = None

        msg = self.recvd_msg
        self.recvd_msg = None
        return msg
