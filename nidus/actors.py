import json
import logging
import queue
import signal
import sys
from dataclasses import asdict
from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, SOL_SOCKET, socket, timeout
from threading import Event, Thread

from nidus.messages import message_from_payload
from nidus.transport import MsgTransport

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
logger.addHandler(handler)

_system = None


def get_system():
    global _system
    if _system is None:
        _system = TCPSystem()
    return _system


class TCPSystem:
    def __init__(self):
        self._actors = {}
        self._inboxes = {}
        self._server_threads = {}
        self._shutdown_events = {}
        self._evt_loop_shutdown = Event()
        self._event_loop_thread = Thread(
            target=self._event_loop,
            args=[self._evt_loop_shutdown],
            name="EventLoopThread",
        )
        self._event_loop_thread.start()

    def create(self, addr, actor_cls, *args, **kwargs):
        inbox = queue.Queue()
        shutdown_evt = Event()
        server_thread = Thread(
            target=self._tcp_server,
            args=[addr, inbox, shutdown_evt],
            name=f"ServerThread-{addr}",
        )

        self._actors[addr] = actor_cls(*args, **kwargs)
        self._inboxes[addr] = inbox
        self._shutdown_events[addr] = shutdown_evt
        self._server_threads[addr] = server_thread

        server_thread.start()
        return addr

    def destroy(self, addr):
        # sequencing might be tricky here
        server_thread = self._server_threads.pop(addr)
        shutdown_evt = self._shutdown_events.pop(addr)
        shutdown_evt.set()

        self._inboxes.pop(addr)
        actor = self._actors.pop(addr)
        actor.handle_destroy()

        server_thread.join()

    def shutdown(self):
        self._evt_loop_shutdown.set()
        self._event_loop_thread.join()

        for addr in list(self._actors):
            self.destroy(addr)

    def send(self, to, msg):
        try:
            sock = socket(AF_INET, SOCK_STREAM)
            sock.connect(to)
            transport = MsgTransport(sock)
            msg = json.dumps(asdict(msg))
            transport.send_message(msg.encode("utf8"))
        except OSError as ex:
            logger.debug(f"failed to send message to {to}: {ex}")
        else:
            logger.debug(f"sent message to {to}: {msg}")
        pass

    def _tcp_server(self, server_addr, inbox, shutdown_evt):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.settimeout(0.5)
        sock.bind(server_addr)
        sock.listen()

        while not shutdown_evt.is_set():
            try:
                client, addr = sock.accept()

                transport = MsgTransport(client)
                msg = transport.recv_message().decode("utf8")
                logger.debug(f"recieved message from {addr}: {msg}")

                msg = message_from_payload(json.loads(msg))
                inbox.put(msg)
            except timeout:
                pass
            else:
                client.close()

    def _event_loop(self, shutdown_evt):
        while not shutdown_evt.is_set():
            for addr in list(self._inboxes):
                inbox = self._inboxes[addr]
                if not inbox.empty():
                    try:
                        msg = inbox.get_nowait()
                    except queue.Empty:
                        pass
                    else:
                        actor = self._actors[addr]
                        actor._handle_message(msg)


class SyncSystem:

    """Primarily usefull for unit testing purposes"""

    def __init__(self):
        self._actors = {}
        self._inboxes = {}
        self._dead_letter_inbox = queue.Queue()

    def create(self, addr, actor_cls, *args, **kwargs):
        inbox = queue.Queue()
        self._actors[addr] = actor_cls(*args, **kwargs)
        self._inboxes[addr] = inbox
        return addr

    def destroy(self, addr):
        self._inboxes.pop(addr)
        actor = self._actors.pop(addr)
        actor.handle_destroy()

    def shutdown(self):
        for addr in list(self._actors):
            self.destroy(addr)

    def send(self, to, msg):
        try:
            self._inboxes[to].put(msg)
        except KeyError:
            self._dead_letter_inbox.put(msg)

    def flush(self):
        emptied = False
        while not emptied:
            emptied = True

            for addr in list(self._inboxes):
                inbox = self._inboxes[addr]
                if not inbox.empty():
                    try:
                        msg = inbox.get_nowait()
                    except queue.Empty:
                        pass
                    else:
                        emptied = False
                        actor = self._actors[addr]
                        actor._handle_message(msg)


class Actor:
    def _handle_message(self, msg):
        handler = getattr(self, f"handle_{msg.msg_type}")
        handler(msg)

    def handle_destroy(self):
        pass


def _sighandler(signum, frame):
    logger.info("shutting down")
    if _system:
        _system.shutdown()


signal.signal(signal.SIGINT, _sighandler)
signal.signal(signal.SIGTERM, _sighandler)
