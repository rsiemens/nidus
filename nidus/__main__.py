import argparse
import queue

from nidus.actors import Actor, get_system
from nidus.config import load_user_config
from nidus.kvstore import KVStore
from nidus.messages import ClientRequest
from nidus.raft import RaftNetwork

response = queue.Queue()


class Client(Actor):
    def handle_client_response(self, res):
        response.put(res.result)


def main():
    parser = argparse.ArgumentParser(description="Start a node or run a client command")
    parser.add_argument(
        "-c", "--config", help="Configuration file to be used for the cluster"
    )
    parser.add_argument(
        "-l", "--leader", help="The leader address for a client command"
    )
    parser.add_argument(
        "name", nargs="*", help="Name or command if --leader flag is provided"
    )
    args = parser.parse_args()

    if args.leader:
        host, port = args.leader.split(":")

        actor_system = get_system()
        addr = actor_system.create(("localhost", 12345), Client)
        actor_system.send((host, int(port)), ClientRequest(addr, args.name))
        try:
            res = response.get(timeout=5)
        except queue.Empty:
            print("Timeout waiting for response")
        else:
            print(res)
        actor_system.shutdown()
    else:
        config = load_user_config(args.config)
        net = RaftNetwork(config, KVStore)

        nodes = [net.create_node(n) for n in args.name]
        return nodes


nodes = main()
