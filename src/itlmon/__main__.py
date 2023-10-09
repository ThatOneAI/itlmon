import argparse
import os
import random
import yaml

import curses

from itllib import Itl
from .stream_interface import StreamInterface
from .directory_watcher import DirectoryWatcher

parser = argparse.ArgumentParser(description="Upload files to S3")
parser.add_argument(
    "--secrets",
    default="./secrets",
    type=str,
    help="Directory to search for secret keys.",
)
parser.add_argument(
    "--config",
    default="./config.yaml",
    type=str,
    help="File containing the service configuration.",
)
args = parser.parse_args()


# Connect to the loop
itl = Itl()

with open(args.config) as inp:
    config = yaml.safe_load(inp)

itl.apply_config(config, args.secrets)

streams = [x['name'] for x in config['streams']]
itl.upstreams(streams)
itl.downstreams(streams)

itl.start_thread()

# Display the terminal interface
chat = StreamInterface(itl, streams)

@chat.onmessage()
async def send_message(channel, message):
    if channel not in streams:
        return
    await itl.stream_send(channel, message)

chat.start()

itl.stop_thread()
