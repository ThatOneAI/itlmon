import argparse

from itllib import Itl
from .chat_interface import ChatInterface
from .stream_interface import StreamInterface
from .directory_watcher import DirectoryWatcher
from .cluster_interface import ClusterInterface

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
itl.apply_config(args.config, args.secrets)

chat = ChatInterface()

# Display the terminal interface
streams = StreamInterface(itl, chat)
clusters = ClusterInterface(itl, chat)


@chat.oncommand("/help")
async def help_handler(channel, msg):
    chat.display_message("#system", "Available commands:")
    for command in chat.command_handlers:
        chat.display_message("#system", command)


@chat.oncommand("/quit")
async def quit_handler(channel, msg):
    chat.exit()


# This blocks until the chat is closed
itl.start_thread()
clusters.start()
chat.run()

# Clean up
clusters.stop()
itl.stop_thread()
