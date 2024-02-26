import argparse
import atexit
from collections import defaultdict
from glob import glob
import json
import asyncio
import boto3
import threading
import shlex

import yaml

from .directory_watcher import DirectoryWatcher
from .s3_file_downloader import S3FileDownloader, split_s3_url
from .chat_interface import ChatInterface
from .command_parser import CommandParser
from itllib import Itl


class StreamInterface:
    def __init__(self, itl, chat):
        self.itl: Itl = itl
        self.chat = chat
        self.message_handlers = defaultdict(list)
        self._channel_streams = {}

        self._attach_chat_handlers(self.chat)
        self._attach_streams()

        self.stream_command_parser = CommandParser(
            self.chat, prog="/stream", add_help=False
        )
        self.stream_command_parser.add_argument(
            "stream_name", type=str, help="Name of the stream"
        )
        self.stream_command_parser.add_argument(
            "command",
            type=str,
            choices=["add", "delete"],
            help="Command to run",
        )

        self.stream_add_parser = CommandParser(self.chat, prog="/stream streamName add")
        self.stream_add_parser.add_argument(
            "--loop", type=str, required=False, help="Name of the loop"
        )
        self.stream_add_parser.add_argument(
            "--streamId",
            type=str,
            required=False,
            default=None,
            help="Id of the stream, defaults to stream name",
        )
        self.stream_add_parser.add_argument(
            "--group",
            type=str,
            required=False,
            default=None,
            help="Stream group for load balancing messages",
        )
        self.stream_add_parser.add_argument(
            "--url",
            type=str,
            required=False,
            default=None,
            help="Connect to a stream by URL instead of a loop name and key",
        )

    def onmessage(self, channel=None):
        def decorator(func):
            async def wrapped(*args, **kwargs):
                await func(*args, **kwargs)

            self.message_handlers[channel].append(wrapped)
            return func

        return decorator

    def _create_handler(self, channel_name, stream_identifier):
        self._channel_streams[channel_name] = stream_identifier

        async def ondata(*args, **kwargs):
            if args:
                self.chat.display_message(channel_name, args[0])
            else:
                self.chat.display_message(channel_name, f"/obj {json.dumps(kwargs)}")

        return ondata

    def _attach_streams(self):
        for stream_identifier in self.itl._streams:
            self.itl.ondata(stream_identifier)(
                self._create_handler(stream_identifier, {"stream": stream_identifier})
            )
            self.chat.add_channel(stream_identifier)

    def _attach_chat_handlers(self, chat):
        @chat.onmessage()
        async def message_handler(channel, msg):
            if channel not in self._channel_streams:
                return
            stream_identifier = self._channel_streams[channel]
            chat.display_message("#system", f"sending to {stream_identifier}: {msg}")

            self.itl.stream_send(message=msg, **stream_identifier)

        @chat.oncommand("/obj")
        async def obj_handler(channel, msg):
            if channel not in self._channel_streams:
                return
            try:
                json_data = json.loads(msg)
            except json.JSONDecodeError:
                chat.display_message("#system", f"Invalid JSON: {msg}")
                return
            stream_identifier = self._channel_streams[channel]
            self.itl.stream_send(message=json_data, **stream_identifier)

        @chat.oncommand("/stream")
        async def stream_handler(channel, msg):
            try:
                if not msg:
                    msg = "--help"
                args = shlex.split(msg)

                parsed_args, rest = self.stream_command_parser.parse_known_args(args)
                channel_name = parsed_args.stream_name

                if parsed_args.command == "add":
                    args = self.stream_add_parser.parse_args(rest)
                    if args.loop:
                        if args.loop not in self.itl._loops:
                            chat.display_message(
                                "#system",
                                f"Loop {args.loop} does not exist",
                            )
                            return

                        streamId = args.streamId or stream_identifier
                        stream_identifier = {"loop": args.loop, "streamId": streamId}

                        chat.display_message(
                            "#system", f"Adding stream {args.loop}/{streamId}"
                        )
                    else:
                        chat.display_message(
                            "#system",
                            "Must specify either a loop",
                        )
                        return

                    self.itl.ondata(loop=args.loop, streamId=streamId)(
                        self._create_handler(channel_name, stream_identifier)
                    )
                    self.chat.add_channel(channel_name)

            except SystemExit:
                return

    def display_message(self, channel, msg):
        self.chat.display_message(channel, msg)

    def onput(self, stream_name):
        def decorator(func):
            async def wrapped(*args, **kwargs):
                await func(*args, **kwargs)

            return wrapped

    def ondelete(self, stream_name):
        def decorator(func):
            async def wrapped(*args, **kwargs):
                await func(*args, **kwargs)

            return wrapped

    def ondata(self, stream_name):
        def decorator(func):
            async def wrapped(*args, **kwargs):
                await func(*args, **kwargs)

            return wrapped
