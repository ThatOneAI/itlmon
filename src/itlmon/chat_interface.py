import asyncio
import threading
from typing import Any, Coroutine
from textual import events
from textual.app import App, ComposeResult
from textual.widgets import Static, Button, Input
from textual.widget import Widget
from textual.reactive import reactive


class ChannelLink(Static):
    DEFAULT_CSS = """
    ChannelLink {
        width: 100%;
    }

    .channel-link-active {
        background: #00ff00;
        text-style: bold;
    }

    .channel-link-messages {
        color: #00ffff;
        link-color: #00ff00;
    }
    """

    active_link = None

    def __init__(self, name, onchange, *args, active=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.channel_name = name
        self.onchange = onchange
        if active:
            self.action_open_channel()

    def render(self) -> str:
        return f'- [@click="open_channel"]{self.channel_name}[/]'

    def action_open_channel(self) -> None:
        if ChannelLink.active_link is not None:
            ChannelLink.active_link.remove_class("channel-link-active")
        ChannelLink.active_link = self
        self.remove_class("channel-link-messages")
        self.add_class("channel-link-active")
        self.onchange(self.channel_name)

    def set_pending_messages(self):
        if not self.has_class("channel-link-active"):
            self.add_class("channel-link-messages")


class ChannelList(Widget):
    DEFAULT_CSS = """
        ChannelList {
            overflow-y: auto;
            padding-left: 1;
            padding-right: 1;
        }
    """

    def __init__(self, onchange, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.onchange = onchange
        self.channels = []
        self.active_channel = None

    def compose(self) -> ComposeResult:
        for channel in self.channels:
            yield channel

    def add_channel(self, name, active=False):
        new_channel = ChannelLink(name, self.onchange, active=active)
        self.channels.append(new_channel)
        if self.is_attached:
            self.mount(new_channel)

    def cycle_down(self):
        for i, channel in enumerate(self.channels):
            if channel == ChannelLink.active_link:
                activate_index = (i + 1) % len(self.channels)
                self.channels[activate_index].action_open_channel()
                return
        if self.channels:
            self.channels[0].action_open_channel()

    def cycle_up(self):
        for i, channel in enumerate(self.channels):
            if channel == ChannelLink.active_link:
                activate_index = (i - 1) % len(self.channels)
                self.channels[activate_index].action_open_channel()
                return

        if self.channels:
            self.channels[0].action_open_channel()

    def set_pending_messages(self, channel_name):
        for channel in self.channels:
            if channel.channel_name == channel_name:
                channel.set_pending_messages()
                return


class Message(Static):
    def __init__(self, channel, message, *args, **kwargs):
        super().__init__(f"[b]{channel}:[/b] {message}", *args, **kwargs)
        self.channel = channel
        self.message = message


class MessageView(Widget):
    DEFAULT_CSS = """
        MessageView {
            overflow-y: auto;
            margin-bottom: 2;
        }
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.counter = reactive(0)

    def compose(self) -> ComposeResult:
        yield Static("")

    def add_message(self, channel, message):
        new_message = Message(channel, message)
        self.mount(new_message)
        new_message.scroll_visible()
        return new_message


class InputBox(Input):
    def __init__(self, border_title, on_submit, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_submit = on_submit
        self.border_title = border_title
        self.history = [""]
        self.current_index = 0

    async def on_key(self, event: events.Key) -> None:
        self.history[self.current_index] = self.value

        if event.character == "\r":
            if self.value == "":
                return
            await self.on_submit(self.value)
            if len(self.history) < 2 or (self.value != self.history[-2]):
                self.history[-1] = self.value
                self.history.append("")
            else:
                self.history[-1] = ""
            self.current_index = len(self.history) - 1
            self.value = ""

        if event.key == "up":
            self.current_index = max(self.current_index - 1, 0)
            self.value = self.history[self.current_index]
        elif event.key == "down":
            self.current_index = min(self.current_index + 1, len(self.history) - 1)
            self.value = self.history[self.current_index]


class ChannelView(Widget):
    DEFAULT_CSS = """
        ChannelView {
            padding-left: 1;
            padding-right: 1;
        }

        ChannelView > MessageView {
            dock: top;
            margin-bottom: 4;
        }

        ChannelView > InputBox {
            dock: bottom;
        }
    """
    last_message = None

    def __init__(self, channel_name, message_handler=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message_view = MessageView()
        self.input = InputBox(channel_name, self._handle_message)
        self.channel_name = channel_name
        self.message_handler = message_handler

    def compose(self) -> ComposeResult:
        yield self.message_view
        yield self.input

    def add_message(self, text):
        self.last_message = self.message_view.add_message(self.channel_name, text)

    async def _handle_message(self, text):
        if self.message_handler is not None:
            await self.message_handler(self.channel_name, text)
        # else:
        # self.add_message(text)


class ChatInterface(App):
    DEFAULT_CSS = """
    #channel_list {
        background: $boost;
        width: 20%;
        height: 100%;
        dock: left;
    }

    .channel-invisible {
        display: none;
    }
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.channel_list = ChannelList(self.switch_channel, id="channel_list")
        self.channel_views = {}
        self.current_view = None
        self.current_channel = None

        self.add_channel("#system", active=True)

        self.command_handlers = {}
        self.message_handlers = {}
        self._handler_queue = asyncio.Queue()

    def compose(self) -> ComposeResult:
        yield self.channel_list

        for view in self.channel_views.values():
            yield view

    def on_key(self, event: events.Key) -> None:
        if event.key == "ctrl+down":
            self.channel_list.cycle_down()
        elif event.key == "ctrl+up":
            self.channel_list.cycle_up()
        else:
            self.current_view.input.on_key(event)

    def add_channel(self, channel_name, active=False):
        new_channel = ChannelView(
            channel_name,
            message_handler=self.handle_message,
            classes="channel-invisible",
        )
        if active:
            self.current_view = new_channel
            self.current_channel = channel_name

        self.channel_views[channel_name] = new_channel
        self.channel_list.add_channel(channel_name, active=active)
        if self.is_running:
            self.mount(new_channel)

    def switch_channel(self, new_channel):
        self.current_view.add_class("channel-invisible")
        self.current_view: ChannelView = self.channel_views[new_channel]
        self.current_view.remove_class("channel-invisible")

        if self.current_view.last_message != None:
            self.current_view.last_message.scroll_visible(animate=False, force=True)
        self.current_view.input.focus()
        self.current_channel = new_channel

    def display_message(self, channel, message):
        if not self.is_running:
            return

        if threading.current_thread().ident != self._thread_id:
            self.call_from_thread(self.channel_views[channel].add_message, message)
            self.call_from_thread(self.channel_list.set_pending_messages, channel)
        else:
            self.channel_views[channel].add_message(message)
            self.channel_list.set_pending_messages(channel)

    async def handle_command(self, channel, message):
        command = message.split()[0]
        if command in self.command_handlers:
            contents = message[len(command) :].strip()
            for f in self.command_handlers.get(None, []):
                await f(channel, contents)
            for f in self.command_handlers.get(command, []):
                await f(channel, contents)

    async def handle_message(self, channel, message):
        if message.startswith("/"):
            await self.handle_command(channel, message)
            return

        for f in self.message_handlers.get(None, []):
            await f(channel, message)
        for f in self.message_handlers.get(channel, []):
            await f(channel, message)

    def oncommand(self, keyword=None):
        def decorator(func):
            self.command_handlers.setdefault(keyword, []).append(func)
            return func

        return decorator

    def onmessage(self, channel=None):
        def decorator(func):
            self.message_handlers.setdefault(channel, []).append(func)
            return func

        return decorator


if __name__ == "__main__":
    app = ChatInterface()
    app.run()
