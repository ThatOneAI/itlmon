import argparse
import sys
import io
from contextlib import redirect_stdout, redirect_stderr


class CommandParser(argparse.ArgumentParser):
    def __init__(self, chat, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.chat = chat
        self._args_passed = []

    def parse_args(self, args=None, namespace=None):
        # Store the arguments passed to parse_args
        self._args_passed = args
        if "--help" in args:
            self.error("Help requested")
        return super().parse_args(args, namespace)

    def parse_known_args(self, args=None, namespace=None):
        # Store the arguments passed to parse_known_args
        self._args_passed = args
        return super().parse_known_args(args, namespace)

    def error(self, message):
        with io.StringIO() as buf, redirect_stdout(buf), redirect_stderr(buf):
            if "--help" in self._args_passed:
                self.print_help()
            else:
                self.print_usage()
            message = buf.getvalue()

        self.chat.display_message("#system", message)
        sys.exit(2)
