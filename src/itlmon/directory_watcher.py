from collections import defaultdict
from glob import glob
import asyncio
import re
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import inspect
import threading
import os


class AsyncLooper:
    def __init__(self):
        self._stop = False
        self._messages = asyncio.Queue()
        self.looper = None
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self._run_in_thread)
        self.thread.start()

    def _run_in_thread(self):
        self.looper = asyncio.new_event_loop()
        asyncio.set_event_loop(self.looper)
        self.looper.run_until_complete(self.loop())
        self.looper.close()

    def stop(self):
        self._stop = True
        self.execute(lambda: None)

    def join(self):
        self.thread.join()

    async def loop(self):
        while not self._stop:
            fn, args = await self._messages.get()

            if inspect.iscoroutinefunction(fn):
                await fn(*args)
            elif inspect.isfunction(fn) or inspect.ismethod(fn):
                fn(*args)
            else:
                print(f"Unsupported function type: {type(fn)}")

    def execute(self, callback):
        if not isinstance(callback, tuple) and not isinstance(callback, list):
            callback = (callback, ())
        self.looper.call_soon_threadsafe(lambda: self._messages.put_nowait(callback))


import subprocess


def is_file_open_linux_or_mac(filepath):
    try:
        # Using lsof to check if the file is opened by any process.
        result = subprocess.run(
            ["lsof", filepath],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        lines = result.stdout.splitlines()
        for line in lines:
            # Check the FD column for 'w'
            parts = line.split()
            if len(parts) > 3 and "w" in parts[3]:
                return True

        return False
    except Exception as e:
        print(f"Error checking file {filepath}: {e}")
        return False


def is_file_open_windows(filepath):
    try:
        # Using `handle` to check if the file is opened by any process.
        result = subprocess.run(
            ["handle", filepath],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Check for indicators of write access in the output.
        # This is heuristic-based and may not be 100% reliable.
        write_indicators = ["Write", "ReadWrite", "Append"]
        for indicator in write_indicators:
            if indicator in result.stdout:
                return True

        return False
    except Exception as e:
        print(f"Error checking file {filepath}: {e}")
        return False


import platform

if platform.system() == "Windows":
    is_file_open = is_file_open_windows
else:
    is_file_open = is_file_open_linux_or_mac


class ReadyFileExecutor(AsyncLooper):
    def __init__(self, put=lambda x: None, delete=lambda x: None):
        super().__init__()
        self.pending_operations = {}
        self._put_callback = put
        self._delete_callback = delete
        self._running = False

    async def _run(self):
        for file, fn in list(self.pending_operations.items()):
            if not is_file_open(file):
                self.execute(fn)
                del self.pending_operations[file]

        if self.pending_operations:
            self.looper.call_later(1, self.run)

        self._running = False

    def run(self):
        if self._running:
            return
        self._running = True
        self.execute(self._run)

    def _sync_put(self, file):
        self.pending_operations[file] = self._put_callback, (file,)
        self.run()

    def put(self, file):
        if is_in_progress_download(file):
            return
        self.execute(lambda: self._sync_put(file))

    def _sync_delete(self, file):
        self.pending_operations[file] = self._delete_callback, (file,)
        self.run()

    def delete(self, file):
        if is_in_progress_download(file):
            return
        self.execute(lambda: self._sync_delete(file))


def is_in_progress_download(filename):
    in_progress_extensions = [".crdownload", ".part", ".partial", ".tmp"]
    return any(filename.endswith(ext) for ext in in_progress_extensions)


class WatcherHandler(FileSystemEventHandler):
    def __init__(self, executor):
        self.executor = executor

    def on_created(self, event):
        if event.is_directory:
            return
        self.executor.put(os.path.normpath(event.src_path))

    def on_deleted(self, event):
        if event.is_directory:
            return
        self.executor.delete(os.path.normpath(event.src_path))

    def on_modified(self, event):
        if event.is_directory:
            return
        self.executor.put(os.path.normpath(event.src_path))

    def on_moved(self, event):
        if event.is_directory:
            return
        self.executor.put(os.path.normpath(event.dest_path))
        self.executor.delete(os.path.normpath(event.src_path))


class DirectoryWatcher:
    def __init__(self, directory):
        self.directory = os.path.normpath(directory)
        self._last_updated = {}
        self._put_handlers = defaultdict(list)
        self._delete_handlers = defaultdict(list)

        self.file_watcher = ReadyFileExecutor(put=self.put, delete=self.delete)

        event_handler = WatcherHandler(self.file_watcher)
        self.observer = Observer()
        self.observer.schedule(event_handler, directory, recursive=True)

        os.makedirs(self.directory, exist_ok=True)

    def start(self):
        self.file_watcher.start()
        self.observer.start()

    def stop(self):
        self.observer.stop()
        self.file_watcher.stop()

    def join(self):
        self.observer.join()
        self.file_watcher.thread.join()

    def onput(self, pattern):
        def wrapper(func):
            self._put_handlers[pattern].append(func)
            return func

        return wrapper

    def ondelete(self, pattern):
        def wrapper(func):
            self._delete_handlers[pattern].append(func)
            return func

        return wrapper

    async def put(self, filepath):
        fullpath = os.path.realpath(filepath)
        relpath = os.path.relpath(fullpath, self.directory)

        for pattern in self._put_handlers:
            if re.match(pattern, relpath):
                for handler in self._put_handlers[pattern]:
                    if inspect.iscoroutinefunction(handler):
                        await handler(fullpath=fullpath, relpath=filepath)
                    else:
                        handler(fullpath=fullpath, relpath=filepath)

        # try:
        #     mtime = os.path.getmtime(filepath)
        #     if filepath not in self._last_updated or mtime > self._last_updated[filepath]:
        #         relpath = os.path.relpath(filepath, self.directory)
        #         with open(filepath, 'rb') as f:
        #             response = self.client.put_object(Bucket=self.bucket, Key=self.key_prefix + relpath, Body=f)
        #     self._last_updated[filepath] = mtime
        # except FileNotFoundError:
        #     pass

    async def delete(self, filepath):
        # relpath = os.path.relpath(filepath, self.directory)
        # response = self.client.delete_object(Bucket=self.bucket, Key=self.key_prefix + relpath)
        fullpath = os.path.normpath(filepath)
        relpath = os.path.relpath(filepath, self.directory)
        for pattern in self._delete_handlers:
            if re.match(pattern, relpath):
                for handler in self._delete_handlers[pattern]:
                    if inspect.iscoroutinefunction(handler):
                        await handler(fullpath=fullpath, relpath=filepath)
                    else:
                        handler(fullpath=fullpath, relpath=filepath)
