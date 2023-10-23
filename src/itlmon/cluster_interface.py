import json
import os
import yaml
import shlex
from .directory_watcher import DirectoryWatcher
from .chat_interface import ChatInterface
from itllib import Itl

from .command_parser import CommandParser


def _split_path(path: str, base: str):
    path = os.path.splitext(path)[0]
    relpath = os.path.relpath(path, base)
    parts = relpath.split(os.path.sep)
    if len(parts) != 5:
        return None, None, None, None, None
    return parts


def _get_relevant_clusters(itl: Itl, database: str, name: str):
    for cluster, cluster_obj in itl._clusters.items():
        if cluster_obj.database.name != database:
            continue
        if name.startswith(cluster_obj.prefix):
            yield cluster


def compare(a, b):
    if a == b:
        return True
    if isinstance(a, dict) and isinstance(b, dict):
        if len(a) != len(b):
            return False
        for key in a:
            if key not in b:
                return False
            if not compare(a[key], b[key]):
                return False
        return True
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return False
        for i in range(len(a)):
            if not compare(a[i], b[i]):
                return False
        return True
    return False


class ClusterInterface:
    def __init__(self, itl: Itl, chat: ChatInterface, directory="./clusters"):
        self.itl = itl
        self.chat = chat
        self.directory = os.path.normpath(directory)
        self.watcher = DirectoryWatcher(self.directory)
        self.remote_state = {}

        self.cluster_parser = CommandParser(self.chat, prog="/cluster", add_help=False)
        self.cluster_parser.add_argument(
            "command",
            type=str,
            choices=["get", "apply", "delete"],
            help="Command to run",
        )
        self.cluster_parser.add_argument(
            "cluster", type=str, help="Name of the cluster, from config.yaml"
        )

        self.cluster_get_parser = CommandParser(self.chat, prog="/cluster get cluster")
        self.cluster_get_parser.add_argument(
            "--group", type=str, required=False, help="Name of the resource group"
        )
        self.cluster_get_parser.add_argument(
            "--version", type=str, required=False, help="Version of the resource group"
        )
        self.cluster_get_parser.add_argument(
            "--kind", type=str, required=False, help="Kind of the resource"
        )
        self.cluster_get_parser.add_argument(
            "--name", type=str, required=False, help="Name of the resource"
        )

        self.cluster_apply_parser = CommandParser(
            self.chat, prog="/cluster apply cluster"
        )
        self.cluster_apply_parser.add_argument(
            "path",
            type=str,
            help="Path to the yaml resource file to apply, or a folder to apply recursively",
        )

        self.cluster_delete_parser = CommandParser(
            self.chat, prog="/cluster delete cluster"
        )
        self.cluster_delete_parser.add_argument(
            "--group", type=str, required=True, help="Name of the resource group"
        )
        self.cluster_delete_parser.add_argument(
            "--version", type=str, required=True, help="Version of the resource group"
        )
        self.cluster_delete_parser.add_argument(
            "--kind", type=str, required=True, help="Kind of the resource"
        )
        self.cluster_delete_parser.add_argument(
            "--name", type=str, required=True, help="Name of the resource"
        )

        self._attach_fs_handlers()
        self._attach_clusters()
        self._attach_command_handler()

    async def _apply(self, cluster, data, force=False):
        database = self.itl._clusters[cluster].database.name
        if "apiVersion" not in data or data["apiVersion"].count("/") != 1:
            self.chat.display_message(
                "#system",
                f"Failed to parse apiVersion: must be in the form group/version",
            )
            return
        group, version = data["apiVersion"].split("/")
        if "kind" not in data:
            self.chat.display_message("#system", f"The config must have a 'kind' field")
            return
        kind = data["kind"]
        if "metadata" not in data or "name" not in data["metadata"]:
            self.chat.display_message(
                "#system", f"The config must have a 'metadata.name' field"
            )
            return
        name = data["metadata"]["name"]
        key = os.path.join(database, group, version, kind, name)
        if force or key not in self.remote_state:
            self.chat.display_message("#system", f"Updating remote {key}")
            await self.itl.resource_apply(cluster, data)
            return

        remote_contents = self.remote_state[key]
        if remote_contents == data:
            return
        self.chat.display_message("#system", f"Updating remote {key}")
        await self.itl.resource_apply(cluster, data)

    async def _delete_remote(self, cluster, group, version, kind, name, force=False):
        database = self.itl._clusters[cluster].database.name
        key = os.path.join(database, group, version, kind, name)
        if not force:
            if key not in self.remote_state or self.remote_state[key] == None:
                return
        self.chat.display_message("#system", f"Deleting remote {key}")
        await self.itl.resource_delete(cluster, group, version, kind, name)

    def _attach_fs_handlers(self):
        @self.watcher.onput(r".*")
        async def local_file_written(relpath, **kwargs):
            if not relpath.endswith(".yaml"):
                return
            if not os.path.exists(os.path.join(self.directory, relpath)):
                self.chat.display_message("#system", f"Can't find {relpath}")
                return
            cluster, group, version, kind, name = _split_path(relpath, "")
            # check if the remote file is different from the local one
            try:
                with open(os.path.join(self.directory, relpath)) as inp:
                    data = yaml.safe_load(inp)
            except:
                self.chat.display_message(
                    "#system", f"Failed to load yaml from {relpath}"
                )
                return

            try:
                return await self._apply(cluster, data)
            except Exception as e:
                self.chat.display_message("#system", f"Failed to apply {relpath}: {e}")
                return

        @self.watcher.ondelete(r".*")
        async def local_file_deleted(relpath, **kwargs):
            if not relpath.endswith(".yaml"):
                return
            if os.path.exists(os.path.join(self.directory, relpath)):
                return
            cluster, group, version, kind, name = _split_path(relpath, "")
            if group == None:
                self.chat.display_message("#system", f"Failed to parse path {relpath}")
                return
            return await self._delete_remote(cluster, group, version, kind, name)

    async def _get(self, database, group, version, kind, name, data=None):
        key = os.path.join(database, group, version, kind, name)
        for cluster in _get_relevant_clusters(self.itl, database, name):
            # check if the local file should be updated
            if data == None:
                data = await self.itl.resource_read(cluster, group, version, kind, name)
            if data == None:
                self.chat.display_message(
                    "#system",
                    f"Failed to get {cluster}/{group}/{version}/{kind}/{name}",
                )
                del self.remote_state[key]
                return
            path = f"{os.path.join(self.directory, cluster, group, version, kind, name)}.yaml"
            self.remote_state[key] = data
            os.makedirs(os.path.dirname(path), exist_ok=True)
            try:
                with open(path) as inp:
                    local_data = yaml.safe_load(inp)
            except:
                local_data = None
            if local_data == data:
                return
            self.chat.display_message("#system", f"Updating local {path}")
            with open(path, "w") as out:
                yaml.dump(data, out, indent=2)

    async def _delete_local(self, database, group, version, kind, name):
        key = os.path.join(database, group, version, kind, name)
        for cluster in _get_relevant_clusters(self.itl, database, name):
            path = f"{os.path.join(self.directory, cluster, group, version, kind, name)}.yaml"
            self.remote_state[key] = None
            if not os.path.exists(path):
                return
            self.chat.display_message("#system", f"Deleting local {path}")
            os.remove(path)

    def _create_handler(self):
        async def remote_config_changed(
            event, url, database, group, version, kind, name, timestamp, **kwargs
        ):
            if event == "put":
                await self._get(database, group, version, kind, name)
            elif event == "delete":
                await self._delete_local(database, group, version, kind, name)

        return remote_config_changed

    def _attach_command_handler(self):
        @self.chat.oncommand("/cluster")
        async def cluster_handler(channel, msg):
            args = shlex.split(msg)
            try:
                args, rest = self.cluster_parser.parse_known_args(args)
                cluster = args.cluster
                database = self.itl._clusters[cluster].database.name
                if args.command == "get":
                    args = self.cluster_get_parser.parse_args(rest)
                    self.chat.display_message(
                        "#system",
                        f"Getting {cluster}/{args.group}/{args.version}/{args.kind}/{args.name}",
                    )
                    results = await self.itl.resource_read_all(
                        cluster, args.group, args.version, args.kind, args.name
                    )
                    if not results:
                        self.chat.display_message(
                            "#system",
                            f"No results found for {cluster}/{args.group}/{args.version}/{args.kind}/{args.name}",
                        )
                        return
                    self.chat.display_message(
                        "#system", f"Found {len(results)} results"
                    )
                    for result in results:
                        self.chat.display_message(
                            "#system",
                            f"Getting {cluster}/{result['group']}/{result['version']}/{result['kind']}/{result['name']}",
                        )
                        self.chat.display_message(
                            "#system", f"Config: {json.dumps(result['config'])}"
                        )
                        await self._get(
                            database,
                            result["group"],
                            result["version"],
                            result["kind"],
                            result["name"],
                            result["config"],
                        )
                elif args.command == "apply":
                    args = self.cluster_apply_parser.parse_args(rest)
                    with open(args.path) as inp:
                        data = yaml.safe_load(inp)
                    await self._apply(cluster, data, force=True)
                elif args.command == "delete":
                    args = self.cluster_delete_parser.parse_args(rest)
                    await self._delete_remote(
                        cluster,
                        args.group,
                        args.version,
                        args.kind,
                        args.name,
                        force=True,
                    )
            except SystemExit:
                return

    def _attach_clusters(self):
        for cluster_obj in self.itl._clusters.values():
            self.itl.ondata(cluster_obj.stream)(self._create_handler())

    def start(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory, exist_ok=True)

        self.watcher.start()

    def stop(self):
        self.watcher.stop()
