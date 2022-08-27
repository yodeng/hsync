import os
import re
import sys
import time
import signal
import base64
import logging
import socket
import struct
import atexit
import hashlib
import asyncio
import argparse
import functools
import subprocess

from copy import deepcopy
from fnmatch import fnmatch
from threading import Thread
from multiprocessing import cpu_count, current_process, get_logger, Pool

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, as_completed

from tqdm import tqdm
from aiohttp import ClientSession, TCPConnector, ClientTimeout, web, hdrs
from aiohttp.client_reqrep import ClientRequest
from aiohttp.client_exceptions import *

from ._version import __version__


class exitSync(Thread):
    def __init__(self, obj=None, daemon=True):
        super(exitSync, self).__init__(daemon=daemon)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.obj = obj

    def run(self):
        time.sleep(1)

    def signal_handler(self, signum, frame):
        self.obj.write_offset()
        self.obj.loger.debug("Update %s before exit",
                             os.path.basename(self.obj.rang_file))
        sys.exit(signum)


class HsyncLog(object):

    @property
    def loger(self):
        return logging.getLogger()


class HsyncDecorator(object):

    def check_ipaddres(func):
        async def wrapper(self, *args, **kwargs):
            if not self.conf.info.hsyncd.Allowed_host or "*" in self.conf.info.hsyncd.Allowed_host.split():
                pass
            else:
                for ip in split_values(self.conf.info.hsyncd.Allowed_host):
                    if fnmatch(self.request.remote, ip):
                        break
                else:
                    self.loger.error(
                        "Ip %s forbidden, only allow %s", self.request.remote, self.conf.info.hsyncd.Allowed_host)
                    return web.HTTPForbidden()
            res = await func(self, *args, **kwargs)
            return res
        return wrapper

    def check_filepath(func):
        async def wrapper(self, *args, **kwargs):
            data = await self.request.json()
            query = dict(data)
            file_path = query.get('path')
            file_path = os.path.abspath(file_path)
            if os.path.isfile(file_path):
                filename = os.path.basename(file_path)
                for pt in split_values(self.conf.info.hsyncd.Forbidden_file):
                    pt = pt.strip('"').strip("'").strip().strip(',')
                    if fnmatch(filename, pt.strip()):
                        self.loger.error(
                            "%s file forbidden in %s", file_path, self.conf.info.hsyncd.Forbidden_file)
                        return web.HTTPForbidden()
                for ex_dir in split_values(self.conf.info.hsyncd.Forbidden_dir):
                    ex_dir = ex_dir.strip().strip(',').strip()
                    if file_path.startswith(ex_dir):
                        self.loger.error(
                            "%s file forbidden in %s", file_path, self.conf.info.hsyncd.Forbidden_dir)
                        return web.HTTPForbidden()
            else:
                return web.HTTPForbidden()
            self.hsync_file_path = file_path
            res = await func(self, *args, **kwargs)
            return res
        return wrapper


class KeepAliveClientRequest(ClientRequest):
    async def send(self, conn: "Connection") -> "ClientResponse":
        sock = conn.protocol.transport.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
        return (await super().send(conn))


default_headers = {
    "Connection": "close",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
}

HSYNC_DIR = os.getenv("HSYNC_DIR", False) or os.path.join(
    os.path.expanduser("~"), ".hsync")

ReloadException = (
    ServerDisconnectedError,
    OSError,
    ClientPayloadError,
    # ClientOSError,
)


def max_async():
    max_as = 50
    n_cpu = cpu_count()
    if n_cpu > 60:
        max_as = Chunk.MAX_AS
    elif n_cpu > 30:
        max_as = 200
    elif n_cpu > 20:
        max_as = 100
    elif n_cpu > 10:
        max_as = 50
    else:
        max_as = 20
    return max(max_as, 1)


def get_as_part(filesize):
    mn_as, mn_pt = 10, 20
    min_as, min_pt = 10, 10
    if filesize > 1 * Chunk.OneG:
        mn_pt = Chunk.MAX_PT
    elif filesize > 500 * Chunk.OneM:
        mn_pt = filesize // (Chunk.OneM*1.3)
    elif filesize > 100 * Chunk.OneM:
        mn_pt = filesize // (Chunk.OneM * 1.2)
    else:
        mn_pt = filesize // (Chunk.OneM * 2)
    mn_pt = min(mn_pt, Chunk.MAX_PT)
    mn_as = max_async()
    mn_as = min(mn_as, mn_pt)
    return max(int(mn_as), min_as), max(int(mn_pt), min_pt)


class Chunk(object):
    OneK = 1024
    OneM = OneK * OneK
    OneG = OneM * OneK
    OneT = OneG * OneK
    MAX_AS = 300
    MAX_PT = 1000


class TimeoutException(Exception):
    pass


class ServerForbidException(Exception):
    pass


def human_size(num):
    for unit in ['B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s" % (num, unit)
        num /= 1024.0
    return "%.1f%s" % (num, 'Y')


def split_values(s):
    if s is None:
        return []
    out = []
    j = ""
    for i in s:
        if i.startswith("#"):
            break
        if i == "," or re.match("\s", i):
            if j:
                out.append(j)
            j = ""
        else:
            j += i
    if j:
        out.append(j)
    return out


def query_parse(req):
    queryitem = {}
    queryitem.update(req.query.items())
    return queryitem


def hsyncdArg():
    parser = argparse.ArgumentParser(
        description="hsyncd server daemon")
    parser.add_argument("-host", "--host-ip", dest="Host_ip", type=str,
                        help='hsyncd server host ip, 0.0.0.0 by default', metavar="<str>")
    parser.add_argument("-p", "--port", dest="Port", type=int,
                        help='hsyncd port 10808 by default',  metavar="<int>")
    parser.add_argument("-l", "--log", type=str,
                        help='hsyncd logging file, %s by default' % os.path.join(HSYNC_DIR, "hsyncd.log"),  metavar="<str>")
    parser.add_argument("-d", "--daemon", action='store_true',
                        help="daemon process", default=False)
    return parser.parse_known_args()


def hsyncArg():
    parser = argparse.ArgumentParser(
        description="hsync for remote file synchronize")
    parser.add_argument("-i", "--input", type=str, required=True,
                        help='input remote path', metavar="<str>")
    parser.add_argument("-host", "--host-ip", dest="Host_ip", type=str,
                        help='connect host ip, localhost by default', metavar="<str>")
    parser.add_argument("-p", "--port", dest="Port", type=int,
                        help='connect port, 10808 by default',  metavar="<int>")
    parser.add_argument("-o", "--output", type=str,
                        help="output path", metavar="<str>")
    return parser.parse_args()


def hscpArg():
    parser = argparse.ArgumentParser(
        description="hscp for remote file copy")
    parser.add_argument("-i", "--input", type=str, required=True,
                        help='input remote path', metavar="<str>")
    parser.add_argument("-host", "--host-ip", dest="Host_ip", type=str,
                        help='connect host ip, localhost by default', metavar="<str>")
    parser.add_argument("-p", "--port", dest="Port", type=int,
                        help='connect port, 10808 by default',  metavar="<int>")
    parser.add_argument("-n", "--num", type=int,
                        help='max file copy in parallely, 3 by default', default=3, metavar="<int>")
    parser.add_argument("-o", "--output", type=str,
                        help="output path", metavar="<str>")
    return parser.parse_args()


def mk_hsync_args(args=None, conf=None, name=None, default=None):
    arg_value = getattr(conf, name, "")
    if getattr(args, name, ""):
        arg_value = getattr(args, name)
    if not arg_value:
        arg_value = default
    return arg_value


def loger(logfile=None, level="info", multi=False):
    if multi:
        logger = get_logger()
    else:
        logger = logging.getLogger()
    if level.lower() == "info":
        logger.setLevel(logging.INFO)
        f = logging.Formatter(
            '[%(levelname)s %(asctime)s] %(message)s')
    elif level.lower() == "debug":
        logger.setLevel(logging.DEBUG)
        f = logging.Formatter(
            '[%(levelname)s %(threadName)s %(asctime)s %(funcName)s(%(lineno)d)] %(message)s')
    if logfile is None:
        h = logging.StreamHandler(sys.stdout)
    else:
        h = logging.FileHandler(logfile, mode='a+')
    h.setFormatter(f)
    logger.addHandler(h)
    return logger


def mkdir(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def mkfile(filename, size=0, add=False):
    if os.path.isfile(filename):
        mode = "r+b"
    else:
        mode = "wb"
    with open(filename, mode) as f:
        cur = 0
        if add:
            f.seek(0, os.SEEK_END)
            cur = f.tell()
            if size <= cur:
                return
        if size > 0 and size > cur:
            f.seek(size-1)
            f.write(b'\x00')
        else:
            pass


def addLogHandler(logfile=None):
    if logfile is None:
        h = logging.StreamHandler(sys.stdout)
    else:
        h = logging.FileHandler(logfile, mode='a+')
    logger = logging.getLogger()
    oh = logger.handlers[0]
    h.setFormatter(oh.formatter)
    logger.addHandler(h)


def restart_with_reloader():
    new_environ = os.environ.copy()
    while True:
        args = [sys.executable] + sys.argv
        new_environ["RUN_MAIN"] = 'true'
        exit_code = subprocess.call(args, env=new_environ)
        if exit_code != 3:
            return exit_code
        new_environ["RUN_HGET_FIRST"] = "false"
        time.sleep(0.1)


def autoreloader(main_func, *args, **kwargs):
    if os.environ.get("RUN_MAIN") == "true":
        main_func(*args, **kwargs)
    else:
        try:
            exit_code = restart_with_reloader()
            if exit_code < 0:
                os.kill(os.getpid(), -exit_code)
            else:
                sys.exit(exit_code)
        except KeyboardInterrupt:
            pass


def check_md5(filename, size):
    hm = hashlib.md5()
    size = int(size)
    with open(filename, "rb") as fi:
        cur = 0
        while True:
            b = fi.read(10240)
            if not b:
                break
            cur += len(b)
            if cur > size:
                hm.update(b[:-(cur-size)])
                break
            else:
                hm.update(b)
    return filename, hm.hexdigest()


class Daemon(HsyncLog):
    def __init__(self, args, pidfile=os.path.join(HSYNC_DIR, "hsyncd.pid")):
        self.args = args
        self.pidfile = pidfile
        mkdir(os.path.dirname(self.pidfile))

    def daemonize(self):
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError:
            sys.exit(1)
        si = open(os.devnull, 'r')
        so = open(os.devnull, 'a+')
        se = open(os.devnull, 'a+')

        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        atexit.register(self._delpid)
        with open(self.pidfile, 'w') as f:
            f.write('%d\n' % os.getpid())

    def start(self):
        self._check_pid()
        self.daemonize()
        self.run()

    def restart(self):
        self.stop()
        self.start()

    def run(self):
        pass

    def stop(self):
        if os.path.isfile(self.pidfile):
            with open(self.pidfile) as f:
                pid = int(f.read().strip())
            self._delpid()
            os.kill(pid, signal.SIGTERM)
        else:
            raise Exception("Daemon hsyncd is not started")

    def _delpid(self):
        if os.path.isfile(self.pidfile):
            os.remove(self.pidfile)

    def _check_pid(self):
        try:
            with open(self.pidfile, 'r') as pf:
                pid = int(pf.read().strip())
        except:
            pid = None
        if pid:
            message = "pidfile {0} already exist. " + \
                "Daemon hsyncd already running?\n"
            sys.stderr.write(message.format(self.pidfile))
            sys.exit(1)
