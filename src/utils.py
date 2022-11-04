import os
import re
import ssl
import sys
import time
import signal
import base64
import logging
import socket
import struct
import atexit
import psutil
import hashlib
import asyncio
import argparse
import functools
import subprocess

from copy import deepcopy
from fnmatch import fnmatch
from threading import Thread
from logging.handlers import RotatingFileHandler
from multiprocessing import cpu_count, current_process, get_logger, Pool
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, as_completed

from tqdm import tqdm
from aiohttp import ClientSession, TCPConnector, ClientTimeout, web, hdrs, BasicAuth, request
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
                if is_cert(file_path):
                    return web.HTTPForbidden()
                filename = os.path.basename(file_path)
                ap = split_values(
                    self.conf.info.hsyncd.get("Only_allowed_path"))
                if ap:
                    for pt in ap:
                        pt = os.path.abspath(pt)
                        if pt in file_path:
                            break
                    else:
                        return web.HTTPForbidden()
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
            elif os.path.isdir(file_path):
                ap = split_values(
                    self.conf.info.hsyncd.get("Only_allowed_path"))
                if ap:
                    for pt in ap:
                        pt = os.path.abspath(pt)
                        if pt in file_path:
                            break
                    else:
                        return web.HTTPForbidden()
                for ex_dir in split_values(self.conf.info.hsyncd.Forbidden_dir):
                    ex_dir = ex_dir.strip().strip(',').strip()
                    if file_path.startswith(ex_dir):
                        self.loger.error(
                            "%s file forbidden in %s", file_path, self.conf.info.hsyncd.Forbidden_dir)
                        return web.HTTPForbidden()
            else:
                return web.HTTPForbidden(reason="no such file or directory %s in remote" % file_path)
            self.hsync_file_path = file_path
            res = await func(self, *args, **kwargs)
            return res
        return wrapper

    def exit_exec(func):
        async def wrapper(*args, **kwargs):
            try:
                res = await func(*args, **kwargs)
                return res
            except ServerForbidException as e:
                sys.exit(e.args[0])
            except HsyncKeyException as e:
                sys.exit(e.args[0])
            except Exception as e:
                raise e
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
    hdrs.CONNECTION: "close",
    hdrs.CONTENT_TYPE: "application/octet-stream",
    hdrs.USER_AGENT: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
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


class HsyncKeyException(Exception):
    pass


class ConfigSectionsError(Exception):
    '''Config Sections exists'''


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
    parser.add_argument("-l", "--log", type=str, default=os.path.join(HSYNC_DIR, "hsyncd.log"),
                        help='hsyncd logging file, %s by default' % os.path.join(HSYNC_DIR, "hsyncd.log"),  metavar="<str>")
    parser.add_argument("-c", "--config", type=str,
                        help="configuration files to search, will overwrite `HSYNC_DIR` default setting if conflict", metavar="<file>")
    parser.add_argument("-d", "--daemon", action='store_true',
                        help="daemon process", default=False)
    parser.add_argument('-v', '--version',
                        action='version', version="v" + __version__)
    return parser.parse_known_args()


def ShowConfig():
    parser = argparse.ArgumentParser(
        description="show hsync config")
    parser.add_argument("-c", "--config", type=str,
                        help="configuration files to search, will overwrite `HSYNC_DIR` default setting if conflict", metavar="<file>")
    return parser.parse_args()


def hsyncArg():
    parser = argparse.ArgumentParser(
        description="hsync for remote file synchronize")
    parser.add_argument("-i", "--input", type=str, required=True,
                        help='input remote path', metavar="<str>")
    parser.add_argument("-host", "--host-ip", dest="Host_ip", type=str,
                        help='connect host ip, localhost by default', metavar="<str>")
    parser.add_argument("-p", "--port", dest="Port", type=int,
                        help='connect port, 10808 by default',  metavar="<int>")
    parser.add_argument("-c", "--config", type=str,
                        help="configuration files to search, will overwrite `HSYNC_DIR` default setting if conflict", metavar="<file>")
    parser.add_argument("--ignore-exists", action="store_true", default=False,
                        help="ignore exists files or directorys whatever changes")
    parser.add_argument("--no-md5", action="store_true", default=False,
                        help="do not md5 check for hsync")
    parser.add_argument("-o", "--output", type=str,
                        help="output path", metavar="<str>")
    parser.add_argument('-v', '--version',
                        action='version', version="v" + __version__)
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
    parser.add_argument("-c", "--config", type=str,
                        help="configuration files to search, will overwrite `HSYNC_DIR` default setting if conflict", metavar="<file>")
    parser.add_argument("-o", "--output", type=str,
                        help="output path", metavar="<str>")
    parser.add_argument('-v', '--version',
                        action='version', version="v" + __version__)
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
        h = RotatingFileHandler(
            logfile, mode="a", maxBytes=100 >> 20, backupCount=100, encoding=None)
    h.setFormatter(f)
    logger.addHandler(h)
    return logger


def KeyBoardExit(func):
    def wrapper(*args, **kwargs):
        try:
            res = func(*args, **kwargs)
            return res
        except KeyboardInterrupt:
            sys.stdout.write("\n")
            sys.exit(signal.SIGINT)
        except Exception as e:
            raise e
    return wrapper


def mkdir(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def mkfile(filename, size=0, add=False):
    mode = os.path.isfile(filename) and "r+b" or "wb"
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


def AddStdoutLog():
    h = logging.StreamHandler(sys.stdout)
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
    if size >= os.path.getsize(filename):
        with open(filename, "rb") as fi:
            while True:
                b = fi.read(10240)
                if not b:
                    break
                hm.update(b)
        return filename, hm.hexdigest()
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
            ps = psutil.Process(pid)
            for p in ps.children(recursive=True):
                os.kill(p.pid, signal.SIGTERM)
            os.kill(pid, signal.SIGTERM)
        else:
            raise Exception("Daemon hsyncd is not started")

    def _delpid(self):
        if os.path.isfile(self.pidfile):
            os.remove(self.pidfile)

    def _check_pid(self):
        if os.path.isfile(self.pidfile):
            with open(self.pidfile, 'r') as pf:
                pid = pf.read().strip()
            if os.path.exists(os.path.join("/proc/", pid)):
                message = "pidfile {0} already exist. " + \
                    "Daemon hsyncd already running?\n"
                sys.stderr.write(message.format(self.pidfile))
                sys.exit(1)
            else:
                self._delpid()


def interrupt(signum, frame):
    raise TimeoutException()


def _timeout(timeout_secs, func, *args, **kwargs):
    default_return = kwargs.pop('default_return', "")

    signal.signal(signal.SIGALRM, interrupt)
    signal.alarm(timeout_secs)

    try:
        ret = func(*args, **kwargs)
        signal.alarm(0)
        return ret
    except TimeoutException:
        return default_return


def ask(msg="", timeout=100, default=""):
    sys.stdout.write(msg)
    sys.stdout.flush()
    content = _timeout(timeout, sys.stdin.readline)
    if not content:
        sys.stdout.write("\n")
        sys.stdout.flush()
    if not content.strip():
        content = default
    return content.strip()


def cert_path(config=None, section="hsyncd"):
    def cert_dir(f): return os.path.join(HSYNC_DIR, "cert", f)
    section = section.lower()
    cafile = cert_dir("ca.pem")
    if section == "hsyncd":
        certfile = cert_dir("hsyncd.crt")
        keyfile = cert_dir("hsyncd.key")
    elif section in ["hsync", "hscp"]:
        certfile = cert_dir("hsync.crt")
        keyfile = cert_dir("hsync.key")
    else:
        raise ConfigSectionsError("No such section in config" % section)
    if config:
        certfile = config[section].Cert_file or certfile
        keyfile = config[section].Key_file or keyfile
        cafile = config[section].CA_file or cafile
    return cafile, certfile, keyfile


def ssl_context(config=None, section="hsync"):
    sslcontext = False
    if not config:
        return sslcontext
    cafile, certfile, keyfile = cert_path(config=config, section=section)
    if all([os.path.isfile(i) for i in [cafile, certfile, keyfile]]):
        sslcontext = ssl.create_default_context(
            purpose=ssl.Purpose.SERVER_AUTH,)
        sslcontext.check_hostname = False
        sslcontext.load_cert_chain(certfile=certfile, keyfile=keyfile)
        sslcontext.load_verify_locations(cafile=cafile)
    return sslcontext


def is_cert(f):
    cert_files = [os.path.join("cert", i) for i in [
        "ca.key", "ca.pem", "hsyncd.crt", "hsyncd.key", "hsync.crt", "hsync.key"]]
    if os.path.isfile(f):
        f = os.path.abspath(f)
        for c in cert_files:
            if f.endswith(c):
                return True
    return False


@web.middleware
class HsyncAuthMiddleware(object):

    def __init__(self, force=True):
        self.force = force

    async def authenticate(self, request):
        js = await request.json()
        if "key" not in js:
            return False
        enc = js["key"]
        k = HsyncKey()
        try:
            k.load_key(prikey_file=os.path.join(HSYNC_DIR, "hsync.private"))
            key = k.decode(enc)
            if len(key) != 50:
                return False
            return True
        except:
            return False

    def check_fail(self):
        return web.Response(
            body=b'',
            status=401,
            reason='UNAUTHORIZED',
        )

    def required(self, handler):
        @functools.wraps(handler)
        async def wrapper(*args):
            request = None
            for arg in args:
                if isinstance(arg, web.View):
                    request = arg.request
                if isinstance(arg, web.Request):
                    request = arg

            if request is None:
                raise ValueError('Request argument not found for handler')

            if await self.authenticate(request):
                return await handler(*args)
            else:
                return self.check_fail()

        return wrapper

    async def __call__(self, request, handler):
        if not self.force:
            return await handler(request)
        else:
            if await self.authenticate(request):
                return await handler(request)
            else:
                return self.check_fail()


class HsyncKey(object):
    def __init__(self, keydir=""):
        self.cmd = ""
        self.ca_dir = os.path.abspath(keydir)
        mkdir(self.ca_dir)
        self.capem = os.path.join(self.ca_dir, "ca.pem")
        self.cakey = os.path.join(self.ca_dir, "ca.key")
        self.serverkeyfile = os.path.join(self.ca_dir, "hsyncd.key")
        self.servercrtfile = os.path.join(self.ca_dir, "hsyncd.crt")
        self.clientkeyfile = os.path.join(self.ca_dir, "hsync.key")
        self.clientcrtfile = os.path.join(self.ca_dir, "hsync.crt")

    def call(self, cmd, run=True):
        if not run:
            return
        with open(os.devnull, "w") as fo:
            subprocess.check_call(cmd, shell=True, stdout=fo, stderr=fo)

    def create_ca_key(self, length=2048, days=3650):
        cmd = "openssl genrsa -out %s %s" % (
            os.path.join(self.ca_dir, "ca.key"), length)
        cmd += '\nopenssl req -x509 -new -nodes -key %s -days %s -out %s -subj "/C=CN/ST=SC/L=CD/O=XY/OU=FF/CN=hugo" -sha256' % (
            self.cakey, days, self.capem)
        self.cmd += cmd
        self.call(cmd)
        sys.stdout.write("Create CA files %s and %s success\n" %
                         (self.capem, self.cakey))

    def create_hsyncd_key(self, length=2048, days=3650):
        if not os.path.isfile(self.cakey) or not os.path.isfile(self.capem):
            sys.exit("No CA %s and %s exists" % (self.pem, self.cakey))
        cmd = "openssl genrsa -out %s %s" % (
            self.serverkeyfile, length)
        cmd += '\nopenssl req -new -key %s -out %s -subj "/C=CN/ST=SC/L=CD/O=XY/OU=FF/CN=0.0.0.0" -sha256' % (
            self.serverkeyfile, os.path.join(self.ca_dir, "hsyncd.csr"))
        cmd += "\nopenssl x509 -req -in %s -CA %s -CAkey %s -CAcreateserial -out %s -days %s -sha256" % (
            os.path.join(self.ca_dir, "hsyncd.csr"), self.capem,
            self.cakey, self.servercrtfile, days)
        self.cmd += "\n" + cmd
        self.call(cmd)
        sys.stdout.write("Create hsyncd key files %s and %s success\n" %
                         (self.serverkeyfile, self.servercrtfile))

    def create_hsync_key(self, length=2048, days=3650):
        if not os.path.isfile(self.cakey) or not os.path.isfile(self.capem):
            sys.exit("No CA %s and %s exists" % (self.pem, self.cakey))
        cmd = "openssl genrsa -out %s %s" % (
            self.clientkeyfile, length)
        cmd += '\nopenssl req -new -key %s -out %s -subj "/C=CN/ST=SC/L=CD/O=XY/OU=FF/CN=0.0.0.0" -sha256' % (
            self.clientkeyfile, os.path.join(self.ca_dir, "hsync.csr"))
        cmd += "\nopenssl x509 -req -in %s -CA %s -CAkey %s -CAcreateserial -out %s -days %s -sha256" % (
            os.path.join(self.ca_dir, "hsync.csr"), self.capem,
            self.cakey, self.clientcrtfile, days)
        self.cmd += "\n" + cmd
        self.call(cmd)
        sys.stdout.write("Create hsync key files %s and %s success\n" %
                         (self.clientkeyfile, self.clientcrtfile))

    def rm_tmp(self):
        cmd = "rm -fr %s %s %s" % (
            os.path.join(self.ca_dir, "ca.srl"),
            os.path.join(self.ca_dir, "hsyncd.csr"),
            os.path.join(self.ca_dir, "hsync.csr")
        )
        self.cmd += "\n" + cmd
        self.call(cmd)

    def create_keys(self, length=2048, days=3650):
        self.create_ca_key(length=length, days=days)
        self.create_hsyncd_key(length=length, days=days)
        self.create_hsync_key(length=length, days=days)
        self.rm_tmp()


def isSwapFile(path):
    if not os.path.isfile(path):
        return False
    name = os.path.basename(path)
    if name.endswith(".swp") and name.startswith("."):
        return True
    return False
