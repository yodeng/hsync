#!/usr/bin/env python

import os
import sys
import _thread

import requests

from .utils import *
from .config import *


class DownloadByRange(object):

    def __init__(self, url="", outfile="", quite=False, headers={}, **kwargs):
        self.conf = Config.LoadConfig().info
        self.url = url
        if outfile:
            self.outfile = os.path.abspath(outfile)
        else:
            self.outfile = os.path.join(
                os.getcwd(), os.path.basename(self.url))
        self.outdir = os.path.dirname(self.outfile)
        mkdir(self.outdir)
        self.rang_file = self.outfile + ".ht"
        self.threads = int(self.conf.hscp.Max_runing)
        self.parts = int(self.conf.hscp.Max_part_num)
        self.tcp_conn = int(self.conf.hscp.Max_tcp_conn)
        self.datatimeout = int(self.conf.hscp.Data_timeout)
        self.headers = headers
        self.headers.update(default_headers)
        self.offset = {}
        self.range_list = []
        self.tqdm_init = 0
        self.quite = quite
        self.extra = kwargs
        self.ftp = False
        self.startime = int(time.time())
        self.current = current_process()

    async def get_range(self, session, size=1024 * 1000):
        if os.path.isfile(self.rang_file) and os.path.isfile(self.outfile):
            self.load_offset()
            content_length = 0
            for end, s in self.offset.items():
                start0, read = s
                self.tqdm_init += read
                start = sum(s)
                rang = {'Range': (start, end)}
                self.range_list.append(rang)
                content_length = max(content_length, end + 1)
            mn_as, mn_pt = get_as_part(content_length)
            self.threads = self.threads or min(
                max_async(), mn_as)
            self.parts = min(self.parts, mn_pt)
            self.content_length = content_length
            self.loger.debug("%s of %s has download" %
                             (self.tqdm_init, self.content_length))
        else:
            req = await self.fetch(session)
            content_length = int(req.headers['content-length'])
            mn_as, mn_pt = get_as_part(content_length)
            self.threads = self.threads or min(
                max_async(), mn_as)
            self.parts = min(self.parts, mn_pt)
            self.content_length = content_length
            if self.parts:
                size = content_length // self.parts
                count = self.parts
            else:
                count = content_length // size
            for i in range(count):
                start = i * size
                if i == count - 1:
                    end = content_length - 1
                else:
                    end = start + size
                if i > 0:
                    start += 1
                rang = {'Range': (start, end)}
                self.offset[end] = [start, 0]
                self.range_list.append(rang)
            if self.content_length < 1:
                self.offset = {}
                self.loger.warn(
                    "Remote file %s has no content with filesize 0 bytes" % self.extra['path'])
            if not os.path.isfile(self.outfile):
                mkfile(self.outfile, self.content_length)
            self.write_offset()

    async def download(self):
        self.timeout = ClientTimeout(total=60*60*24, sock_read=2400)
        self.connector = TCPConnector(
            limit=self.tcp_conn, ssl=False)
        async with ClientSession(connector=self.connector, timeout=self.timeout) as session:
            await self.get_range(session)
            if self.content_length < 1:
                return
            self.set_sem(self.threads)
            if os.getenv("RUN_HGET_FIRST") != 'false':
                self.loger.info("File size: %s (%d bytes)",
                                human_size(self.content_length), self.content_length)
                self.loger.info("Starting download remote file: %s --> %s",
                                self.extra["path"], self.outfile)
            self.loger.debug("Ranges: %s, Sem: %s, Connections: %s, %s", self.parts,
                             self.threads, self.tcp_conn or 100, get_as_part(self.content_length))
            if len(self.current._identity):
                pos = self.current._identity[0]-1
            else:
                pos = None
            with tqdm(position=pos, disable=self.quite, total=int(self.content_length), initial=self.tqdm_init, unit='', ascii=True, unit_scale=True) as bar:
                tasks = []
                for h_range in self.range_list:
                    s, e = h_range["Range"]
                    if s > e:
                        continue
                    h_range.update(self.headers)
                    task = self.fetch(
                        session, pbar=bar, headers=h_range.copy())
                    tasks.append(task)
                await asyncio.gather(*tasks)

    async def fetch(self, session, pbar=None, headers=None):
        if headers:
            async with self.sem:
                s, e = headers["Range"]
                headers["Range"] = 'bytes={0}-{1}'.format(s, e)
                self.loger.debug(
                    "Start %s %s", asyncio.current_task().get_name(), headers["Range"])
                async with session.post(self.url, headers=headers, timeout=self.timeout, json=self.extra) as req:
                    with open(self.outfile, 'r+b') as f:
                        f.seek(s, os.SEEK_SET)
                        async for chunk in req.content.iter_chunked(102400):
                            if chunk:
                                f.write(chunk)
                                f.flush()
                                self.offset[e][-1] += len(chunk)
                                pbar.update(len(chunk))
                self.loger.debug(
                    "Finished %s %s", asyncio.current_task().get_name(), headers["Range"])
        else:
            async with session.post(self.url, timeout=self.datatimeout, json=self.extra) as req:
                if req.status == 403:
                    raise ServerForbidException(
                        "Server forbidden for download %s" % self.extra["path"])
                return req

    def set_sem(self, n):
        self.sem = asyncio.Semaphore(n)

    def _run(self):
        self.startime = int(time.time())
        Done = False
        try:
            if self.url.startswith("http"):
                self.loop = asyncio.get_event_loop()
                self.loop.run_until_complete(self.download())
            else:
                self.loger.error("Only http/https urls allowed.")
                sys.exit(1)
            Done = True
        except ReloadException as e:
            self.loger.debug(e)
            raise e
        except asyncio.TimeoutError as e:
            raise TimeoutException("Connect url timeout")
        except Exception as e:
            self.loger.error(e)
            raise e
        finally:
            self.write_offset()
        return Done

    def write_offset(self):
        if len(self.offset):
            with open(self.rang_file, "wb") as fo:
                fo.write(struct.pack('<Q', self.startime))
                fo.write(struct.pack('<Q', len(self.offset)))
                fo.write(os.urandom(3))
                for e, s in self.offset.items():
                    l = [e, ] + s
                    for i in l:
                        fo.write(struct.pack('<Q', int(i)))
                        fo.write(os.urandom(3))

    def load_offset(self):
        with open(self.rang_file, "rb") as fi:
            self.startime = struct.unpack('<Q', fi.read(8))[0]
            fileno = struct.unpack('<Q', fi.read(8))[0]
            fi.read(3)
            for i in range(fileno):
                offset = []
                for j in range(3):
                    p = struct.unpack('<Q', fi.read(8))[0]
                    offset.append(p)
                    fi.read(3)
                self.offset[offset[0]] = offset[1:]

    @property
    def loger(self):
        if self.current.name == "MainProcess":
            log = logging.getLogger()
        else:
            log = get_logger()
        if self.quite:
            log.setLevel(logging.ERROR)
        return log

    def run(self):
        es = exitSync(obj=self)
        es.start()
        res = self._run()
        if res:
            if os.path.isfile(self.rang_file):
                os.remove(self.rang_file)
                self.loger.debug("Remove %s", os.path.basename(self.rang_file))
            els = int(time.time()) - self.startime
            self.loger.info("Donwload success, time elapse: %s sec", els)
            sys.exit()
        return res


def down_file_by_range(url, outfile, path):
    d = DownloadByRange(
        url=url,
        outfile=outfile,
        path=path)
    d.run()


class Hsync(HsyncLog):

    def __init__(self, url="", outfile="", ss=0, ts=None, headers={}, md5={}, **kwargs):
        self.conf = Config.LoadConfig().info
        self.url = url
        if outfile:
            self.outfile = os.path.abspath(outfile)
        else:
            self.outfile = os.path.join(
                os.getcwd(), os.path.basename(self.url))
        self.tcp_conn = int(self.conf.hsync.Max_tcp_conn)
        self.headers = headers
        self.headers.update(default_headers)
        self.extra = kwargs
        self.from_range = ss
        self.quite = kwargs.get("quite", False)
        self.end_range = ts
        self.md5 = md5
        if self.extra["path"] not in self.md5:
            self.md5[self.extra["path"]] = hashlib.md5()

    async def run(self):
        self.timeout = ClientTimeout(total=60*60*24, sock_read=2400)
        self.connector = TCPConnector(
            limit=self.tcp_conn, ssl=False)
        async with ClientSession(connector=self.connector, timeout=self.timeout) as session:
            self.headers["Range"] = "bytes={}-{}".format(
                self.from_range, self.end_range)
            with tqdm(disable=self.quite, total=self.end_range, initial=self.from_range, unit='', ascii=True, unit_scale=True) as bar:
                async with asyncio.Semaphore(int(self.conf.hsync.Max_tcp_conn)):
                    await self._hync(session, pbar=bar, headers=self.headers)

    async def _hync(self, session, pbar=None,  headers={}):
        self.loger.info("Starting async remote file: %s --> %s, size from %s to %s",
                        self.extra["path"], self.outfile, self.from_range, self.end_range)
        async with session.post(self.url, headers=headers, timeout=self.timeout, json=self.extra) as req:
            if req.status == 403:
                self.loger.error(
                    "Server forbidden for download %s", self.extra["path"])
                return
            with open(self.outfile, self.from_range and 'ab' or "wb") as f:
                async for chunk in req.content.iter_chunked(10240):
                    if chunk:
                        f.write(chunk)
                        f.flush()
                        self.md5[self.extra["path"]].update(chunk)
                        pbar.update(len(chunk))
        self.loger.info("Finished async remote file: %s --> %s",
                        self.extra["path"], self.outfile)


def echo_config():
    Config().LoadConfig().PrintConfig()


def hsync_main():
    args = hsyncArg()
    conf = Config.LoadConfig().info
    asyncio.run(hsync(args, conf))


def hscp_main():
    args = hscpArg()
    conf = Config.LoadConfig().info
    hscp(args, conf)


async def hsync(args, conf):
    remote_path = os.path.abspath(args.input)
    if args.output:
        local_path = os.path.abspath(args.output)
    else:
        local_path = os.path.join(os.getcwd(), os.path.basename(remote_path))
    conf.hsync.Host_ip = conf.hsync.Host_ip or conf.hscp.Host_ip or conf.hsyncd.Host_ip
    conf.hsync.Port = conf.hsync.Port or conf.hscp.Port or conf.hsyncd.Port
    host = mk_hsync_args(args, conf.hsync, "Host_ip", "127.0.0.1")
    port = mk_hsync_args(args, conf.hsync, "Port", 10808)
    log = loger(multi=False)
    n = 0
    md5_rec = {}
    file_map = {}
    mtime = {}
    while n < int(conf.hsync.Max_timeout_retry):
        trans_files = {}
        listdir = requests.post(
            "http://{}:{}/lsdir".format(host, port), json={"path": remote_path}, timeout=int(conf.hsync.Data_timeout))
        if listdir.status_code == 403:
            raise ServerForbidException(
                "Server forbidden for ip connected")
        else:
            mtime_tmp = {i: j[1] for i, j in listdir.json().items()}
            listdir = {i: j[0] for i, j in listdir.json().items()}
        if not listdir:
            sys.exit("No such file or directory %s in remote host." %
                     remote_path)
        else:
            if len(listdir) == 1 and list(listdir.values())[0] < 0:
                mkdir(local_path)
                log.warn("empty remote directory %s --> %s",
                         remote_path, local_path)
            else:
                tasks = []
                try:
                    for d, s in listdir.items():
                        if d == remote_path:
                            outpath = local_path
                        else:
                            outpath = os.path.join(
                                local_path, d[len(remote_path)+1:])
                        if s < 0:
                            mkdir(outpath)
                            log.warn("empty remote directory %s --> %s",
                                     d, outpath)
                        else:
                            mkdir(os.path.dirname(outpath))
                            current_size = os.path.isfile(
                                outpath) and os.path.getsize(outpath) or 0
                            if current_size == s:
                                if s == 0:
                                    mkfile(outpath, s)
                                else:
                                    file_map[d] = outpath
                                    if d not in mtime:
                                        md5 = check_md5(outpath, s)
                                        md5_rec[d] = md5[1]
                                        trans_files[d] = s
                                    elif mtime[d] != mtime_tmp[d]:
                                        trans_files[d] = s
                            elif current_size < s:
                                sync = Hsync(url="http://{}:{}/get".format(host, port), outfile=outpath,
                                             ss=current_size, ts=s, md5=md5_rec, path=d)
                                tasks.append(sync.run())
                                file_map[d] = outpath
                                trans_files[d] = s+1
                            else:
                                os.remove(file_map[d])
                                md5_rec[d] = hashlib.md5()
                                sync = Hsync(url="http://{}:{}/get".format(host, port), outfile=outpath,
                                             ss=0, ts=s, md5=md5_rec, path=d)
                                tasks.append(sync.run())
                                file_map[d] = outpath
                                trans_files[d] = s+1
                    await asyncio.gather(*tasks)
                    if len(trans_files):
                        md5query = trans_files.copy()
                        checkout = requests.post(
                            "http://{}:{}/check".format(host, port), json=md5query).json()
                        for f, md5 in checkout.items():
                            if md5 != (isinstance(md5_rec[f], str) and md5_rec[f] or md5_rec[f].hexdigest()) or f not in md5query:
                                log.warn(
                                    "MD5 check error for %s file, re-hsync", file_map[f])
                                os.remove(file_map[f])
                                md5_rec[f] = hashlib.md5()
                except asyncio.TimeoutError as e:
                    n += 1
                except ReloadException:
                    pass
                except Exception as e:
                    raise e
        time.sleep(int(conf.hsync.Sync_interval_sec))
        mtime.update(mtime_tmp)


def hscp(args, conf):
    remote_path = os.path.abspath(args.input)
    if args.output:
        local_path = os.path.abspath(args.output)
    else:
        local_path = os.path.join(os.getcwd(), os.path.basename(remote_path))
    conf.hscp.Host_ip = conf.hscp.Host_ip or conf.hsyncd.Host_ip
    conf.hscp.Port = conf.hscp.Port or conf.hsyncd.Port
    host = mk_hsync_args(args, conf.hscp, "Host_ip", "0.0.0.0")
    port = mk_hsync_args(args, conf.hscp, "Port", 10808)
    listdir = requests.post(
        "http://{}:{}/lsdir".format(host, port), json={"path": remote_path}, timeout=int(conf.hsync.Data_timeout))
    if listdir.status_code == 403:
        raise ServerForbidException(
            "Server forbidden for ip connected")
    else:
        listdir = {i: j[0] for i, j in listdir.json().items()}
    if not listdir:
        sys.exit("No such file or directory %s in remote host." % remote_path)
    elif len(listdir) == 1:
        log = loger(multi=False)
        d = list(listdir.keys())[0]
        s = listdir[d]
        if s < 0:
            mkdir(local_path)
            log.warn("empty remote directory %s --> %s",
                     remote_path, local_path)
        else:
            mkdir(os.path.dirname(local_path))
            df = down_file_by_range(
                "http://{}:{}/get".format(host, port), outfile=local_path, path=d)
    else:
        log = loger(multi=True)
        mkdir(local_path)
        p = ProcessPoolExecutor(min(args.num, len(listdir)))
        features = []
        for d, s in listdir.items():
            outpath = os.path.join(local_path, d[len(remote_path)+1:])
            mkdir(os.path.dirname(outpath))
            if s >= 0:
                f = p.submit(down_file_by_range,
                             "http://{}:{}/get".format(host, port), outpath, d)
                features.append(f)
            elif s < 0:
                mkdir(outpath)
                log.warn("empty remote directory %s --> %s",
                         d, outpath)
        wait(features, return_when="ALL_COMPLETED")
