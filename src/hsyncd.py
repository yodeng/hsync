import os
import sys
import asyncio

from aiohttp import web, hdrs

from .utils import *
from .config import *


class Download(web.View, HsyncLog):

    conf = Config.LoadConfig()

    async def get(self):
        if not self.conf.info.hsyncd.Allowed_host or "*" in self.conf.info.hsyncd.Allowed_host.split():
            pass
        else:
            if self.request.remote not in split_values(self.conf.info.hsyncd.Allowed_host):
                return web.HTTPForbidden()
        query = query_parse(self.request)
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
            response = web.FileResponse(path=file_path,
                                        headers={
                                            hdrs.CONTENT_DISPOSITION: 'attachment;filename={}'.format(filename),
                                            hdrs.CONTENT_TYPE: "application/octet-stream"
                                        },
                                        chunk_size=256 * 1024)
            return response
        else:
            return web.HTTPForbidden()


class Listpath(web.View, HsyncLog):

    async def get(self):
        query = query_parse(self.request)
        qpath = query.get('dir')
        res = {}
        if os.path.isdir(qpath):
            qpath = os.path.abspath(qpath)
            res[qpath] = -1
            for a, b, c in os.walk(qpath, followlinks=True):
                for d in b:
                    d = os.path.join(a, d)
                    if not os.listdir(d):
                        res[d] = -1
                for i in c:
                    f = os.path.join(a, i)
                    s = os.path.getsize(f)
                    res[f] = s
        elif os.path.isfile(qpath):
            qpath = os.path.abspath(qpath)
            res[qpath] = os.path.getsize(qpath)
        return web.json_response(res)


class HsyncDaemon(Daemon):

    def run(self):
        conf = Config.LoadConfig().info
        h = mk_hsync_args(self.args, conf.hsyncd, "Host_ip", "0.0.0.0")
        p = mk_hsync_args(self.args, conf.hsyncd, "Port", 10808)
        self.loger.info("hsyncd server start: %s:%s", h, p)
        web.run_app(app=init_app(), host=h, port=int(p))


async def init_app():
    app = web.Application()
    app.router.add_view('/get', Download)
    app.router.add_view('/lsdir', Listpath)
    return app


def main():
    args, _ = hsyncdArg()
    daemon = HsyncDaemon(args, )
    log = loger(args.log)
    addLogHandler()
    if 'stop' in sys.argv:
        daemon.stop()
        sys.exit()
    elif 'start' in sys.argv:
        daemon.start()
        sys.exit()
    elif 'restart' in sys.argv:
        daemon.restart()
        sys.exit()
    if args.daemon:
        daemon.start()
    else:
        daemon.run()


if __name__ == "__main__":
    main()
