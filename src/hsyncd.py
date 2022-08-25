import os
import sys
import asyncio

from .utils import *
from .config import *


class ReloadConf(object):
    conf = Config.LoadConfig()


class Download(web.View, HsyncLog, ReloadConf):

    @HsyncDecorator.check_ipaddres
    @HsyncDecorator.check_filepath
    async def get(self):
        return web.FileResponse(path=self.hsync_file_path,
                                headers={
                                    hdrs.CONTENT_DISPOSITION: 'attachment;filename={}'.format(os.path.basename(self.hsync_file_path)),
                                    hdrs.CONTENT_TYPE: "application/octet-stream"
                                },
                                chunk_size=256 * 1024)


class Listpath(web.View, HsyncLog, ReloadConf):

    @HsyncDecorator.check_ipaddres
    async def get(self):
        query = query_parse(self.request)
        qpath = query.get('path')
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
    if args.log:
        addLogHandler()
    if 'stop' in sys.argv:
        daemon.stop()
    elif 'start' in sys.argv:
        daemon.start()
    elif 'restart' in sys.argv:
        daemon.restart()
    else:
        if args.daemon:
            daemon.start()
        else:
            daemon.run()


if __name__ == "__main__":
    main()
