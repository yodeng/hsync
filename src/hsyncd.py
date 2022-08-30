import os
import pdb
import sys
import asyncio

from .utils import *
from .config import *


class ReloadConf(object):
    conf = Config.LoadConfig()


class Download(web.View, HsyncLog, ReloadConf):

    @HsyncDecorator.check_ipaddres
    @HsyncDecorator.check_filepath
    async def post(self):
        return web.FileResponse(path=self.hsync_file_path,
                                headers={
                                    hdrs.CONTENT_DISPOSITION: 'attachment;filename={}'.format(os.path.basename(self.hsync_file_path)),
                                    hdrs.CONTENT_TYPE: "application/octet-stream"
                                },
                                chunk_size=256 * 1024)


class CheckMd5(web.View, HsyncLog, ReloadConf):

    @HsyncDecorator.check_ipaddres
    async def post(self):
        data = await self.request.json()
        query = dict(data)
        if "key" in query:
            query.pop("key")
        executor = ProcessPoolExecutor(max_workers=min(
            10, int(self.conf.info.hsyncd.MD5_check_nproc)))
        tasks = [executor.submit(check_md5, f, size)
                 for f, size in query.items()]
        checkout = {}
        for task in as_completed(tasks):
            filename, md5 = task.result()
            checkout[filename] = md5
        executor.shutdown()
        return web.json_response(checkout)


class Listpath(web.View, HsyncLog, ReloadConf):

    @HsyncDecorator.check_ipaddres
    async def post(self):
        data = await self.request.json()
        query = dict(data)
        qpath = query.get('path')
        res = {}
        if os.path.isdir(qpath):
            qpath = os.path.abspath(qpath)
            for a, b, c in os.walk(qpath, followlinks=True):
                for d in b:
                    d = os.path.join(a, d)
                    if not os.listdir(d):
                        res[d] = (-1, os.path.getmtime(d))
                for i in c:
                    f = os.path.join(a, i)
                    res[f] = (os.path.getsize(f), os.path.getmtime(f))
            if not len(res):
                res[qpath] = (-1, os.path.getmtime(qpath))
        elif os.path.isfile(qpath):
            qpath = os.path.abspath(qpath)
            res[qpath] = (os.path.getsize(qpath), os.path.getmtime(qpath))
        return web.json_response(res)


class HsyncDaemon(Daemon):

    def run(self):
        conf = Config.LoadConfig().info
        h = mk_hsync_args(self.args, conf.hsyncd, "Host_ip", "0.0.0.0")
        p = mk_hsync_args(self.args, conf.hsyncd, "Port", 10808)
        self.loger.info("hsyncd server start: %s:%s", h, p)
        web.run_app(app=init_app(conf), host=h, port=int(p))


async def init_app(conf):
    if conf.hsyncd.Hsync_authorization == "yes":
        auth = HsyncAuthMiddleware()
        app = web.Application(middlewares=[auth])
    else:
        app = web.Application()
    app.router.add_view('/get', Download)
    app.router.add_view('/lsdir', Listpath)
    app.router.add_view('/check', CheckMd5)
    return app


@KeyBoardExit
def create_hsync_keys():
    k = HsyncKey()
    sys.stdout.write("Generating public/private hsync key pair.\n")
    sys.stdout.flush()
    while True:
        length = ask("Enter hsync key length (2048):",
                     timeout=60).strip() or 2048
        try:
            length = int(length)
            break
        except:
            sys.stdout.write("only int allowed\n")
            sys.stdout.flush()
    outdir = ask("Enter directory in which to save the key (%s):" %
                 HSYNC_DIR, timeout=60) or HSYNC_DIR
    pubfile = os.path.join(outdir, "hsync.public")
    prifile = os.path.join(outdir, "hsync.private")
    if os.path.isfile(pubfile) or os.path.isfile(prifile):
        while True:
            cover = ask(
                "Key files %s or %s exists, overwrite? y/[n] :" % (pubfile, prifile), timeout=60, default="n")
            if cover == "n":
                sys.stdout.write("Overwrite : No, exit.\n")
                return
            elif cover == "y":
                break
            else:
                continue
    k.create_keys(int(length))
    mkdir(outdir)
    k.save_key(pubfile, prifile)
    sys.stdout.write("Create key file %s and %s success\n" %
                     (pubfile, prifile))


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
