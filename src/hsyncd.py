import os
import pdb
import sys
import asyncio

from .utils import *
from .config import *


class ReloadConf(object):
    conf = Config.LoadConfig()


class TestConnect(web.View, HsyncLog, ReloadConf):

    @HsyncDecorator.check_ipaddres
    async def post(self):
        return web.HTTPForbidden()


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
                if len(res) > 1000:
                    return web.HTTPForbidden(reason="To many files or dirs under the query path %s" % qpath)
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
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        if conf.hsyncd.Hsync_verify == "yes":
            ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_cert_chain(certfile=os.path.join(
            HSYNC_DIR, "cert", 'hsyncd.crt'), keyfile=os.path.join(HSYNC_DIR, "cert", 'hsyncd.key'))
        ssl_context.load_verify_locations(
            cafile=os.path.join(HSYNC_DIR, "cert", 'ca.pem'))
        h = mk_hsync_args(self.args, conf.hsyncd, "Host_ip", "0.0.0.0")
        p = mk_hsync_args(self.args, conf.hsyncd, "Port", 10808)
        self.loger.info("hsyncd server start: %s:%s", h, p)
        web.run_app(app=init_app(), host=h,
                    port=int(p), ssl_context=ssl_context)


async def init_app():
    app = web.Application()
    app.router.add_view('/get', Download)
    app.router.add_view('/lsdir', Listpath)
    app.router.add_view('/check', CheckMd5)
    return app


@KeyBoardExit
def create_hsync_keys():
    sys.stdout.write("Generating hsyncd/hsync CA key files.\n")
    sys.stdout.flush()
    while True:
        length = ask("Enter key length (2048):",
                     timeout=60).strip() or 2048
        try:
            length = int(length)
            break
        except:
            sys.stdout.write("only int allowed\n")
            sys.stdout.flush()
    while True:
        days = ask("Enter key effectivate days (3650):",
                   timeout=60).strip() or 3650
        try:
            days = int(days)
            break
        except:
            sys.stdout.write("only int allowed\n")
            sys.stdout.flush()
    outdir = ask("Enter directory in which to save the keys (%s):" %
                 os.path.join(HSYNC_DIR, "cert"), timeout=60) or os.path.join(HSYNC_DIR, "cert")
    k = HsyncKey(keydir=outdir)
    mkdir(outdir)
    End = True
    if not os.path.isfile(k.capem) or not os.path.isfile(k.serverkeyfile) or not os.path.isfile(k.servercrtfile):
        k.create_keys(length=int(length), days=days)
    else:
        while End:
            cover = ask(
                "Hsyncd/Hsync CA key files, overwrite? y/[n] :", timeout=60, default="n")
            if cover == "n":
                sys.stdout.write("Overwrite : No, exit.\n")
                break
            elif cover == "y":
                End = False
            else:
                continue
        else:
            k.create_keys(length=int(length), days=days)


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
