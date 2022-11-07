import os
import sysconfig

from setuptools import setup
from setuptools.extension import Extension


class Packages(object):

    def __init__(self, pkg_name=""):
        self.name = pkg_name
        self.base_dir = os.path.dirname(__file__)
        self.source_dir = os.path.join(self.base_dir, "src")
        self.version_file = os.path.join(self.base_dir, "src/_version.py")
        self.des_file = os.path.join(self.base_dir, "README.md")
        self.req_file = os.path.join(self.base_dir, "requirements.txt")

    @property
    def listdir(self):
        df = []
        for a, b, c in os.walk(self.source_dir):
            if os.path.basename(a).startswith("__"):
                continue
            for i in c:
                if i.startswith("__") or i.endswith(".ini"):
                    continue
                p = os.path.join(a, i)
                df.append(p)
        return df

    @property
    def description(self):
        des = ""
        with open(self.des_file) as fi:
            des = fi.read()
        return des

    @property
    def version(self):
        v = {}
        with open(self.version_file) as fi:
            c = fi.read()
        exec(compile(c, self.version_file, "exec"), v)
        return v["__version__"]

    @property
    def requirements(self):
        requires = []
        with open(self.req_file) as fi:
            for line in fi:
                line = line.strip()
                requires.append(line)
        return requires

    @property
    def _extensions(self):
        exts = []
        for f in self.listdir:
            e = Extension(self.name + "." + os.path.splitext(os.path.basename(f))[0],
                          [f, ], extra_compile_args=["-O3", ],)
            e.cython_directives = {
                'language_level': sysconfig._PY_VERSION_SHORT_NO_DOT[0]}
            exts.append(e)
        return exts

    def install(self, ext=False):
        kwargs = {}
        kwargs.update(
            name=self.name,
            version=self.version,
            packages=[self.name, ],
            package_data={self.name: ["*.ini", ]},
            license="MIT",
            url="https://github.com/yodeng/hsync",
            package_dir={self.name: os.path.basename(self.source_dir)},
            install_requires=self.requirements,
            python_requires='>=3.8',
            long_description=self.description,
            long_description_content_type='text/markdown',
            entry_points={'console_scripts': self._entrys},
        )
        if ext:
            kwargs.pop("package_dir")
            kwargs["ext_modules"] = self._extensions
        setup(**kwargs)

    @property
    def _entrys(self):
        eps = [
            'hsyncd = hsync.hsyncd:main',
            'hsync-keygen = hsync.hsyncd:create_hsync_keys',
            'hscp = %s.%s:hscp_main' % (self.name, self.name),
            'hsync = %s.%s:hsync_main' % (self.name, self.name),
            'hsync-echo-config = hsync.hsync:echo_config',
        ]
        return eps


def main():
    pkgs = Packages("hsync")
    pkgs.install()


if __name__ == "__main__":
    main()
