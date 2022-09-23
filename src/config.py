import os
import configparser

from .utils import HSYNC_DIR


class Conf(configparser.ConfigParser):
    def __init__(self, defaults=None):
        super(Conf, self).__init__(defaults=defaults)

    def optionxform(self, optionstr):
        return optionstr


class Dict(dict):
    def __getattr__(self, name):
        return self.get(name)  # fix Key Error

    def __setattr__(self, name, value):
        self[name] = value


class Config(object):

    def __init__(self, config_file=None):
        self.cf = []
        self.info = Dict()
        if config_file is None:
            return
        self._path = os.path.join(os.getcwd(), config_file)
        self.cf.append(self._path)
        if not os.path.isfile(self._path):
            return
        self._config = Conf()
        self._config.read(self._path)
        for s in self._config.sections():
            self.info[s] = Dict(dict(self._config[s].items()))

    def get(self, section, name):
        return self.info.get(section, Dict()).get(name, None)

    def update_config(self, config):
        if config is None:  # update None
            return
        self.cf.append(config)
        if not os.path.isfile(config):
            return
        c = Conf()
        c.read(config)
        for s in c.sections():
            self.info.setdefault(s, Dict()).update(dict(c[s].items()))

    def update_dict(self, **kwargs):
        self.info.setdefault("args", Dict()).update(kwargs)

    def write_config(self, configile):
        with open(configile, "w") as fo:
            for s, info in self.info.items():
                fo.write("[%s]\n" % s)
                for k, v in info.items():
                    fo.write("%s = %s\n" % (k, v))
                fo.write("\n")

    def __str__(self):
        return self.info

    def __call__(self):
        return self.info

    @classmethod
    def LoadConfig(cls):
        configfile_home = os.path.join(HSYNC_DIR, "hsync.ini")
        configfile_default = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), 'hsync.ini')
        conf = cls(configfile_default)
        conf.update_config(configfile_home)
        return conf

    def PrintConfig(self):
        print("Configuration files to search (order by order):")
        for cf in self.cf[::-1]:
            print(" - %s" % os.path.abspath(cf))
        print("\nAvailable Config:")
        for k, info in self.info.items():
            print("[%s]" % k)
            for v, p in sorted(info.items()):
                print(" - %s : %s" % (v, p))
