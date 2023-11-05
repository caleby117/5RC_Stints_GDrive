import configparser
from pathlib import Path

class Config:
    __instance = None

    @classmethod 
    def instance(self):
        if Config.__instance:
            return Config.__instance

        return Config()

    def __init__(self):
        if Config.__instance:
            raise ValueError("Config is a singleton. Get an instance with Config.instance() method.")
        
        Config.__instance = self

    @classmethod
    def parse_config(self, file):
        parser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        configpath = Path(file)
        parser.read(configpath.as_posix())
        for k in parser.sections():
            setattr(Config.__instance, k, Section(parser.items(k)))




class Section:
    def __init__(self, attributes):
        self.keys = []
        for k,v in attributes:
            self.keys.append(k)
            setattr(self, k, v)

