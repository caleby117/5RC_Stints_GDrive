from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

@dataclass
class FileMeta:
    name: str

@dataclass
class IbtFileMeta(FileMeta):
    # Defines an ibt file that is being downloaded from Drive
    g_id: str
    g_mimeType: str
    g_checksum: str
    filesize: int
    g_parentid: str

    fields: ClassVar[str] = "files(mimeType, id, name, size, sha256Checksum)"
    
    def __post_init__(self):
        self.path = Path(self.name).with_suffix(".ibt")
        self.ext = self.path.suffix
    
    def set_path(self, dir):
        self.path = dir/self.path

    def delete(self):
        self.path.unlink(missing_ok=True)

@dataclass
class CsvFileMeta(FileMeta):
    # metadata for generated csv files
    g_parentid: str
    def __post_init__(self):
        self.mimeType = "text/csv"
        self.path =  Path(self.name).with_suffix(".csv")
        self.g_id = ""
        self.name = self.path.name
    
    def set_path(self, dir):
        self.path = dir/self.path

    def delete(self):
        self.path.unlink(missing_ok=True)

class TelemetryFiles:
    # takes in an ibt file meta and a csv file meta
    def __init__(self, ibt, csv, driver):
        self.ibt = ibt
        self.csv = csv
        self.driver = driver

    def __repr__(self):
        return f"TelemFile({self.ibt.path.stem})"
