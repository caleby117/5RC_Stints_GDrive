from dataclasses import dataclass
from pathlib import Path
from telempaths import ROOT, CREDS, IBT_READER_PATH, TELEM_DL_FOLDER, TELEM_CSV_FOLDER
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
    g_parentid: str

    fields: ClassVar[str] = "files(mimeType, id, name, sha256Checksum)"
    
    def __post_init__(self):
        self.path = (TELEM_DL_FOLDER / self.name).with_suffix(".ibt")
        self.ext = self.path.suffix

@dataclass
class CsvFileMeta(FileMeta):
    # metadata for generated csv files
    g_parentid: str
    def __post_init__(self):
        self.mimeType = "text/csv"
        self.path = (TELEM_CSV_FOLDER / self.name).with_suffix(".csv")
        self.g_id = ""
        self.name = self.path.name
    



class TelemetryFiles:
    # takes in an ibt file meta and a csv file meta
    def __init__(self, ibt, csv):
        self.ibt = ibt
        self.csv = csv
