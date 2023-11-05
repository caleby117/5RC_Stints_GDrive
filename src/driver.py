from pathlib import Path
from gdrive import DriveApiHandler
from telem import TelemDataHandler
from config import Config
from filemeta import CsvFileMeta, TelemetryFiles


class Driver:
    ignores = set()

    @classmethod
    def _filter_ignores(self, file):
        if file.g_id in Driver.ignores:
            print(f"File {file.name} already processed - ignoring")
            return False
        return True

    def __init__(self, name, telem=None):
        self.name = name
        self.telem = telem
        self.l_csv_path = Path(
            Config.instance().PATHS.csv_folder.replace("%t", "csv").replace("%d", self.name)
        )
        self.l_ibt_path = Path(
            Config.instance().PATHS.ibt_folder.replace("%t", "ibt").replace("%d", self.name)
        )
        csv_g_path = Path(Config.instance().DRIVE.csv_path.replace("%d", self.name))

        # Get the folder id of the folder that we're supposed to upload to
        self.g_csv_folder = self.telem.drivecache.get(csv_g_path)

        # if this fails, then build the new folder in the gdrive while the util is running
        if not self.g_csv_folder:
            self.g_csv_folder = self.telem.create_folders_at(csv_g_path)

        self.files = []


    def set_telem_handler(self, telem):
        '''
        Sets the telem handler object to poll for fs structure etc from
        '''
        self.telem = handler

    def set_local_filepaths(self):
        for file in self.files:
            file.ibt.set_path(Config.instance().PATHS.root/self.l_ibt_path)
            file.csv.set_path(Config.instance().PATHS.root/self.l_csv_path)

    def create_file_metas(self, ibt_files):
        self.files = list(
            map(
                lambda x: TelemetryFiles(x, CsvFileMeta(x.name, self.g_csv_folder.id), self.name),
                filter(Driver._filter_ignores, ibt_files)
            )
        )
        self.set_local_filepaths()
        
