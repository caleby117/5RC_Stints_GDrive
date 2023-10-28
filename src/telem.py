from gdrive import DriveApiHandler
from pathlib import Path
import subprocess
import sys
import concurrent.futures
from filemeta import CsvFileMeta, TelemetryFiles, IbtFileMeta

class TelemDataHandler:
    def __init__(self, l_ibt_path, l_csv_path, d_ibt_pathpat="telemetry/ibt", d_csv_pathpat="telemetry/csv", ibt_ignores_txt=Path().cwd()/".ibtignore", ibt_reader_path=None, creds=''):
        self.l_ibt_path = l_ibt_path
        self.l_csv_path = l_csv_path
        self.ibt_reader_path = ibt_reader_path
        self.service = DriveApiHandler(creds)
        self.g_ibt_pathid = self.service.get_folder_id("telemetry/ibt")
        self.g_csv_pathid = self.service.get_folder_id("telemetry/csv")
        self.ibt_ignores_txt = ibt_ignores_txt
        try:
            with open(self.ibt_ignores_txt, "r") as ignores:
                self.ignores = set(map(lambda x: x.rstrip(), ignores.readlines()))
                
        except FileNotFoundError:
            # file not found - create new file
            with open(self.ibt_ignores_txt, "w") as f:
                pass
            self.ignores = set()

        
    def download_unprocessed_ibt(self):
        # Downloads unprocessed ibt files and returns a list of TelemetryFiles()
        files = list(map(lambda x: TelemetryFiles(x, CsvFileMeta(x.name, self.g_csv_pathid)),
                     self.service.get_ibt_file_info(self.g_ibt_pathid)))
        if not files:
            raise IOError(f"{files=}, {self.g_ibt_pathid=}")
        downloaded_files = []
        
        # read file of the ids that have already been read and ignore them
        for file in files:
            if file.ibt.g_id in self.ignores:
                print(f"File {file.ibt.name} already processed - ignoring")
                continue
                
            # We've not seen this file yet. Process it
            if self.service.download_file(file):
                downloaded_files.append(file)

        return downloaded_files 


    def process_ibt_files(self, files):
        # Process the ibt files with the executable
        # process the ibt files by file name 
        to_upload = []
        for file in files:
            if self._exec_stint_util(file) == 0:
                # successfully extracted data from the .ibt file
                to_upload.append(file)

                # delete ibt file after the data is extracted
                file.ibt.path.unlink()
            
        return to_upload


    def upload_csv_files(self, files):
        # TODO: IMPLEMENT FILEMETA class
        # Track upload status for each uploading thread
        uploaded = []
        for f in files:
            self.service.upload_file(f)
            self.ignores.add(f.csv.g_id)
        #with concurrent.futures.ThreadPoolExecutor() as executor:
        #    uploads = [executor.submit(self.service.upload_file, f) for f in files]
        #    for future in concurrent.futures.as_completed(uploads):
        #        print(future.result())

        # TODO: add to ignore list

        # Once the csv files are all uploaded, write to ignores file
        with open(self.ibt_ignores_txt, "w") as f:
            for id in self.ignores:
                f.write(id+'\n')

        return uploaded

    def _exec_stint_util(self, files, vars=None):
        if not vars:
            vars = self.ibt_reader_path.parent/"SampleVars.txt"
        if self.ibt_reader_path is None:
            raise FileNotFoundError("Stint Time Util not initialised in this object")
        retcode = None
        exec = [
            self.ibt_reader_path.as_posix(),
            "--vars", vars.as_posix(),
            "-o", files.csv.path.as_posix(),
            (files.ibt.path).as_posix()
        ]

        with subprocess.Popen(exec, stdout=sys.stdout, stderr=sys.stderr) as proc:
            while proc.poll() is None:
                pass
            retcode = proc.poll()

        return retcode


