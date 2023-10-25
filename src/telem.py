from gdrive import DriveApiHandler
from pathlib import Path
import subprocess
import sys

class TelemDataHandler:
    def __init__(self, l_ibt_path, l_csv_path, d_ibt_pathpat="telemetry/ibt", d_csv_pathpat="telemetry/csv", ibt_ignores_txt=Path().cwd()/".ibtignore", ibt_reader_path=None, creds=''):
        self.l_ibt_path = l_ibt_path
        self.l_csv_path = l_csv_path
        self.ibt_reader_path = ibt_reader_path
        self.service = DriveApiHandler(creds)
        self.d_ibt_pathid = self.service.get_folder_id("telemetry/ibt")
        self.d_csv_pathid = self.service.get_folder_id("telemetry/csv")
        self.ibt_ignores_txt = ibt_ignores_txt
        self.files_processing = {}
        try:
            with open(self.ibt_ignores_txt, "r") as ignores:
                self.ignores = set(map(lambda x: x.rstrip(), ignores.readlines()))
                
        except FileNotFoundError:
            # file not found - create new file
            with open(self.ibt_ignores_txt, "w") as f:
                pass
            self.ignores = set()

        
    def download_unprocessed_ibt(self):
        # Downloads unprocessed ibt files and returns a list of file(id, name)
        ibt_file_metas = self.service.get_ibt_file_info(self.d_ibt_pathid)
        downloaded_files = []
        
        # read file of the ids that have already been read and ignore them
        for file in ibt_file_metas:
            ibt_filename = file['name']
            ibt_fileid = file['id']
            if ibt_fileid not in self.ignores:
                # We've not seen this file yet. Process it
                ibt_filepath = self.l_ibt_path / ibt_filename
                self.service.download_file(ibt_fileid, ibt_filepath.as_posix())
                downloaded_files.append(file)
                self.files_processing[Path(ibt_filename).stem] = ibt_fileid
            # else do nothing 
            else:
                print(f"File {ibt_filename} already processed - ignoring")

        return downloaded_files 


    def process_ibt_files(self, files):
        # Process the ibt files with the executable
import os
        # process the ibt files by file name 
        filenames = list(map(lambda x: x['name'], files))
        to_upload = []
        for file in filenames:
            csv_filename = Path(file).with_suffix('.csv')
            if self._exec_stint_util(file, csv_filename) == 0:
                # successfully extracted data from the .ibt file
                to_upload.append(csv_filename)
            
        return to_upload


    def upload_csv_files(self, files):
        uploaded = []
        # files: the file name of the csv we just exported
        for csvfile in files:
            # uploads the csv files to the Drive
            # for google spreadsheet, use "application/vnd.google-apps.spreadsheet"
            fileid = self.service.upload_file(
                self.l_csv_path/csvfile, 
                parentid=self.d_csv_pathid, 
                meta_mimetype="text/csv",
                upload_mimetype="text/csv")
            
            uploaded.append(fileid)
            # add to ignore list
            self.ignores.add(self.files_processing[csvfile.stem])
            del self.files_processing[csvfile.stem]

        # Once the csv files are all uploaded, write to ignores file
        with open(self.ibt_ignores_txt, "w") as f:
            for id in self.ignores:
                f.write(id+'\n')

        return uploaded

    def _exec_stint_util(self, ibt_filename, csv_filename, vars=None):
        if self.ibt_reader_path is None:
            raise FileNotFoundError("Stint Time Util not initialised in this object")
        if vars is None:
            vars = self.ibt_reader_path.parent/"SampleVars.txt"
        retcode = None
        exec = [
            self.ibt_reader_path.as_posix(),
            "--vars", vars.as_posix(),
            "-o", (self.l_csv_path/csv_filename).as_posix(),
            (self.l_ibt_path/ibt_filename).as_posix()
        ]

        with subprocess.Popen(exec, stdout=sys.stdout, stderr=sys.stderr) as proc:
            while proc.poll() is None:
                pass
            retcode = proc.poll()

        return retcode
