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
        # Filters out files whose ids are already in .ibtignore
        files = list(
            map(
                lambda x: TelemetryFiles(x, CsvFileMeta(x.name, self.g_csv_pathid)),
                filter(lambda f: self._filter_ignores(f),
                         self.service.get_ibt_file_info(self.g_ibt_pathid)
                )
            )
        )

        #downloads_status = self.service.download_files_async(files)
        downloaded_files = []

        '''
        for file, status in zip(files, downloads_status):
            if not status:
                print(f"Error downloading {file.ibt.name} - skipping")
                continue
            print(f"Downloaded {file.ibt.name}")
            downloaded_files.append(file)


        '''
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_dl = {executor.submit(self.service.download_file, file):file for file in files}
            for future in concurrent.futures.as_completed(future_dl):
                try:
                    l_ibt = future.result()
                except Exception as e:
                    print(future.exception())
                    print(f"Error downloading {future_dl[future].ibt.name}. Skipping")
                else:
                    if not future.result():
                        print(f"{future_dl[future].ibt.name} could not be downloaded.")
                        continue
                    print(f"{future_dl[future].ibt.name} downloaded.")
                    downloaded_files.append(future_dl[future])

        return downloaded_files 


    def process_ibt_files(self, files):
        # Process the ibt files with the executable
        # process the ibt files by file name 
        to_upload = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            processes = {executor.submit(self._exec_stint_util, file): file for file in files}
            for future in concurrent.futures.as_completed(processes):
                try:
                    retval = future.result()
                except Exception as e:
                    print(e)
                    print(f"Error processing {processes[future].ibt.name}. Skipping")
                else:
                    if retval == 0:
                        to_upload.append(processes[future])

        # Delete all ibt files
        for file in files:
            file.ibt.path.unlink()
            
        return to_upload


    def upload_csv_files(self, files):
        uploaded = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            uploads = {executor.submit(self.service.upload_file, f): f for f in files}
            for future in concurrent.futures.as_completed(uploads):
                try:
                    telem_meta = future.result()
                except Exception as e:
                    print(e)
                    print(f"Error uploading {uploads[future].csv.name}. Skipping.")
                else:
                    if telem_meta.csv.g_id:
                        self.ignores.add(telem_meta.ibt.g_id)
                        uploaded.append(telem_meta)


        # Once the csv files are all uploaded, write to ignores file
        with open(self.ibt_ignores_txt, "w") as f:
            for id in self.ignores:
                f.write(id+'\n')

        return uploaded


    def _filter_ignores(self, ibt):
        if ibt.g_id in self.ignores:
            print(f"File {ibt.name} already processed - ignoring")
            return False
        return True

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
        print(f"Extracting data from {files.ibt.path.as_posix()}")
        with subprocess.Popen(exec, stdout=sys.stdout, stderr=sys.stderr) as proc:
            while proc.poll() is None:
                pass
            retcode = proc.poll()
        print("Completed")

        return retcode


