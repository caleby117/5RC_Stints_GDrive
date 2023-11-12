from gdrive import DriveApiHandler, DriveFSHelper
from pathlib import Path
import subprocess
import sys
import concurrent.futures
from filemeta import CsvFileMeta, TelemetryFiles, IbtFileMeta
from config import Config
from threading import Thread



class TelemDataHandler:
    def __init__(self, 
                 l_ibt_path, 
                 l_csv_path, 
                 g_ibt_pathpat="telemetry/ibt", 
                 g_csv_pathpat="telemetry/csv", 
                 ibt_ignores_txt=Path().cwd()/".ibtignore", 
                 ibt_reader_path=None, 
                 creds=''):
        self.l_ibt_path = l_ibt_path
        self.l_csv_path = l_csv_path
        self.ibt_reader_path = ibt_reader_path
        self.service = DriveApiHandler(root_pat=Config.instance().DRIVE.root_pat)
        self.g_ibt_pathid = self.service.get_folder_id(g_ibt_pathpat)
        self.ibt_ignores_txt = ibt_ignores_txt
        self.drivecache = DriveFSHelper(self.service.g_root)


        self.setup_ibtignore()

    def setup_ibtignore(self):
        '''
        Deal with ibtignore file based on the config settings
        
        Config settings:
            - Config.use_ibtignore: bool        # Whether to use ibtignore file
            - Config.write_ibtignore: bool      # Whether to write to/modify ibtignore file
            - Config.fresh_ibtignore: bool      # Whether to create a fresh ibtignore file
        '''

        # If Config.fresh_ibtignore is set, delete the existing file
        if bool(int(Config.instance().IBTIGNORE.fresh_ibtignore)):
            self.ibt_ignores_txt.unlink(missing_ok=True)

        # If Config.use_ibtignore is set, read from the file if it exists
        if bool(int(Config.instance().IBTIGNORE.use_ibtignore)):
            if self.ibt_ignores_txt.exists():
                with open(self.ibt_ignores_txt, "r") as ignores:
                    self.ignores = set(map(lambda x: x.rstrip(), ignores.readlines()))
            else: 
                self.ignores = set()
        else:
            self.ignores = set()

    
    def create_folders_at(self, path):
        exists, nexists = self.drivecache.split_by_existence(path)
        # print(f"create_folders_at: {exists=}, {nexists=}")
        # ask Google to generate folder ids for the nonexistent folders
        ids = self.service.generate_fileids(count=len(nexists.parts))
        # print(f"{ids=}")

        # create the folders in the local drive fs cache, push to remote when running the Util
        return self.drivecache.create_cached_path(exists, nexists, ids)




    def download_drivers_ibt(self, drivers):
        # Downloads the drivers' ibt files
        # get ibtfileinfo
        # download files

        # get ibt file info for each driver
        # all the information as to where each file should eventually end up is 
        # saved in the file object
        # driver object is to help with configuring each drivers' filepaths
        self.service.get_drivers_ibts_info(drivers, self.g_ibt_pathid)

        all_files = []
        for driver in drivers:
            all_files.extend(driver.files)

        # download all the files
        return self.service.download_files(all_files)

        
    def download_unprocessed_ibt(self):
        '''
        NOT USED
        Downloads unprocessed ibt files and returns a list of TelemetryFiles()
            - Filters out files whose ids are already in .ibtignore
        '''
        files = list(
            map(
                lambda x: TelemetryFiles(x, CsvFileMeta(x.name, self.g_csv_pathid)),
                filter(lambda f: self._filter_ignores(f),
                         self.service.get_ibt_file_info(self.g_ibt_pathid)
                )
            )
        )

        return self.service.download_files(files)


    def process_telemetry(self, files):
        '''
        Processes the telemetry as well as create any google files that need to be created
        '''

        # Create folders one by one
        create_folder_thread = Thread(target=self.sync_folders)

        # start the threads
        create_folder_thread.start()

        processed_files = self.process_ibt_files(files)

        create_folder_thread.join()

        return processed_files
        
    
    def sync_folders(self):
        for folder in self.drivecache.folders_to_add:
            # folders have already been arranged by hierarchy, leaves at the end
            self.service.create_folder(folder)
            self.drivecache.folders_to_add = []
        

    def process_ibt_files(self, files):
        '''
        Process the ibt files with the executable
            - process the ibt files by file name 

        Use multithreading as we are already spawning new processes from this so the GIL 
        doesn't really matter at all

        '''
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

            
        return to_upload


    def upload_csv_files(self, files):
        '''
        Passes the csv files to the drive object to upload
        Write to ibtignore file once the files are successfully uploaded
        '''
        uploaded = self.service.upload_files(files)
        for file in uploaded:
            self.ignores.add(file.ibt.g_id)

        # Once the csv files are all uploaded, write to ignores file
        self.write_ibtignores()

        return uploaded


    def write_ibtignores(self):
        # Only write to the ibtignore file if Config.IBTIGNORE.write_ibtignore is set
        if bool(Config.instance().IBTIGNORE.write_ibtignore):
            with open(self.ibt_ignores_txt, "w") as f:
                for id in self.ignores:
                    f.write(id+'\n')


    def cleanup(self, drivers):
        '''
        Delete all csv and ibt files from local fs
        '''
        for driver in drivers:
            for filemeta in driver.files:
                if int(Config.instance().FILES.keep_ibt) == 0:
                    filemeta.ibt.delete()
                if int(Config.instance().FILES.keep_csv) == 0:
                    filemeta.csv.delete()


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


