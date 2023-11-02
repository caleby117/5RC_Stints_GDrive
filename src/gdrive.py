import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
from hashlib import sha256
import pathlib
from filemeta import IbtFileMeta, CsvFileMeta, TelemetryFiles
import concurrent.futures
from queue import Queue
from contextlib import contextmanager
import traceback

VERIFY_TYPE = "sha256Checksum"

class SHAVerifier:
    cur_digest = ""
    @staticmethod
    def verify(file, expected_digest):
        hasher = sha256()
        # file: bytes io obj
        hasher.update(file.read())
        SHAVerifier.cur_digest = hasher.hexdigest()
        return (hasher.hexdigest(), hasher.hexdigest() == expected_digest)


class ServiceProvider:
    '''
    Helper object that provides the thread-safe queue for each api service object.
    Each thread using its own service will ensure that api calls and downloads are done in parallel.
    '''
    def __init__(self, api, ver, creds, maxprocs=4):
        self.serviceq = Queue(maxsize=max_procs)
        for i in range(max_procs):
            self.serviceq.put(build(api, ver, credentials=creds))
    
    @contextmanager
    def get_service(self):
        service = self.serviceq.get()
        try:
            yield service
        finally:
            self.serviceq.put(service)



class DriveApiHandler:
    def __init__(self, 
                 creds_file, 
                 scope=["https://www.googleapis.com/auth/drive"], 
                 max_procs=4, 
                 max_filesize=10*0x400*0x400*0x400):

        self.creds = service_account.Credentials.from_service_account_file(filename=creds_file, scopes=scope)

        # serviceq is for putting service objects needed in a thread-safe manner
        # ONLY ACCESS THROUGH self._get_service() method
        self.provider = ServiceProvider("drive", "v3", self.creds)
        self._max_filesize = max_filesize

    def download_files(self, files):
        downloaded_files = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(self.download_file, file):file for file in files}
            for future in concurrent.futures.as_completed(futures):
                try:
                    l_ibt = future.result()
                except Exception as e:
                    traceback.print_exception(future.exception())
                    print(f"Error downloading {futures[future].ibt.name}. Skipping")
                else:
                    if not future.result():
                        print(f"{futures[future].ibt.name} could not be downloaded.")
                        continue
                    print(f"{futures[future].ibt.name} downloaded")
                    downloaded_files.append(futures[future])
        return downloaded_files



    def download_file(self, file_meta):
        # Thread-safe implementation of downloading files from Google Drive

        # Do not download the file if it is too big
        if file_meta.ibt.filesize > self._max_filesize:
            file_meta.ibt.path.unlink()
            print(f"Could not download file {file_meta.ibt.name} - too big")

        # Get a service
        with self.provider.get_service() as service:
            content_request = service.files().get_media(fileId=file_meta.ibt.g_id, acknowledgeAbuse=True)

            # directly write the file to the disk
            with open(file_meta.ibt.path, "wb") as ibt:
                downloader = MediaIoBaseDownload(ibt, content_request)
                done = False
                print(f"Downloading to {file_meta.ibt.path}:\r", end='')
                too_big = False
                while not done:
                    status, done = downloader.next_chunk()

                    # Ensure that the file is not too big to download
                    if status.total_size > self._max_filesize:
                        print(f"File {file_meta.ibt.name}"
                              f"({(status.total_size//self._max_filesize)*10}GB) is too big to download")

                        break
                    print(f"Downloading to {file_meta.ibt.path}: {int(status.progress()*100):>3}%\r", end='')
                print()


        # Verify the SHA256 checksum
        with open(file_meta.ibt.path, "rb") as file:
            digest, verified = SHAVerifier.verify(file, file_meta.ibt.g_checksum)
            if not verified:
                print("File Integrity failed.") 
                print(f"Expected: {file_meta.ibt.g_checksum}\nGot: {digest}")
                print(f"Ignoring file {file_meta.ibt.name}")
                return False
            
        return True



    def get_folder_id(self, pattern, parent=None):
        if not pattern:
            return ''

        '''
        Gets folder id of the specified folder path 
        eg name = "telemetry/csv"
            find id of telemetry folder
            call getFolderId(csv, parent=telemetry_id)
        '''
        return self._traverse_nested(self._get_folder_id_priv, pattern)


    def get_ibt_file_info(self, folder_id, namecontains=".ibt"):

        #Finds ibt files in the folder_id
        query = f"name contains '{namecontains}' and '{folder_id}' in parents"
        with self._get_service() as service:
            results = self._api_call(service.files().list, path='files', q=query, fields=IbtFileMeta.fields)

        return map(
                lambda x: IbtFileMeta(
                    x["name"], 
                    x["id"], 
                    x["mimeType"], 
                    x["sha256Checksum"], 
                    int(x["size"]),
                    folder_id), 
                results
            )

    
    def upload_files(self, files):
        # Uploads files to google drive
        uploaded = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            uploads = {executor.submit(self.service.upload_file, f): f for f in files}
            for future in concurrent.futures.as_completed(uploads):
                try:
                    telem_meta = future.result()
                except Exception as e:
                    traceback.print_exception(future.exception())
                    print(f"Error uploading {uploads[future].csv.name}. Skipping.")
                else:
                    if telem_meta.csv.g_id:
                        self.ignores.add(telem_meta.ibt.g_id)
                        uploaded.append(telem_meta)
        return uploaded


    def upload_file(self, file): 
        # Takes in filemeta obj and returns same object but updated with the new fileid
        metadata = {
            "name": file.csv.path.name,
            "mimeType": file.csv.mimeType,
            "parents": [file.csv.g_parentid]
        }
        media = MediaFileUpload(file.csv.path.as_posix(), mimetype=file.csv.mimeType, resumable=False)
        print(f"Uploading file {file.csv.path.as_posix()}")
        with self.provider.get_service() as service:
            try:
                g_file = service.files()\
                                .create(body=metadata, 
                                        uploadType="multipart", 
                                        media_body=media, 
                                        fields='id')\
                                .execute()
            except HttpError as e:
                traceback.print_exc()
                print(f"IGNORING {file.csv.path.as_posix()}")
            else:
                file.csv.g_id = g_file["id"]

        return file


    def _get_folder_id_priv(self, name, parent=None, **kwargs):
        # accepts paths and traverses the filesystem recursively to get to the correct folder
        fields = "files(id)"
        query = f"mimeType='application/vnd.google-apps.folder' and name='{name}'"

        # if this is a subfolder from a specified parent
        if parent:
            query += f" and '{parent}' in parents"

        with self._get_service() as service:
            result = self._api_call_ret_single(service.files().list, path='files/_0/id', q=query, fields=fields)
        return result


    def _dict_get_wrapper(self, key, parent=None, resource=None):
        # format for getting elements from list index: key="_<idx>"
        # Leading '_' with a non-number string after that will not be taken as a list index
        if not parent:
            # use resource if we're at root
            return resource.get(key, None)
        idx = None
        if key[0] == '_':
            try:
                idx = int(key[1:])
            except ValueError:
                # take the string literally as a key
                pass

            else:
                return parent[idx]

        return parent.get(key, None)

    
    def _api_call_ret_single(self, method, path=None, **kwargs):
        # returns a single entry; returns None if there is no result
        result = method(**kwargs, pageSize=1).execute()
        if path:
            return self._traverse_nested(self._dict_get_wrapper, path, resource=result)
        return result

    
    def _api_call(self, method, path=None, **kwargs):
        result = method(**kwargs).execute()
        if not result:
            raise IOError(f"{result=}, {kwargs=}")
        if path:
            return self._traverse_nested(self._dict_get_wrapper, path, resource=result)
        return result


    def _traverse_nested(self, method, path, resource=None, **kwargs):
        '''
        Traverses a nested structure, be it the fs on gdrive or nested JSON/dict

        '''
        #print(f"_traverse_nested: {method=}, {path=}, {resource=}")
        if not path:
            return None

        path_elements = path.split('/')
        if len(path_elements) == 1:
            # direct access
            return method(path_elements[0], resource=resource, **kwargs)

        # else for nested stuff, traverse to get the element
        parent = None
        for elem in path_elements:
            #print(f"{parent=}, {elem=}")
            parent = method(elem, parent=parent, resource=resource, **kwargs)

        #print(f"_traverse_nested: Exiting from  {method=}, {path=}, {resource=}")

        return parent

    @contextmanager
    def _get_service(self):
        # acquire a service from the queue
        service = self._serviceq.get()
        try:
            yield service
        finally:
            # When the thread is done, return the service to the queue
            self._serviceq.put(service)

