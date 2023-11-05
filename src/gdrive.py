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
from config import Config
from pathlib import Path, PurePath

VERIFY_TYPE = "sha256Checksum"

class DriveFolder:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.parentid = ''
        self.children = {}
        self.path = None

    def add_child(self, child):
        child.parentid = self.id

        # Only applicable to root folder
        if self.path is None:
            self.path = Path(self.name)
            child.path = self.path/child.name
        self.children[child.name] = child


    def get(self, path: PurePath):
        '''
        Recursively traverses the file tree to get to the folder as designated by path (relative)
        eg:

            Looking for root/a/b/c
                - call root.get(a/b/c)
                    - child = a
                    - call a.get((a/b/c).relative_to(a))
                        - (a/b/c).relative_to(a) = b/c
        '''
        # path is relative to this folder
        if not path.name:
            return self

        # get the path relative to this folder
        try:
            child = self.children[path.parts[0]]
        except KeyError:
            print(f"... /{self.name}/{path.parts[0]} does not exist")
            return None

        return child.get(path.relative_to(PurePath(path.parts[0])))

    
    def get_longest_existing_path(self, path):
        '''
        Gets longest existing path relative to this folder
        
        '''

        if not path.name:
            return path

        # walk the path until we get an error
        folder = self
        not_exists_path = path
        exists_path = Path()
        try:
            while True:
                folder = folder.children[not_exists_path.parts[0]]
                exists_path = exists_path/not_exists_path.parts[0]
                not_exists_path = not_exists_path.relative_to(Path(not_exists_path.parts[0]))
        except KeyError:
            return exists_path



    def find_by_id(self, path):
        '''
        Find child node by id
        '''
        pass


class DriveFSHelper:
    '''
    Helper class to model the drive file structure
    '''
    def __init__(self, fsnode):
        self.fs = fsnode
        self.folders_to_add = []


    def split_by_existence(self, path):
        longest_existing = self.fs.get_longest_existing_path(path)
        
        # get the path of the existing parent
        # Get the relative paths to this existing parent
        folders_to_create = path.relative_to(longest_existing)
        return longest_existing, folders_to_create


    def create_cached_path(self, exists, nexists, ids):
        '''
        Creates folder at path and creates the folders on the remote while the util is running.
            - Generate file ids now?
        '''

        existing_leaf = self.fs.get(exists)
        for part in nexists.parts:
            # add on to the folder in longest_existing
            part_folder = DriveFolder(part, ids.pop())
            existing_leaf.add_child(part_folder)
            existing_leaf = part_folder
            self.folders_to_add.append(part_folder)
        return existing_leaf


    def get(self, path):
        return self.fs.get(path)


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
    Each thread will use its own service and owns its state.
    '''
    def __init__(self, api, ver, creds, max_threads=4):
        self.serviceq = Queue(maxsize=max_threads)
        for i in range(max_threads):
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
                 scope=["https://www.googleapis.com/auth/drive"], 
                 max_threads=4, 
                 max_filesize=10*0x400*0x400*0x400,
                 root_pat=''):
        self.max_threads = max_threads

        creds_path = Path(Config.instance().PATHS.root)/Config.instance().AUTH.gdrive
        if not creds_path.exists():
            raise FileNotFoundError(f"ERROR: creds file not found at {creds_path.as_posix()}")

        creds = service_account.Credentials.from_service_account_file(filename=creds_path, scopes=scope)

        # serviceq is for putting service objects needed in a thread-safe manner
        # ONLY ACCESS THROUGH self.provicder.get_service() method
        self.provider = ServiceProvider("drive", "v3", creds, max_threads=max_threads)
        self._max_filesize = max_filesize

        # set up root folder id
        #root_info = self.get_folder_info(root_pat)
        self.g_root = None #DriveFolder(root_info["name"], root_info["id"])
        self.build_fs()



    def download_files(self, files):
        downloaded_files = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
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


    def generate_fileids(self, count=1):
        with self.provider.get_service() as service:
            return _api_call(service.files().generateIds, count=count, path="ids")


    def build_fs(self):
        '''
        Makes a local representation of the gdrive fs (folders only) rooted at root
        dfs traverse the file system
        '''

        # Get a list of all available folders
        all_folders = self._get_folders_info_priv()

        # cache parent-child relationships
        # edge(id1, id2) => id1 is parent of id2

        cache = {}
        # print(all_folders)
        for folder in all_folders:
            try:
                folder["parents"]
            except KeyError:
                # this is our root folder
                self.g_root = DriveFolder(folder["name"], folder["id"])
                continue
            try:
                cache[folder["parents"][0]].append(folder)

            except KeyError:
                cache[folder["parents"][0]] = [folder]

        # perform dfs to build the fs tree 
        to_search = [self.g_root]
        while to_search:
            cur_folder = to_search.pop()
            try:
                cur_folder_children = cache[cur_folder.id]
            except KeyError:
                continue

            for folder in cur_folder_children:
                child = DriveFolder(folder["name"], folder["id"])
                cur_folder.add_child(child)
                to_search.append(child)
        
    
    def get_fs_root(self):
        return self.g_root

    def get_folder_id(self, path):
        """
        Uses the cached copy of the fs to find the folder id
        """
        # print(path)
        return self.g_root.get(PurePath(path)).id



    def get_folders_info(self, pattern, parent=None):
        if not pattern:
            return None

        # print(f"get_folders_info {pattern=}")

        '''
        Gets folder id of the specified folder path 
        eg name = "telemetry/csv"
            find id of telemetry folder
            call getFolderId(csv, parent=telemetry_id)
        '''

        return _traverse_nested(self._get_folders_info_priv, pattern)


    def get_drivers_ibts_info(self, drivers, folder_id):
        '''
        Gets file information for each driver. 
        '''
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = {executor.submit(self.get_ibt_file_info, 
                                       folder_id, 
                                       namecontains=[".ibt", driver.name]):driver for driver in drivers}
            for future in concurrent.futures.as_completed(futures):
                try:
                    files = list(future.result())
                except Exception as e:
                    traceback.print_exception(future.exception())
                else:
                    if not future.result():
                        print(f"{futures[future].name} has no ibt files.")
                        continue
                    futures[future].create_file_metas(files)


    def get_ibt_file_info(self, folder_id, namecontains=[".ibt"]):

        #Finds ibt files in the folder_id
        conditions = [f"name contains '{x}'" for x in namecontains]
        conditions.append(f"'{folder_id}' in parents")
        query = " and ".join(conditions)

        with self.provider.get_service() as service:
            results = _api_call(service.files().list, path='files', q=query, fields=IbtFileMeta.fields)

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
            uploads = {executor.submit(self.upload_file, f): f for f in files}
            for future in concurrent.futures.as_completed(uploads):
                try:
                    telem_meta = future.result()
                except Exception as e:
                    traceback.print_exception(future.exception())
                    print(f"Error uploading {uploads[future].csv.name}. Skipping.")
                else:
                    if telem_meta.csv.g_id:
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


    def create_folder(self, folder):
        metadata = {
            "name": folder.name,
            "mimeType": "application/vnd.google-apps.folder",
            "id":folder.id,
            "parents": [folder.parentid]
        }
        with self.provider.get_service() as service:
            id = _api_call(service.files().create, path='id', body=metadata, fields='id')

        return id


    def _get_folders_info_priv(self, **kwargs):
        '''
        Gets all folders
        '''

        fields = "files(id, name, parents)"
        query = "mimeType='application/vnd.google-apps.folder'"

        with self.provider.get_service() as service:
            result = _api_call(service.files().list, path='files', q=query, fields=fields)

        return result


    def get_folder_info(self, name, parent=None, **kwargs):
        # accepts paths and traverses the filesystem recursively to get to the correct folder
        fields = "files(id, name, parents)"
        query = f"mimeType='application/vnd.google-apps.folder' and name='{name}'"

        # if this is a subfolder from a specified parent
        if parent:
            query += f" and '{parent}' in parents"

        with self.provider.get_service() as service:
            try:
                result = _api_call_ret_single(service.files().list, path='files/_0', q=query, fields=fields)
            except (KeyError, IndexError) as e:
                print(f"Get folder id: {name} not found")
                result = ''
        return result


def _dict_get_wrapper(key, parent=None, resource=None):
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


def _api_call_ret_single(method, path=None, **kwargs):
    # returns a single entry; returns None if there is no result
    result = method(**kwargs, pageSize=1).execute()
    if path:
        return _traverse_nested(_dict_get_wrapper, path, resource=result)
    return result


def _api_call(method, path=None, **kwargs):
    result = method(**kwargs).execute()
    if not result:
        raise IOError(f"{result=}, {kwargs=}")
    if path:
        return _traverse_nested(_dict_get_wrapper, path, resource=result)
    return result


def _traverse_nested(method, path, resource=None, **kwargs):
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


