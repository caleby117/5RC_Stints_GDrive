import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
from hashlib import sha256
import pathlib
from filemeta import IbtFileMeta, CsvFileMeta, TelemetryFiles
from queue import Queue

VERIFY_TYPE = "sha256Checksum"

class SHAVerifier:
    cur_digest = ""
    @staticmethod
    def verify(file, expected_digest):
        hasher = sha256()
        # file: bytes io obj
        hasher.update(file.getvalue())
        SHAVerifier.cur_digest = hasher.hexdigest()
        return (hasher.hexdigest(), hasher.hexdigest() == expected_digest)


class DriveApiHandler:
    def __init__(self, creds_file, scope=["https://www.googleapis.com/auth/drive"]):
        self.creds = service_account.Credentials.from_service_account_file(filename=creds_file, scopes=scope)
        self.drive = build("drive", "v3", credentials=self.creds)


    def download_file(self, file_meta):
        content_request = self.drive.files().get_media(fileId=file_meta.ibt.g_id, acknowledgeAbuse=True)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, content_request)
        done = False
        print(f"Downloading to {file_meta.ibt.path}: {0:>3}%\r", end='')
        while not done:
            status, done = downloader.next_chunk()
            print(f"Downloading to {file_meta.ibt.path}: {int(status.progress()*100)}%\r", end='')
        print()

        digest, verified = SHAVerifier.verify(file, file_meta.ibt.g_checksum)
        if not verified:
            print("File Integrity failed.") 
            print(f"Expected: {file_meta.ibt.g_checksum}\nGot: {digest}")
            print(f"Ignoring file {file_meta.ibt.name}")
            return False
        
        ret = True
        with open(file_meta.ibt.path, 'wb') as f:
            try:
                f.write(file.getvalue())
            except OSError as e:
                print(e)
                print(f"OS Error. Ignoring file {file_meta.ibt.name}")
                ret = False

        return ret

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
        results = self._api_call(self.drive.files().list, path='files', q=query, fields=IbtFileMeta.fields)
        if not results:
            raise IOError(f"{results=}, {IbtFileMeta.fields=}, {query=}")
        return map(
                lambda x: IbtFileMeta(
                    x["name"], 
                    x["id"], 
                    x["mimeType"], 
                    x["sha256Checksum"], 
                    folder_id), 
                results
            )


    def upload_file(self, file): #csv_to_upload, parentid='', meta_mimetype='', upload_mimetype=''):
        # Takes in filemeta obj and returns same object but updated
        # Returns file id
        metadata = {
            "name": file.csv.path.name,
            "mimeType": file.csv.mimeType,
            "parents": [file.csv.g_parentid]
        }
        media = MediaFileUpload(file.csv.path.as_posix(), mimetype=file.csv.mimeType, resumable=False)
        print(f"Uploading file {file.csv.path.as_posix()}")
        try:
            g_file = self.drive.files().create(body=metadata, uploadType="multipart", media_body=media, fields='id').execute()
        except HttpError as e:
            print(e)
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

        return self._api_call_ret_single(self.drive.files().list, path='files/_0/id', q=query, fields=fields)


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
