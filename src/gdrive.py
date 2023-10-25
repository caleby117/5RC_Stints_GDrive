import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
from hashlib import sha256
import pathlib


class DriveApiHandler:
    def __init__(self, creds_file, scope=["https://www.googleapis.com/auth/drive"]):
        self.creds = service_account.Credentials.from_service_account_file(filename=creds_file, scopes=scope)
        self.drive = build("drive", "v3", credentials=self.creds)
        self.activity = build("driveactivity", "v2", credentials=self.creds)


    def download_file(self, fileid, filepath):
        content_request = self.drive.files().get_media(fileId=fileid, acknowledgeAbuse=True)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, content_request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Downloading to {filepath}: {int(status.progress()*100)}%\r", end='')
        print()

        verify_type = "sha256Checksum"

        checksum = self.drive.files().get(fileId=fileid, fields=verify_type).execute()[verify_type]

        hasher = sha256()
        hasher.update(file.getvalue())
        if hasher.hexdigest() == checksum:
            with open(filepath, 'wb') as f:
                f.write(file.getvalue())

        else:
            raise IOError(f"File Integrity failed. \nExpected: {checksum}\nGot: {hasher.hexdigest()}")

    def get_folder_id(self, name, parent=None):
        if not name:
            return ''

        '''
        Gets folder id of the specified folder path 
        eg name = "telemetry/csv"
            find id of telemetry folder
            call getFolderId(csv, parent=telemetry_id)
        '''
        return self._traverse_nested(self._get_folder_id_priv, name)


    def get_ibt_file_info(self, folder_id):

        #Finds ibt files in the folder_id
        query = f"mimeType='application/octet-stream' and name contains '.ibt' and '{folder_id}' in parents"
        fields = 'files(id, name)'
        results = self._api_call(self.drive.files().list, path='files', q=query, fields=fields)
        return results


    def upload_file(self, csv_to_upload, parentid='', meta_mimetype='', upload_mimetype=''):
        # Returns file id
        metadata = {
            "name": csv_to_upload.name,
            "mimeType": meta_mimetype,
            "parents": [parentid]
        }
        media = MediaFileUpload(csv_to_upload.as_posix(), mimetype=upload_mimetype, resumable=True)
        print(f"Uploading file {csv_to_upload.as_posix()}")
        file = self.drive.files().create(body=metadata, media_body=media, fields='id').execute()
        
        return file["id"]


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
