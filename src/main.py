from gdrive import DriveApiHandler
from telem import TelemDataHandler
from pathlib import Path
from telempaths import ROOT, CREDS, IBT_READER_PATH, TELEM_DL_FOLDER, TELEM_CSV_FOLDER


def main():
    '''
    main.py:
        - Gets all new ibt files that have been uploaded to the google drive in the past x minutes
        - Downloads the ibt files into some temp storage and runs the 5RC_Stint_Time_Util on it to produce a csv file
        - Uploads the csv file to the drive

    '''

    # define creds and scope of the API calls on behalf of the service account

    '''
    USE THE GOOGLE DRIVE API TO DO THE REQUESTS
        developers.google.com/drive/api/reference/rest/v3

    '''

    # Create folders if they do not exist 
    TELEM_CSV_FOLDER.mkdir(parents=True, exist_ok=True)
    TELEM_DL_FOLDER.mkdir(parents=True, exist_ok=True)

    telem = TelemDataHandler(TELEM_DL_FOLDER, TELEM_CSV_FOLDER, ibt_reader_path=IBT_READER_PATH, creds=CREDS, ibt_ignores_txt=ROOT/"src"/".ibtignore")
    
    ibt_files = telem.download_unprocessed_ibt()
    csv_to_upload = telem.process_ibt_files(ibt_files)
    uploaded_files = telem.upload_csv_files(csv_to_upload)
    print(uploaded_files)
    


if __name__ == "__main__":
    main()

