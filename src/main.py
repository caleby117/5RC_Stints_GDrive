from gdrive import DriveApiHandler
from telem import TelemDataHandler
from driver import Driver
from pathlib import Path
from config import Config
import argparse
from sys import argv


def main():
    '''
    main.py:
        - Searches the google drive for .ibt files to download
        - Downloads the ibt files into some temp storage and runs the 5RC_Stint_Time_Util on it to produce a csv file
        - Uploads the csv file to the drive
        - Removes the temp storage

    '''

    # define creds and scope of the API calls on behalf of the service account

    '''
    USE THE GOOGLE DRIVE API TO DO THE REQUESTS
        developers.google.com/drive/api/reference/rest/v3

    '''
    argparser = argparse.ArgumentParser(
                            prog="5RC_Stints_GDrive",
                            description="A tool that converts .ibt files to .csv files on a lap by lap basis"
                            )

    argparser.add_argument('-c', '--config_from')
    argparser.add_argument('-d', '--rootdir')
    argparser.parse_args()

    try:
        ROOT = Path(argparser.rootdir)

    except AttributeError:
        ROOT = Path(__file__).parent.parent

    try:
        CONFIG_PATH = argparser.config_from
    except AttributeError:
        CONFIG_PATH = ROOT/"config/config.ini"

    config = Config.instance()
    config.parse_config(CONFIG_PATH)
    setattr(Config.instance().PATHS, "root", ROOT)

    # Define filepaths from config
    TELEM_CSV_FOLDER = ROOT/Config.instance().PATHS.csv_folder
    TELEM_IBT_FOLDER = ROOT/Config.instance().PATHS.ibt_folder
    IBT_READER_PATH  = ROOT/Config.instance().GENERAL.ibt_reader_path
    IBT_IGNORE = ROOT/Config.instance().PATHS.ibt_ignore 

    # Create folders if they do not exist 
    TELEM_CSV_FOLDER.mkdir(parents=True, exist_ok=True)
    TELEM_IBT_FOLDER.mkdir(parents=True, exist_ok=True)


    telem = TelemDataHandler(TELEM_IBT_FOLDER, 
                             TELEM_CSV_FOLDER, 
                             ibt_reader_path=IBT_READER_PATH, 
                             ibt_ignores_txt=IBT_IGNORE)

    # Get the list of drivers' telemetry to download
    DRIVERS = list(map(lambda x: Driver(x, telem=telem), Config.instance().DRIVERS.keys))
    
    # TODO: Perform the check based on driver's names
    '''
    ibt_files = telem.download_drivers_ibt(DRIVERS)
    csv_to_upload = telem.process_telemetry(ibt_files)
    '''

    ibt_files = telem.download_drivers_ibt(DRIVERS)
    csv_to_upload = telem.process_telemetry(ibt_files)
    uploaded_files = telem.upload_csv_files(csv_to_upload)
    telem.cleanup(DRIVERS)
    print(uploaded_files)
    


if __name__ == "__main__":
    main()


