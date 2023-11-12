from gdrive import DriveApiHandler, _api_call
from telem import TelemDataHandler
from driver import Driver
from pathlib import Path
from config import Config
import argparse
from sys import argv
from pprint import PrettyPrinter


def main():
    '''
    main.py:
        - Searches the google drive for .ibt files to download
        - Downloads the ibt files into some temp storage and runs the 
            5RC_Stint_Time_Util on it to produce a csv file
        - Uploads the csv file to the drive
        - Removes the temp storage


    '''
    argparser = argparse.ArgumentParser(
                            prog="5RC_Stints_GDrive",
                            description="A tool that converts .ibt files to"
                                ".csv files on a lap by lap basis"
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
    try:
        cache_file = Config.instance().PATHS.fs_cache
        Config.instance().PATHS.fs_cache = ROOT/cache_file
    except AttributeError:
        pass

    # Create folders if they do not exist 
    TELEM_CSV_FOLDER.mkdir(parents=True, exist_ok=True)
    TELEM_IBT_FOLDER.mkdir(parents=True, exist_ok=True)


    telem = TelemDataHandler(TELEM_IBT_FOLDER, 
                             TELEM_CSV_FOLDER, 
                             ibt_reader_path=IBT_READER_PATH, 
                             ibt_ignores_txt=IBT_IGNORE)

    # Create Drivers object to track ownership of files and send them to 
    # the right paths
    DRIVERS = list(map(lambda x: Driver(x, telem=telem), 
                       Config.instance().DRIVERS.keys)
                   )
    get_and_process_all(telem, DRIVERS)


def get_and_process_all(telem, drivers):
    
    # Download the drivers' ibt files
    ibt_files = telem.download_drivers_ibt(drivers)

    # Process files with the ibt telem util
    csv_to_upload = telem.process_telemetry(ibt_files)

    # Upload the files to the google drive
    uploaded_files = telem.upload_csv_files(csv_to_upload)
    
    # Clean up all the files
    telem.cleanup(drivers)
    print(uploaded_files)
    


if __name__ == "__main__":
    main()


