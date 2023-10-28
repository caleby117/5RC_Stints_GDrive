from pathlib import Path


ROOT = Path(__file__).parent.parent
CREDS = ROOT / "creds" / "google_creds.json"
IBT_READER_PATH = ROOT/"telem"/"util"
TELEM_DL_FOLDER =  ROOT / "telem" / "ibt"
TELEM_CSV_FOLDER = TELEM_DL_FOLDER.parent / "csv"
