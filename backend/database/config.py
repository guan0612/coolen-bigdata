class Config:
    # lab mysql config
    # LAB_DB_HOST = 'coolen-mysql'
    LAB_DB_HOST = '172.18.0.2'
    LAB_DB_PORT = 3306
    LAB_DB_NAME = "coolen_big_data"
    LAB_DB_USERNAME = 'root'
    LAB_DB_PASSWORD = 'ru4cj84coolen'

    # ncu-machine5 mysql config
    NCU_DB_HOST = '20.89.171.94'
    NCU_DB_PORT = 13306
    NCU_DB_NAME = "coolen_big_data"
    NCU_DB_USERNAME = 'root'
    NCU_DB_PASSWORD = 'ru4cj84coolen'

    # big data folder
    # PROGRESS_FILE_PATH = "C:\\Users\\jessi\\OneDrive\\School\\job\\coolen\\大數據樣本資料\\progressing\\*\\"
    # COMPLETED_FILE_PATH = "C:\\Users\\jessi\\OneDrive\\School\\job\\coolen\\大數據樣本資料\\completed\\"
    PROGRESS_FILE_PATH = '/app/data/progressing/*/'
    COMPLETED_FILE_PATH = '/app/data/completed/'
    ERROR_FILE_PATH = '/app/data/error/'
