import os


def _getenv_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


class Config:
    """
    所有設定改為環境變數優先，保留原本的預設值以維持相容性。

    - LAB_DB_HOST / LAB_DB_PORT / LAB_DB_NAME / LAB_DB_USERNAME / LAB_DB_PASSWORD
    - NCU_DB_HOST / NCU_DB_PORT / NCU_DB_NAME / NCU_DB_USERNAME / NCU_DB_PASSWORD
    - PROGRESS_FILE_PATH / COMPLETED_FILE_PATH / ERROR_FILE_PATH（匯入流程用；統計流程不依賴）
    """

    # lab mysql config
    LAB_DB_HOST = os.getenv("LAB_DB_HOST", "140.115.54.44")
    LAB_DB_PORT = _getenv_int("LAB_DB_PORT", 13306)
    LAB_DB_NAME = os.getenv("LAB_DB_NAME", "coolen_big_data")
    LAB_DB_USERNAME = os.getenv("LAB_DB_USERNAME", "root")
    LAB_DB_PASSWORD = os.getenv("LAB_DB_PASSWORD", "ru4cj84coolen")

    # ncu mysql config
    NCU_DB_HOST = os.getenv("NCU_DB_HOST", "20.89.171.94")
    NCU_DB_PORT = _getenv_int("NCU_DB_PORT", 13306)
    NCU_DB_NAME = os.getenv("NCU_DB_NAME", "coolen_big_data")
    NCU_DB_USERNAME = os.getenv("NCU_DB_USERNAME", "root")
    NCU_DB_PASSWORD = os.getenv("NCU_DB_PASSWORD", "ru4cj84coolen")

    # big data folder (匯入流程用)
    PROGRESS_FILE_PATH = os.getenv("PROGRESS_FILE_PATH", "/app/data/progressing/*/")
    COMPLETED_FILE_PATH = os.getenv("COMPLETED_FILE_PATH", "/app/data/completed/")
    ERROR_FILE_PATH = os.getenv("ERROR_FILE_PATH", "/app/data/error/")
