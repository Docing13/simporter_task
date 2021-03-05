import inspect
import os

from constants.data_constants import DATA_NAME


def get_data_path() -> str:
    # trick to get path for data file
    path = os.path.dirname(
        os.path.abspath(inspect.getfile(inspect.currentframe())))

    path += "/" + DATA_NAME
    return path
