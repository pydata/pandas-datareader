import os
import time

import pandas as pd

from pandas_datareader.base import _BaseReader, string_types
from pandas_datareader.compat import StringIO
from pandas_datareader.exceptions import DEP_ERROR_MSG, ImmediateDeprecationError


class EnigmaReader(_BaseReader):
    """
    Collects current snapshot of Enigma data located at the specified
    data set ID and returns a pandas DataFrame.

    Parameters
    ----------
    dataset_id : str
        Enigma dataset UUID.
    api_key : str, optional
        Enigma API key. If not provided, the environmental variable
        ENIGMA_API_KEY is read.
    retry_count : int, default 5
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used.
    base_url : str, optional (defaults to https://public.enigma.com/api)
        Alternative Enigma endpoint to be used.

    Examples
    --------
    Download current snapshot for the following Florida Inspections Dataset:
    https://public.enigma.com/datasets/bedaf052-5fcd-4758-8d27-048ce8746c6a

    >>> import pandas_datareader as pdr
    >>> df = pdr.get_data_enigma('bedaf052-5fcd-4758-8d27-048ce8746c6a')

    In the event that ENIGMA_API_KEY does not exist in your env, the key can
    be supplied as the second argument or as the keyword argument `api_key`

    >>> df = EnigmaReader(dataset_id='bedaf052-5fcd-4758-8d27-048ce8746c6a',
    ...                   api_key='INSERT_API_KEY').read()
    """

    def __init__(
        self,
        dataset_id=None,
        api_key=None,
        retry_count=5,
        pause=0.75,
        session=None,
        base_url="https://public.enigma.com/api",
    ):
        raise ImmediateDeprecationError(DEP_ERROR_MSG.format("Enigma"))

        super(EnigmaReader, self).__init__(
            symbols=[], retry_count=retry_count, pause=pause, session=session
        )
        if api_key is None:
            self._api_key = os.getenv("ENIGMA_API_KEY")
            if self._api_key is None:
                raise ValueError(
                    "Please provide an Enigma API key or set "
                    "the ENIGMA_API_KEY environment variable\n"
                    "If you do not have an API key, you can get "
                    "one here: http://public.enigma.com/signup"
                )
        else:
            self._api_key = api_key

        self._dataset_id = dataset_id
        if not isinstance(self._dataset_id, string_types):
            raise ValueError(
                "The Enigma dataset_id must be a string (ex: "
                "'bedaf052-5fcd-4758-8d27-048ce8746c6a')"
            )

        headers = {
            "Authorization": "Bearer {0}".format(self._api_key),
            "User-Agent": "pandas-datareader",
        }
        self.session.headers.update(headers)
        self._base_url = base_url
        self._retry_count = retry_count
        self._retry_delay = pause

    def read(self):
        """Read data"""
        try:
            return self._read()
        finally:
            self.close()

    def _read(self):
        snapshot_id = self.get_current_snapshot_id(self._dataset_id)
        exported_data = self.get_snapshot_export(snapshot_id)  # TODO: Retry?
        decoded_data = exported_data.decode("utf-8")
        return pd.read_csv(StringIO(decoded_data))

    def _get(self, url):
        """HTTP GET Request with Retry Logic"""
        url = "{0}/{1}".format(self._base_url, url)
        attempts = 0
        while True:
            try:
                response = self.session.get(url)
                response.raise_for_status()
                return response
            except Exception as e:
                if attempts < self._retry_count:
                    attempts += 1
                    time.sleep(self._retry_delay)
                    continue
                else:
                    raise e

    def get_current_snapshot_id(self, dataset_id):
        """Get ID of the most current snapshot of a dataset"""
        dataset_metadata = self.get_dataset_metadata(dataset_id)
        return dataset_metadata["current_snapshot"]["id"]

    def get_dataset_metadata(self, dataset_id):
        """Get the Dataset Model of this EnigmaReader's dataset
        https://docs.public.enigma.com/resources/dataset/index.html
        """
        url = "datasets/{0}?row_limit=0".format(dataset_id)
        response = self._get(url)
        return response.json()

    def get_snapshot_export(self, snapshot_id):
        """Return raw CSV of a dataset"""
        url = "export/{0}".format(snapshot_id)
        response = self._get(url)
        return response.content
