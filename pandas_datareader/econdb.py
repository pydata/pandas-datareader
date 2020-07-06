import pandas as pd

from pandas_datareader.base import _BaseReader


class EcondbReader(_BaseReader):
    """Get data for the given name from Econdb."""

    _URL = "https://www.econdb.com/api/series/"
    _format = None
    _show = "labels"

    @property
    def url(self):
        """API URL"""
        if not isinstance(self.symbols, str):
            raise ValueError("data name must be string")

        return "{0}?{1}&format=json&page_size=500&expand=both".format(
            self._URL, self.symbols
        )

    def read(self):
        """ read one data from specified URL """
        results = self.session.get(self.url).json()["results"]
        df = pd.DataFrame({"dates": []}).set_index("dates")

        if self._show == "labels":

            def show_func(x):
                return x[x.find(":") + 1 :]

        elif self._show == "codes":

            def show_func(x):
                return x[: x.find(":")]

        for entry in results:
            series = pd.DataFrame(entry["data"])[["dates", "values"]].set_index("dates")
            head = entry["additional_metadata"]

            if head != "":  # this additional metadata is not blank
                series.columns = pd.MultiIndex.from_tuples(
                    [[show_func(x) for x in head.values()]],
                    names=[show_func(x) for x in head.keys()],
                )
            else:
                series.rename(columns={"values": entry["ticker"]}, inplace=True)

            if not df.empty:
                df = df.merge(series, how="outer", left_index=True, right_index=True)
            else:
                df = series
        if df.shape[0] > 0:
            df.index = pd.to_datetime(df.index, errors="ignore")
        df.index.name = "TIME_PERIOD"
        df = df.truncate(self.start, self.end)
        return df
