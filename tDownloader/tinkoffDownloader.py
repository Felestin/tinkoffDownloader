from __future__ import annotations

from datetime import timedelta

import pandas as pd
from tinkoff.invest import Client, CandleInterval


class TinkoffDownloader:
    def __init__(self, TOKEN=None, SANDBOX_TOKEN=None, ):
        self.TOKEN = TOKEN
        self.SANDBOX_TOKEN = SANDBOX_TOKEN

    def to_data_frame(self, _list) -> pd.DataFrame:
        df = pd.DataFrame(
            _list,
        )
        df.drop_duplicates(subset='time')
        df.columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "date",
            "is_complete",
        ]
        if not df.empty:
            df.set_index('date', inplace=True)
            df.index = pd.DatetimeIndex(df.index)

        return df

    def download_data(
            self, ticker_list, start_date, end_date, time_interval, delta=1
    ) -> pd.DataFrame:

        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)
        delta = timedelta(days=delta)
        data_df = pd.DataFrame()
        with Client(
                token=self.TOKEN, sandbox_token=self.SANDBOX_TOKEN) as client:
            for tic in ticker_list:
                while (
                        start_date <= end_date
                ):
                    candles = client.market_data.get_candles(
                        from_=pd.Timestamp(start_date),
                        to=pd.Timestamp(start_date + delta),
                        interval=CandleInterval(time_interval),
                        instrument_id=tic).candles

                    if not candles:
                        start_date += delta
                        continue
                    if len(candles) > 1:
                        candles.pop()

                    temp_df = self.to_data_frame(candles)
                    temp_df["tic"] = tic
                    data_df = pd.concat([data_df, temp_df])
                    start_date += delta
            data_df = data_df.reset_index()

            data_df = data_df.drop(columns=["is_complete"])
            # data_df["day"] = data_df["date"].dt.dayofweek
            data_df["date"] = data_df.date.apply(lambda x: x.strftime("%Y-%m-%d"))
            for data_type in ['open', 'high', 'low', 'close']:
                data_df[data_type] = data_df[data_type].apply(lambda x: x['units'] + (int(str(x['nano'])[:1]) * 0.1))

            return data_df
