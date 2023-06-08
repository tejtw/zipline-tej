#
# Copyright 2013 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#20230210 (by MRC) for treasury return column in perf  

import logbook

import pandas as pd

log = logbook.Logger(__name__)


def get_treasury_returns_from_file(filelike):
    """
    Get a Series of treasury returns from a file

    Parameters
    ----------
    filelike : str or file-like object
        Path to the treasury file.
        expected csv file format:
        date,rate
        2020-01-02 00:00:00+00:00,0.01
        2020-01-03 00:00:00+00:00,-0.02

    """
    log.info("Reading treasury returns from {}", filelike)

    df = pd.read_csv(
        filelike,
        index_col=["date"],
        parse_dates=["date"],
    )
    if not df.index.tz:
        df = df.tz_localize("utc")

    if "rate" not in df.columns:
        raise ValueError(
            "The column 'rate' not found in the "
            "benchmark file \n"
            "Expected treasury file format :\n"
            "date, rate\n"
            "2020-01-02 00:00:00+00:00,0.01\n"
            "2020-01-03 00:00:00+00:00,-0.02\n"
        )

    return df["rate"].sort_index()
