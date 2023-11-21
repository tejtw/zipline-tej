# Installation

## Used packages and environment
* Main package: Zipline
* Python 3.8 or above (currently support up to 3.11)
* Microsoft Windows OS or macOS or Linux
* Other Python dependency packages: Pandas, Numpy, Logbook, Exchange-calendars, etc.

## How to install Zipline Reloaded modified by TEJ

* We're going to illustrate under anaconda environment, so we suggest using [Anaconda](https://www.anaconda.com/data-science-platform) as development environment.

* Download dependency packages.

1. Windows [(zipline-tej.yml)](https://minhaskamal.github.io/DownGit/#/home?url=https://github.com/tejtw/zipline-tej/blob/main/zipline-tej.yml)

2. Mac [(zipline-tej_mac.yml)](https://minhaskamal.github.io/DownGit/#/home?url=https://github.com/tejtw/zipline-tej/blob/main/zipline-tej_mac.yml)

* Start an Anaconda (base) prompt, create an virtual environment and install the appropriate versions of packages:
(We **strongly** recommand using virtual environment to keep every project independent.) [(reason)](https://csguide.cs.princeton.edu/software/virtualenv#definition)

```
Windows Users
# change directionary to the folder exists zipline-tej.yml
$ cd <C:\Users\username\Downloads>

# create virtual env
$ conda env create -f zipline-tej.yml

# activate virtual env
$ conda activate zipline-tej
Mac Users
# change directionary to the folder exists zipline-tej_mac.yml
$ cd <C:\Users\username\Downloads>

# create virtual env
$ conda env create -f zipline-tej_mac.yml

# activate virtual env
$ conda activate zipline-tej

```

Also, if you are familiar with Python enough, you can create a virtual environment without zipline-tej.yml and here's the sample :

```
# create virtual env
$ conda create -n <env_name> python=3.10

# activate virtual env
$ conda activate <env_name>

# download dependency packages
$ pip install zipline-tej

```

While encountering environment problems, we provided a consistent and stable environment on [Docker hub](https://hub.docker.com/).

For users that using docker, we briefly introduce how to download and use it.

First of all, please download and install [docker-desktop](https://www.docker.com/products/docker-desktop/).

```

1. Start docker-desktop. (Registration is not must.)

2. Select the "images" on the leftside and search "tej87681088/tquant" and click "Pull".

3. After the image was downloaded, click the "run" icon the enter the optional settings.

3-1. Contaner-name: whatever you want.

3-2. Ports: the port to connect, "8888" is recommended.

3-3. Volumes: the place to store files. (You can create volume first on the left side.)

e.g. created a volume named "data", host path enter "data", container path "/app" is recommended.

4. Select the "Containers" leftside, the click the one which its image name is tej87681088/tquant

5. In its "Logs" would show an url like 
http://127.0.0.1:8888/tree?token=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

6. Go to your browser and enter "http://127.0.0.1:<port_you_set_in_step_3-2>/tree?token=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

6-1. If your port is 8888, you can just click the hyperlink.

7. Start develop your strategy!

NOTICE: Next time, we just need to reproduce step4 to step6.
```

# Quick start

## CLI Interface

The following code implements a simple buy-and-hold trading algorithm.

```python
from zipline.api import order, record, symbol

def initialize(context):
    context.asset = symbol("2330")
    
def handle_data(context, data):
    order(context.asset, 10)
    record(TSMC=data.current(context.asset, "price"))
    
def analyze(context=None, results=None):
    import matplotlib.pyplot as plt

    # Plot the portfolio and asset data.
    ax1 = plt.subplot(211)
    results.portfolio_value.plot(ax=ax1)
    ax1.set_ylabel("Portfolio value (TWD)")
    ax2 = plt.subplot(212, sharex=ax1)
    results.TSMC.plot(ax=ax2)
    ax2.set_ylabel("TSMC price (TWD)")

    # Show the plot.
    plt.gcf().set_size_inches(18, 8)
    plt.show()

```
You can then run this algorithm using the Zipline CLI. But first, you need to download some market data with historical prices and trading volumes:
* Before ingesting data, you have to set some environment variables as follow:
 
```
# setting TEJAPI_KEY to get permissions loading data
$ set TEJAPI_KEY=<your_key>
$ set TEJAPI_BASE=https://api.tej.com.tw

# setting download ticker
$ set ticker=2330 2317

# setting backtest period
$ set mdate=20200101 20220101

```
* Ingest and run backtesting algorithm

```
$ zipline ingest -b tquant
$ zipline run -f buy_and_hold.py  --start 20200101 --end 20220101 -o bah.pickle --no-benchmark --no-treasury 
```
Then, the resulting performance DataFrame is saved as bah.pickle, which you can load and analyze from Python.

### More useful zipline commands

Before calling **zipline** in CLI, be sure that TEJAPI_KEY and TEJAPI_BASE were set.
Use **zipline --help** to get more information.

For example :
We want to know how to use **zipline run**, we can run as follow:

```
zipline run --help
```

```

Usage: zipline run [OPTIONS]
  Run a backtest for the given algorithm.

Options:
  -f, --algofile FILENAME         The file that contains the algorithm to run.
  -t, --algotext TEXT             The algorithm script to run.
  -D, --define TEXT               Define a name to be bound in the namespace
                                  before executing the algotext. For example
                                  '-Dname=value'. The value may be any python
                                  expression. These are evaluated in order so
                                  they may refer to previously defined names.
  --data-frequency [daily|minute]
                                  The data frequency of the simulation.
                                  [default: daily]
  --capital-base FLOAT            The starting capital for the simulation.
                                  [default: 10000000.0]
  -b, --bundle BUNDLE-NAME        The data bundle to use for the simulation.
                                  [default: tquant]
  --bundle-timestamp TIMESTAMP    The date to lookup data on or before.
                                  [default: <current-time>]
  -bf, --benchmark-file FILE      The csv file that contains the benchmark
                                  returns
  --benchmark-symbol TEXT         The symbol of the instrument to be used as a
                                  benchmark (should exist in the ingested
                                  bundle)
  --benchmark-sid INTEGER         The sid of the instrument to be used as a
                                  benchmark (should exist in the ingested
                                  bundle)
  --no-benchmark                  If passed, use a benchmark of zero returns.
  -bf, --treasury-file FILE       The csv file that contains the treasury
                                  returns
  --treasury-symbol TEXT          The symbol of the instrument to be used as a
                                  treasury (should exist in the ingested
                                  bundle)
  --treasury-sid INTEGER          The sid of the instrument to be used as a
                                  treasury (should exist in the ingested
                                  bundle)
  --no-treasury                   If passed, use a treasury of zero returns.
  -s, --start DATE                The start date of the simulation.
  -e, --end DATE                  The end date of the simulation.
  -o, --output FILENAME           The location to write the perf data. If this
                                  is '-' the perf will be written to stdout.
                                  [default: -]
  --trading-calendar TRADING-CALENDAR
                                  The calendar you want to use e.g. TEJ_XTAI.
                                  TEJ_XTAI is the default.
  --print-algo / --no-print-algo  Print the algorithm to stdout.
  --metrics-set TEXT              The metrics set to use. New metrics sets may
                                  be registered in your extension.py.
  --blotter TEXT                  The blotter to use.  [default: default]
  --help                          Show this message and exit.

``` 
#### New add tickers

```
$ zipline add -t "<ticker_wants_to_add>"
```
If tickers are more than 1 ticker, split them apart by " " or ","  or ";".

For more detail use **zipline add --help** .

#### Display bundle-info
```
$ zipline bundle-info
```
To show what the tickers are there in newest bundle.

For more detail use **zipline bundle-info --help** .

#### Switch bundle

Before using switch, use **zipline bundles** to get the timestamp of each folder.

```
$ zipline switch -t "<The_timestamp_of_the_folder_want_to_use>"
```

Due to zipline only using the newest foler, switch can make previous folder become newest.

For more detail use **zipline switch --help** .

#### Update bundle

```
$ zipline update
```

To update the bundle information to newest date.

For more detail use **zipline update --help** .

## Jupyter Notebook 

### Change Anaconda kernel

* Since we've downloaded package "nb_conda_kernels", we should be able to change kernel in jupyter notebook.

#### How to new a notebook using specific kernel 

(1) Open anaconda prompt

(2) Enter the command as follow :
```
# First one can be ignore if already in environment of zipline-tej
$ conda activate zipline-tej 
# start a jupyter notebook
$ jupyter notebook 
```
(3) Start a notebook and select Python[conda env:zipline-tej]

(4)(Optional) If you have already written a notebook, you can open it and  change kernel by clicking the "Kernel" in menu and "Change kernel" to select the specfic kernel.


### Set environment variables TEJAPI_KEY, ticker and mdate

\* ticker would be your target ticker symbol, and it should be a string. If there're more than one ticker needed, use " ", "," or ";" to split them apart. 

\* mdate refers the begin date and end date, use " ", "," or ";" to split them apart.

```python
In[1]:
import os    
os.environ['TEJAPI_KEY'] = <your_key>    
os.environ['ticker'] ='2330 2317'     
os.environ['mdate'] ='20200101 20220101'  
```
### Call ingest to download data to ~\\\.zipline

```python
In[2]:    
!zipline ingest -b tquant
[Out]: 
Merging daily equity files:
[YYYY-MM-DD HH:mm:ss.ssssss] INFO: zipline.data.bundles.core: Ingesting tquant.
```


### Design the backtesting strategy

```python
In[3]:
from zipline.api import order, record, symbol

def initialize(context):
    context.asset = symbol("2330")
    
def handle_data(context, data):
    order(context.asset, 10)
    record(TSMC=data.current(context.asset, "price"))
    
def analyze(context=None, results=None):
    import matplotlib.pyplot as plt

    # Plot the portfolio and asset data.
    ax1 = plt.subplot(211)
    results.portfolio_value.plot(ax=ax1)
    ax1.set_ylabel("Portfolio value (TWD)")
    ax2 = plt.subplot(212, sharex=ax1)
    results.TSMC.plot(ax=ax2)
    ax2.set_ylabel("TSMC price (TWD)")

    # Show the plot.
    plt.gcf().set_size_inches(18, 8)
    plt.show()
```
### Run backtesting algorithm and plot

```python
In[4]:
from zipline import run_algorithm
import pandas as pd
from zipline.utils.calendar_utils import get_calendar
trading_calendar = get_calendar('TEJ_XTAI')

start = pd.Timestamp('20200103', tz ='utc' )
end = pd.Timestamp('20211230', tz='utc')

result = run_algorithm(start=start,
                  end=end,
                  initialize=initialize,
                  capital_base=1000000,
                  handle_data=handle_data,
                  bundle='tquant',
                  trading_calendar=trading_calendar,
                  analyze=analyze,
                  data_frequency='daily'
                  )
[Out]:
```
![output](https://github.com/tejtw/zipline-tej/blob/main/output_img/output.png?raw=true)


### Show trading process
```python
In[5]: 
result
[Out]:
```
<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>period_open</th>
      <th>period_close</th>
      <th>starting_value</th>
      <th>ending_value</th>
      <th>starting_cash</th>
      <th>ending_cash</th>
      <th>portfolio_value</th>
      <th>longs_count</th>
      <th>shorts_count</th>
      <th>long_value</th>
      <th>...</th>
      <th>treasury_period_return</th>
      <th>trading_days</th>
      <th>period_label</th>
      <th>algo_volatility</th>
      <th>benchmark_period_return</th>
      <th>benchmark_volatility</th>
      <th>algorithm_period_return</th>
      <th>alpha</th>
      <th>beta</th>
      <th>sharpe</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2020-01-03 05:30:00+00:00</th>
      <td>2020-01-03 01:01:00+00:00</td>
      <td>2020-01-03 05:30:00+00:00</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.000000e+06</td>
      <td>1.000000e+06</td>
      <td>1.000000e+06</td>
      <td>0</td>
      <td>0</td>
      <td>0.0</td>
      <td>...</td>
      <td>0.0</td>
      <td>1</td>
      <td>2020-01</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>0.000000</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2020-01-06 05:30:00+00:00</th>
      <td>2020-01-06 01:01:00+00:00</td>
      <td>2020-01-06 05:30:00+00:00</td>
      <td>0.0</td>
      <td>3320.0</td>
      <td>1.000000e+06</td>
      <td>9.966783e+05</td>
      <td>9.999983e+05</td>
      <td>1</td>
      <td>0</td>
      <td>3320.0</td>
      <td>...</td>
      <td>0.0</td>
      <td>2</td>
      <td>2020-01</td>
      <td>0.000019</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>-0.000002</td>
      <td>None</td>
      <td>None</td>
      <td>-11.224972</td>
    </tr>
    <tr>
      <th>2020-01-07 05:30:00+00:00</th>
      <td>2020-01-07 01:01:00+00:00</td>
      <td>2020-01-07 05:30:00+00:00</td>
      <td>3320.0</td>
      <td>6590.0</td>
      <td>9.966783e+05</td>
      <td>9.933817e+05</td>
      <td>9.999717e+05</td>
      <td>1</td>
      <td>0</td>
      <td>6590.0</td>
      <td>...</td>
      <td>0.0</td>
      <td>3</td>
      <td>2020-01</td>
      <td>0.000237</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>-0.000028</td>
      <td>None</td>
      <td>None</td>
      <td>-10.038514</td>
    </tr>
    <tr>
      <th>2020-01-08 05:30:00+00:00</th>
      <td>2020-01-08 01:01:00+00:00</td>
      <td>2020-01-08 05:30:00+00:00</td>
      <td>6590.0</td>
      <td>9885.0</td>
      <td>9.933817e+05</td>
      <td>9.900850e+05</td>
      <td>9.999700e+05</td>
      <td>1</td>
      <td>0</td>
      <td>9885.0</td>
      <td>...</td>
      <td>0.0</td>
      <td>4</td>
      <td>2020-01</td>
      <td>0.000203</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>-0.000030</td>
      <td>None</td>
      <td>None</td>
      <td>-9.298128</td>
    </tr>
    <tr>
      <th>2020-01-09 05:30:00+00:00</th>
      <td>2020-01-09 01:01:00+00:00</td>
      <td>2020-01-09 05:30:00+00:00</td>
      <td>9885.0</td>
      <td>13500.0</td>
      <td>9.900850e+05</td>
      <td>9.867083e+05</td>
      <td>1.000208e+06</td>
      <td>1</td>
      <td>0</td>
      <td>13500.0</td>
      <td>...</td>
      <td>0.0</td>
      <td>5</td>
      <td>2020-01</td>
      <td>0.001754</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.000208</td>
      <td>None</td>
      <td>None</td>
      <td>5.986418</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2021-12-24 05:30:00+00:00</th>
      <td>2021-12-24 01:01:00+00:00</td>
      <td>2021-12-24 05:30:00+00:00</td>
      <td>2920920.0</td>
      <td>2917320.0</td>
      <td>-1.308854e+06</td>
      <td>-1.314897e+06</td>
      <td>1.602423e+06</td>
      <td>1</td>
      <td>0</td>
      <td>2917320.0</td>
      <td>...</td>
      <td>0.0</td>
      <td>484</td>
      <td>2021-12</td>
      <td>0.232791</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.602423</td>
      <td>None</td>
      <td>None</td>
      <td>1.170743</td>
    </tr>
    <tr>
      <th>2021-12-27 05:30:00+00:00</th>
      <td>2021-12-27 01:01:00+00:00</td>
      <td>2021-12-27 05:30:00+00:00</td>
      <td>2917320.0</td>
      <td>2933040.0</td>
      <td>-1.314897e+06</td>
      <td>-1.320960e+06</td>
      <td>1.612080e+06</td>
      <td>1</td>
      <td>0</td>
      <td>2933040.0</td>
      <td>...</td>
      <td>0.0</td>
      <td>485</td>
      <td>2021-12</td>
      <td>0.232577</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.612080</td>
      <td>None</td>
      <td>None</td>
      <td>1.182864</td>
    </tr>
    <tr>
      <th>2021-12-28 05:30:00+00:00</th>
      <td>2021-12-28 01:01:00+00:00</td>
      <td>2021-12-28 05:30:00+00:00</td>
      <td>2933040.0</td>
      <td>2982750.0</td>
      <td>-1.320960e+06</td>
      <td>-1.327113e+06</td>
      <td>1.655637e+06</td>
      <td>1</td>
      <td>0</td>
      <td>2982750.0</td>
      <td>...</td>
      <td>0.0</td>
      <td>486</td>
      <td>2021-12</td>
      <td>0.233086</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.655637</td>
      <td>None</td>
      <td>None</td>
      <td>1.237958</td>
    </tr>
    <tr>
      <th>2021-12-29 05:30:00+00:00</th>
      <td>2021-12-29 01:01:00+00:00</td>
      <td>2021-12-29 05:30:00+00:00</td>
      <td>2982750.0</td>
      <td>2993760.0</td>
      <td>-1.327113e+06</td>
      <td>-1.333276e+06</td>
      <td>1.660484e+06</td>
      <td>1</td>
      <td>0</td>
      <td>2993760.0</td>
      <td>...</td>
      <td>0.0</td>
      <td>487</td>
      <td>2021-12</td>
      <td>0.232850</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.660484</td>
      <td>None</td>
      <td>None</td>
      <td>1.243176</td>
    </tr>
    <tr>
      <th>2021-12-30 05:30:00+00:00</th>
      <td>2021-12-30 01:01:00+00:00</td>
      <td>2021-12-30 05:30:00+00:00</td>
      <td>2993760.0</td>
      <td>2995050.0</td>
      <td>-1.333276e+06</td>
      <td>-1.339430e+06</td>
      <td>1.655620e+06</td>
      <td>1</td>
      <td>0</td>
      <td>2995050.0</td>
      <td>...</td>
      <td>0.0</td>
      <td>488</td>
      <td>2021-12</td>
      <td>0.232629</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.655620</td>
      <td>None</td>
      <td>None</td>
      <td>1.235305</td>
    </tr>
  </tbody>
</table>
<p>488 rows × 38 columns</p>
</div>

---

# More Zipline Tutorials

* For more [tutorials](https://github.com/tejtw/TQuant-Lab)

# Suggestions
* To get TEJAPI_KEY [(link)](https://api.tej.com.tw/trial.html)
* [TEJ Official Website](https://www.tej.com.tw/)
