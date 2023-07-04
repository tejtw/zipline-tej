# Installation

## Used packages and environment
* Main package: Zipline-reloaded 2.2.0
* Python 3.8 or Python 3.9
* Microsoft Windows OS
* Other Python dependency packages: Pandas, Numpy, Logbook, Exchange-calendars

## How to install Zipline Reloaded modified by TEJ
* We're going to illustrate under anaconda environment, so we suggest using [Anaconda](https://www.anaconda.com/data-science-platform) as development environment.

* Download dependency packages. [(zipline-tej.yml)](https://minhaskamal.github.io/DownGit/#/home?url=https://github.com/tejtw/zipline-tej/blob/main/zipline-tej.yml)

* Start an Anaconda (base) prompt, create an virtual environment and install the appropriate versions of packages:
(We **strongly** recommand using virtual environment to keep every project independent.) [(reason)](https://csguide.cs.princeton.edu/software/virtualenv#definition)

```
# change directionary to the folder exists zipline-tej.yml
$ cd <C:\Users\username\Downloads>

# create virtual env
$ conda env create -f zipline-tej.yml

# activate virtual env
$ conda activate zipline-tej

```

Also, if you are familiar with Python enough, you can create a virtual environment without zipline-tej.yml and here's the sample :

```
# create virtual env
$ conda create -n <env_name> python=3.8

# activate virtual env
$ conda activate <env_name>

# download dependency packages
$ conda install -c conda-forge -y ta-lib
$ conda install -c conda-forge nb_conda_kernels
$ conda install -y notebook=6.4.11
$ conda install -y xlrd=2.0.1
$ conda install -y openpyxl=3.0.9
$ pip install zipline-tej

```


* Notice that we need to download TA-lib at first, so that we can download zipline-tej successfully.


## Exchange Calendar Issues

We're now developing specfic on Taiwan securities backtesting strategy, so we're using the unique trading calendar created by ourselves.  [download](https://minhaskamal.github.io/DownGit/#/home?url=https://github.com/tejtw/zipline-tej/tree/main/exchange_calendars)

After downloaded the calendar file above, overwrite **calendar_utils.py** in exchange_calendars folder and add **exchange_calendar_tejxtai.py**.

\* Navigate to the exchange_calendars folder within site packages. This is typically located at C:\Users\username\Anaconda3\envs\zipline-tej\Lib\site-packages\exchange_calendars

But some users may located at C:\Users\username\\AppData\Roaming\Python\Python38\Scripts
which we aren't pleased to see. So if this happened, we suggest to put **exchange-calendars** folder to former path we mentioned above.


# Quick start

## CLI Interface

The following code implements a simple buy_and_hold trading algorithm.

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

# setting download ticker
$ set ticker=2330 2317

# setting backtest period
$ set mdate=20200101 20220101

```
* Ingest and run backtesting algorithm

```
$ zipline ingest -b tquant
$ zipline run -f buy_and_hold.py  --start 20200101 --end 20220101 -o bah.pickle --no-benchmark --trading-calendar TEJ_XTAI
```
Then, the resulting performance DataFrame is saved as bah.pickle, which you can load and analyze from Python.

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


## Common errors 

* NotSessionError : The date of algorithm start date or end date is not available in trading algorithm.
    * Solution : Adjust start date or end date to align trading calendar.
* DateOutOfBounds : The trading calendar would update every day, but it would be fixed on the **FIRST TIME** executed date in Jupyter Notebook.
    * Solution : Restart Jupyter Notebook kernel.

# More Zipline Tutorials

* For more [tutorials]()

# Suggestions
* To get TEJAPI_KEY [(link)](https://api.tej.com.tw/trial.html)
* [TEJ Official Website](https://www.tej.com.tw/)
