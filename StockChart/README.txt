StockChart
------------
A python app to graph data pulled from yahoo finance. Sends notifications based on technical analysis.


Technical Analysis
------------
Simple Moving Average (SMA): 
Last (x)days closing prices / (x)days

Exponential Moving Average (EMA):
There are three steps to calculating an exponential moving average (EMA). First, calculate the simple moving average for the initial EMA value. 
An exponential moving average (EMA) has to start somewhere, so a simple moving average is used as the previous period's EMA in the first calculation. 
Second, calculate the weighting multiplier. Third, calculate the exponential moving average for each day between the initial EMA value and today, 
using the price, the multiplier, and the previous period's EMA value. The formula below is for a 10-day EMA.

Initial SMA: 10-period sum / 10 
Multiplier: (2 / (Time periods + 1) ) = (2 / (10 + 1) ) = 0.1818 (18.18%)
EMA: {Close - EMA(previous day)} x multiplier + EMA(previous day). 

Bolinger Bands:
Shows exit and entry points.


Notifications
------------
Program is ran on windows scheduler and called every 30 mins. Sends graph
to email with description of status for particular stock.