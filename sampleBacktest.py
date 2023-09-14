import asyncio
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import stocks_list
import numpy as np
import pandas as pd

stocks = stocks_list.stocks

def closest_number(close, high, low):
    dist1 = abs(close - high)
    dist2 = abs(close - low)
    if dist1 < dist2:
        return True
    else:
        return False

def getCurrentPrice(stock):
    try:
        url = f'https://www.marketwatch.com/investing/stock/{stock}?mod=search_symbol'
        res = requests.get(url)
        if res.status_code != 200:
            print(f"Error on current price URL: {res.status_code}")
        soup = BeautifulSoup(res.text, 'lxml')
        cp = soup.findAll('li', class_="kv__item")[0]
        cp = cp.text[7:].strip()
    except:
        cp = 1
    return float(cp)

def get_market_cap(stock, open):
    try:
        url = f'https://www.marketwatch.com/investing/stock/{stock}?mod=search_symbol'
        res = requests.get(url)
        if res.status_code != 200:
            print("Error on market cap URL: " + str(res.status_code))
        soup = BeautifulSoup(res.text, 'lxml')
        mc = soup.findAll('li', class_="kv__item")[3]
        mc = mc.text[13:].strip()
        dif = (getCurrentPrice(stock) - open / open)
        if 'B' in mc:
            mc = float(mc[:-1]) / (1 + dif)
            mc = format(round(mc * 1000000000), ",")
        elif 'M' in mc:
            mc = float(mc[:-1]) / (1 + dif)
            mc = format(round(mc * 1000000), ",")
    except:
        mc = "No Data"
    return mc

def calculate_volatility(closing_prices):
    log_returns = [np.log(closing_prices[i] / closing_prices[i - 1]) for i in range(1, len(closing_prices))]
    avg_return = np.mean(log_returns)
    squared_deviations = [(log_return - avg_return) ** 2 for log_return in log_returns]
    variance = np.mean(squared_deviations)
    historical_volatility = np.sqrt(variance)
    vol = round(historical_volatility * np.sqrt(252) * 100, 2)
    return vol

def get_return(stock_data):
    if stock_data['Low'][249]*1.02 < stock_data['Open'][249]:
        change = (stock_data['Open'][249] * .98 - stock_data['Open'][249]) / stock_data['Open'][249] * 100
    else:
        change = (stock_data['Close'][249] - stock_data['Open'][249]) / stock_data['Open'][249] * 100
    return round(change, 2)


dates = []
tickers = []
market_caps = []
gaps = []
vol_ratios = []
volatilities = []
equity_vols = []
returns = []

async def get_stock_data(stock, i):
    try:
        ticker = yf.Ticker(stock)
        p = str(i) + 'd'
        stock_data = await asyncio.to_thread(ticker.history, period=p)
        stock_data = stock_data.reset_index()
        gap = round((stock_data['Open'][247] - stock_data['High'][246]) / stock_data['High'][246] * 100, 2)
        inside_day = stock_data['High'][247] >= stock_data['High'][248] and stock_data['Low'][247] <= stock_data['Low'][248]
        avg_vol = stock_data['Volume'][0:247].mean()
        equity_vol = stock_data['Close'][200:247].mean() * stock_data['Volume'][200:247].median()
        good_positioning = closest_number(stock_data['Close'][248], stock_data['High'][247], (stock_data['Low'][247]*.99))
        green_initial_day = stock_data['Close'][247] > stock_data['Open'][247]
        inside_day_green = stock_data['Close'][248] > stock_data['Open'][248]
        gap_up_following_day = stock_data['Close'][248] <= stock_data['Open'][249]
        vol_ratio = stock_data['Volume'][247] / avg_vol if avg_vol != 0 else "No data"
        good_inside_candle = closest_number(stock_data['Close'][248], stock_data['High'][248], stock_data['Low'][248])
        at_aths = stock_data['High'][247] >= max(stock_data['High'][0:247])
        if gap > 3 and vol_ratio > 3 and inside_day and green_initial_day and inside_day_green and equity_vol > 250000 and good_inside_candle and good_positioning and not at_aths:
            date = stock_data['Date'][249]
            mc = get_market_cap(stock, stock_data['Open'][246])
            vol = calculate_volatility(stock_data['Close'][0:247])
            change = get_return(stock_data)
            dates.append(date)
            tickers.append(stock)
            market_caps.append(mc)
            gaps.append(gap)
            vol_ratios.append(vol_ratio)
            volatilities.append(vol)
            equity_vols.append(equity_vol)
            returns.append(change)
            print(f"{date.strftime('%m/%d/%Y'):>5} {stock:>6} {mc:>16} {gap:>7}% {round(vol_ratio, 2):>9} {vol:>8}%"
                f" {format(round(equity_vol), ','):>13}{str(round(change, 2)):>11}%")
    except:
        pass

async def main():
    tasks = []
    print("   Date      Stock       MarketCap     Gap    VolRatio  Volatility    EquityVol   Return" )
    for i in range(500, 550):
        print(f'{i}--------------------------------------------------------------------------------------')
        for stock in stocks:
            tasks.append(asyncio.create_task(get_stock_data(stock, i)))
        await asyncio.gather(*tasks)
        tasks.clear()

asyncio.run(main())


df = pd.DataFrame()
df['Dates']= dates
df['Stock'] = tickers
df['Market Cap'] = market_caps
df['Gap'] = gaps 
df['Vol Ratio'] = vol_ratios
df['Volatility'] = volatilities
df['Equity Vol'] = equity_vols
df['Return'] = returns


df.to_csv('/Users/evanwright/Documents/Watchlist/data.csv')


