#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from binance.client import Client
from binance.exceptions import BinanceAPIException
from datetime import datetime, timedelta


class ImprovedClientClass(Client):

    ###############################################################################################################################
    def __init__(self, api_key, api_secret):
        super().__init__(api_key, api_secret)
    
    ####################################################################################################################################
    def get_custom_bars(self, symbol, interval, startdate, enddate, limit):
        
        # partie1 : récupération de tous les trades
        
        params = {'symbol': symbol,
                  'interval': interval,
                  'startdate': int(datetime.timestamp(datetime.strptime(startdate, '%m/%d/%y %H:%M:%S'))*1000),
                  'enddate': int(datetime.timestamp(datetime.strptime(enddate, '%m/%d/%y %H:%M:%S'))*1000),
                  'limit': limit}
        
        # découpe de la requête minute par minute pour ne pas dépasser la limite 'limit' de trades
        # que l'on peut récuperer en une fois
        
        delta = timedelta(minutes=1)
        ST = params['startdate']
        dateST = datetime.fromtimestamp(ST / 1000.0)
        dateEND = dateST + delta - timedelta(milliseconds=1)
        END = int(datetime.timestamp(dateEND) * 1000)
        
        alltrades = []
        
        
        while END < params['enddate'] :
            
            params2 = {'symbol': symbol,
                       'startTime': ST,
                       'endTime': END,
                       'limit': limit}
                
            try:
                trades = self.get_aggregate_trades(**params2)
                alltrades.extend(trades)
            except BinanceAPIException as e:
                print(f'Error retrieving aggregated trades: {e}')
                return None
            
            dateST = dateST + delta
            ST = int(datetime.timestamp(dateST) * 1000)
            dateEND = dateEND + delta
            END = int(datetime.timestamp(dateEND) * 1000)
        
        #print de vérification
        #print(alltrades)
        
        #-----------------------------------------------------------------------------------------------------------------
        
        # parti2 : aggrégation des trades en bars de la fréquence choisie par l'utilisateur
        
        # conversion de la fréquence en secondes et en type int
        interval_seconds = self.get_interval_seconds(interval)
    
        custom_bars = []
        current_bar = None
    
        for trade in alltrades:
            trade_timestamp = trade['T']
            trade_price = float(trade['p'])
            trade_quantity = float(trade['q'])
            
            # création d'un nouveau bar avec les données de son premier trade
            if current_bar is None:
                bar_timestamp = trade_timestamp - (trade_timestamp % (interval_seconds*1000))
                current_bar = {'timestamp': bar_timestamp, 'open': trade_price, 'high': trade_price, 'low': trade_price, 'close': trade_price, 'volume': trade_quantity}
            
            # ajout du bar à la liste des bars lorsqu'il est complet d'après les conditions temporelles
            # puis création d'un nouveau bar avec les données du trade en cours de traitement
            elif trade_timestamp >= current_bar['timestamp'] + (interval_seconds*1000):
                custom_bars.append(current_bar)
                
                bar_timestamp = trade_timestamp - (trade_timestamp % (interval_seconds*1000))
                current_bar = {'timestamp': bar_timestamp, 'open': trade_price, 'high': trade_price, 'low': trade_price, 'close': trade_price, 'volume': trade_quantity}
            
            # mise à jour du bar avec les données du trade en cours de traitement
            else:
                current_bar['high'] = max(current_bar['high'], trade_price)
                current_bar['low'] = min(current_bar['low'], trade_price)
                current_bar['close'] = trade_price
                current_bar['volume'] += trade_quantity
                
        # Append the last completed bar to the list
        if current_bar is not None:
            custom_bars.append(current_bar)
            
            
        return custom_bars
    
    ##############################################################################################################################
    def get_interval_seconds(self, interval):

        # convertit la fréquence str en type int et en secondes

        if interval.endswith('s'):
            return int(interval[:-1])
        if interval.endswith('m'):
            return int(interval[:-1]) * 60
        elif interval.endswith('h'):
            return int(interval[:-1]) * 3600
        elif interval.endswith('d'):
            return int(interval[:-1]) * 86400
        else:
            raise ValueError(f'Invalid interval: {interval}')
    
    ###################################################################################################################################################
    def get_symbols_bars(self, symbols, interval, start_time, end_time, limit):
        
        symbols_bars = []
        for symbol in symbols:
            symbol_data = []
            current_symbol = self.get_historical_klines(symbol, interval, start_str=start_time, end_str=end_time, limit=limit)
            for current_bar in current_symbol:
                    bar = {'Open time': current_bar[0],
                           'Open': current_bar[1],
                           'High': current_bar[2],
                           'Low': current_bar[3],
                           'Close': current_bar[4],
                           'Volume': current_bar[5],
                           'Close time': current_bar[6],
                           'Quote asset volume': current_bar[7],
                           'Number of trades': current_bar[8],
                           'Taker buy base asset volume': current_bar[9],
                           'Taker buy quote asset volume': current_bar[10]}             
                    symbol_data.append(bar)
            symbols_bars.append(symbol_data)
        return symbols_bars
    
    ###################################################################################################################################################
    def get_futures_or_spots(self, symbol, interval, start_time, end_time, limit, contract_type=None):
        if contract_type == "futures":
            bars = self.futures_klines(symbol=symbol, interval=interval, startTime=start_time, endTime=end_time, limit=limit)
        else:
            bars = self.get_historical_klines(symbol=symbol, interval=interval, start_str=start_time, end_str=end_time, limit=limit)
        return bars
            
            
##############################################################################################################################################




