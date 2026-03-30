import ccxt
import pandas as pd
from loguru import logger
from pathlib import Path
import time

class DataCenterSrv:
    data_folder_GN = Path(__file__).parent.parent / 'data' / 'GlassNodeData'

    def __init__(self, strat_df: pd.DataFrame):
        self.strat_df = strat_df
        self.exchange = ccxt.bybit({'enableRateLimit': True})

    def create_df(self):
        self.data_folder_GN.mkdir(parents=True, exist_ok=True)

        if self.strat_df is None or self.strat_df.empty:
            logger.error('strat_df is empty or None.')
            return

        timeframe = '1h'
        limit = 1000

        for _, row in self.strat_df.iterrows():
            name = str(row['name'])
            symbol = str(row['symbol'])
            market_symbol = f'{symbol}/USDT:USDT'
            filename_ap = f'{name}_{symbol}_ap.csv'
            file_path_ap = self.data_folder_GN / filename_ap

            logger.info(f'Fetching {timeframe} OHLCV data from Bybit for {market_symbol}...')

            # 加上 3 次防護重試機制
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                try:
                    ohlcv = self.exchange.fetch_ohlcv(market_symbol, timeframe, limit=limit)
                    if not ohlcv:
                        logger.warning(f'No data returned from Bybit for {market_symbol}')
                        break

                    df = pd.DataFrame(ohlcv, columns=['t', 'o', 'h', 'l', 'c', 'v'])
                    df['t'] = pd.to_datetime(df['t'], unit='ms', utc=True)
                    df = df.set_index('t')
                    df.index.name = 'date'

                    df.to_csv(file_path_ap)
                    logger.info(f'Successfully saved {len(df)} rows to {filename_ap}')

                    time.sleep(1.5)  # 成功後稍微休息
                    break  # 跳出重試迴圈

                except Exception as e:
                    if "Too many visits" in str(e) or "Rate Limit" in str(e):
                        logger.warning(
                            f'[{attempt}/{max_retries}] 觸發 Bybit Rate Limit, 暫停 5 秒後重試 ({symbol})...')
                        time.sleep(5)
                    else:
                        logger.error(f'Failed to fetch Bybit data for {symbol}: {e}')
                        time.sleep(2)
                        break