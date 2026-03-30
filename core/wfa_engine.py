import pandas as pd
import numpy as np
import itertools
import requests
import yaml
import ccxt
import time
import os
from pathlib import Path
from loguru import logger

from core.orchestrator import DataSourceConfig


class WFAOptimizer:
    def __init__(self):
        # 設定要窮舉的參數矩陣 (Grid Search)
        self.donchian_list = [10, 15, 20, 25, 30]
        self.atr_mult_list = [0.2, 0.3, 0.4, 0.5]

        self.data_folder = Path(__file__).parent.parent / 'data' / 'GlassNodeData'
        self.config_folder = Path(__file__).parent.parent / 'config'
        self.ds = DataSourceConfig()
        self.strat_df = self.ds.load_info_dict()

    def get_gn_api(self):
        try:
            config_path = self.config_folder / 'config.yaml'
            with open(config_path, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f)
                return cfg.get('glassnode', {}).get('GN_API', None)
        except Exception as e:
            return None

    def download_5y_data(self, symbol):
        gn_api = self.get_gn_api()
        df = None

        if gn_api:
            logger.info(f"嘗試從 Glassnode 獲取 {symbol} 長期歷史數據...")
            try:
                url = "https://api.glassnode.com/v1/metrics/market/price_usd_ohlc"
                params = {'a': symbol, 'i': '1h', 'api_key': gn_api}
                res = requests.get(url, params=params)

                if res.status_code == 200:
                    data = res.json()
                    if len(data) > 2000:
                        rows = [{'date': pd.to_datetime(item['t'], unit='s', utc=True),
                                 'open': float(item.get('v', {}).get('o', 0)),
                                 'high': float(item.get('v', {}).get('h', 0)),
                                 'low': float(item.get('v', {}).get('l', 0)),
                                 'close': float(item.get('v', {}).get('c', 0)),
                                 'volume': 0.0} for item in data]
                        df = pd.DataFrame(rows).set_index('date')
                        logger.info(f"✅ 成功從 Glassnode 獲取 {len(df)} 筆 {symbol} 歷史數據！")
            except Exception as e:
                pass

        if df is None or len(df) < 2000:
            logger.info(f"⚠️ 啟動 Bybit 歷史節點回溯下載 {symbol} (抓取過去 4 年)...")
            try:
                exchange = ccxt.bybit({'enableRateLimit': True})
                market_symbol = f"{symbol}/USDT:USDT"
                since_time = pd.Timestamp.utcnow() - pd.DateOffset(years=4)
                since = exchange.parse8601(since_time.strftime('%Y-%m-%dT00:00:00Z'))

                all_ohlcv = []
                while since < exchange.milliseconds():
                    ohlcv = exchange.fetch_ohlcv(market_symbol, '1h', since=since, limit=1000)
                    if not ohlcv: break
                    since = ohlcv[-1][0] + 1
                    all_ohlcv.extend(ohlcv)
                    time.sleep(0.1)

                df = pd.DataFrame(all_ohlcv, columns=['t', 'open', 'high', 'low', 'close', 'volume'])
                df['date'] = pd.to_datetime(df['t'], unit='ms', utc=True)
                df.set_index('date', inplace=True)
                df.drop(columns=['t'], inplace=True)
                logger.info(f"✅ 成功從 Bybit 獲取 {len(df)} 筆 {symbol} 歷史數據！")
            except Exception as e:
                logger.error(f"Bybit 歷史抓取失敗: {e}")

        return df

    def resample_to_1h(self, df: pd.DataFrame):
        # 1H 系統不需要降頻，直接回傳清理好的 DataFrame
        return df.copy()

    def calc_base_indicators(self, df: pd.DataFrame):
        df['SMA_200'] = df['close'].rolling(window=200).mean()
        df['VIX_Proxy'] = df['close'].pct_change().rolling(window=30).std() * np.sqrt(365 * 6) * 100
        df['K_Body_Abs'] = abs(df['close'] - df['open'])

        tr1 = df['high'] - df['low']
        tr2 = abs(df['high'] - df['close'].shift(1))
        tr3 = abs(df['low'] - df['close'].shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['ATR_14'] = tr.rolling(window=14).mean()
        return df

    def run_vectorized_backtest(self, df: pd.DataFrame, donchian: int, atr_mult: float):
        test_df = df.copy()

        test_df['Donchian_High'] = test_df['high'].rolling(window=donchian).max()
        test_df['Donchian_Low'] = test_df['low'].rolling(window=donchian).min()

        cond_vix = test_df['VIX_Proxy'] > 20.0
        cond_mom = test_df['K_Body_Abs'] > (atr_mult * test_df['ATR_14'])

        # 進場條件
        long_entry = cond_vix & cond_mom & (test_df['close'] > test_df['SMA_200']) & (
                    test_df['close'] > test_df['Donchian_High'].shift(1))
        short_entry = cond_vix & cond_mom & (test_df['close'] < test_df['SMA_200']) & (
                    test_df['close'] < test_df['Donchian_Low'].shift(1))

        # 出場條件 (突破另一邊的通道下緣/上緣即出場)
        long_exit = test_df['close'] < test_df['Donchian_Low'].shift(1)
        short_exit = test_df['close'] > test_df['Donchian_High'].shift(1)

        # 加入「倉位記憶」狀態機
        le = long_entry.to_numpy()
        se = short_entry.to_numpy()
        lx = long_exit.to_numpy()
        sx = short_exit.to_numpy()

        signals = np.zeros(len(test_df))
        current_pos = 0

        for i in range(len(test_df)):
            if current_pos == 0:
                if le[i]:
                    current_pos = 1
                elif se[i]:
                    current_pos = -1
            elif current_pos == 1:
                if lx[i]: current_pos = 0
            elif current_pos == -1:
                if sx[i]: current_pos = 0
            signals[i] = current_pos

        test_df['signal'] = signals

        # 計算持有報酬
        test_df['return'] = test_df['close'].pct_change().shift(-1)
        test_df['strategy_return'] = test_df['signal'] * test_df['return']

        # 扣除千分之1.5手續費 (只在進出場時扣除)
        trades_mask = test_df['signal'].diff().abs() > 0
        test_df.loc[trades_mask, 'strategy_return'] -= 0.0015

        cumulative_return = (1 + test_df['strategy_return']).prod() - 1
        return cumulative_return

    def optimize_all(self):
        logger.info("\n" + "=" * 50 + "\n🚀 啟動 1H WFA 波段動能最佳化引擎\n" + "=" * 50)

        best_params_list = []

        for _, row in self.strat_df.iterrows():
            symbol = str(row['symbol'])

            df_1h_raw = self.download_5y_data(symbol)
            if df_1h_raw is None or df_1h_raw.empty:
                continue

            logger.info(f"正在對 {symbol} 進行 1H 網格回測最佳化運算...")
            df_1h_clean = self.resample_to_1h(df_1h_raw)
            df_1h_clean = self.calc_base_indicators(df_1h_clean)
            df_1h_clean.dropna(inplace=True)

            best_score = -9999
            best_donchian = 20
            best_atr = 0.5

            combinations = list(itertools.product(self.donchian_list, self.atr_mult_list))

            for donchian, atr_mult in combinations:
                score = self.run_vectorized_backtest(df_1h_clean, donchian, atr_mult)
                if score > best_score:
                    best_score = score
                    best_donchian = donchian
                    best_atr = atr_mult

            logger.info(
                f"🌟 {symbol} 最強 1H 黃金參數 -> Donchian: {best_donchian}, ATR_Mult: {best_atr} | 預估長期複合報酬: {best_score * 100:.2f}%")

            best_params_list.append({
                'symbol': symbol,
                'best_donchian': best_donchian,
                'best_atr_mult': best_atr
            })

        params_df = pd.DataFrame(best_params_list)
        output_path = self.config_folder / 'wfa_best_params.csv'
        params_df.to_csv(output_path, index=False)
        logger.info(f"\n🎯 1H WFA 最佳化完成！參數已更新至: {output_path}")


if __name__ == "__main__":
    optimizer = WFAOptimizer()
    optimizer.optimize_all()