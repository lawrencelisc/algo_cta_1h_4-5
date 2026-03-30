import gc
import pandas as pd
import numpy as np
from loguru import logger
from pathlib import Path
import os

class AlgoStrategy:
    data_folder_GN = Path(__file__).parent.parent / 'data' / 'GlassNodeData'
    res_list = ['1h']

    def __init__(self, strat_df: pd.DataFrame):
        self.strat_df = strat_df
        # 讀取 WFA 最佳參數表
        wfa_path = Path(__file__).parent.parent / 'config' / 'wfa_best_params.csv'
        try:
            self.wfa_df = pd.read_csv(wfa_path).set_index('symbol')
        except Exception as e:
            self.wfa_df = pd.DataFrame()
            logger.warning("找不到 wfa_best_params.csv，將使用預設參數 20")

    def data_collect(self):
        for res in self.res_list:
            strat_folder = Path(__file__).parent.parent / 'data' / 'StratData' / res
            strat_folder.mkdir(parents=True, exist_ok=True)

            required_cols = {'name', 'symbol', 'strat'}
            missing = required_cols - set(self.strat_df.columns)
            if missing:
                logger.error(f'strat_df missing required columns: {missing}')
                return

            try:
                for _, row in self.strat_df.iterrows():
                    p_name = str(row['name'])
                    p_symbol = str(row['symbol'])
                    p_filename = f'{p_name}_{p_symbol}_ap.csv'
                    p_file_path = self.data_folder_GN / p_filename

                    if not p_file_path.exists():
                        logger.warning(f"File not found: {p_file_path}")
                        continue

                    p_df = pd.read_csv(p_file_path, index_col='date')
                    p_df.index = pd.to_datetime(p_df.index)

                    if res == '1h':
                        # 1H 系統不需要降頻，直接轉換欄位名稱
                        result_df = p_df.copy()
                        result_df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'},
                                         inplace=True)
                    else:
                        result_df = p_df.copy()

                    # 取得專屬的 Donchian 期數
                    best_donchian = 20
                    if not self.wfa_df.empty and p_symbol in self.wfa_df.index:
                        best_donchian = int(self.wfa_df.loc[p_symbol, 'best_donchian'])

                    # 計算 ATR
                    tr1 = result_df['high'] - result_df['low']
                    tr2 = abs(result_df['high'] - result_df['close'].shift(1))
                    tr3 = abs(result_df['low'] - result_df['close'].shift(1))
                    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                    result_df['ATR_14'] = tr.rolling(window=14).mean()

                    # 計算 CTA 核心指標 (使用動態 Donchian 期數)
                    result_df['SMA_200'] = result_df['close'].rolling(window=200).mean()
                    result_df['Donchian_High'] = result_df['high'].rolling(window=best_donchian).max()
                    result_df['Donchian_Low'] = result_df['low'].rolling(window=best_donchian).min()
                    result_df['K_Body_Abs'] = abs(result_df['close'] - result_df['open'])
                    result_df['VIX_Proxy'] = result_df['close'].pct_change().rolling(window=30).std() * np.sqrt(365 * 6) * 100

                    result_df.dropna(inplace=True)

                    output_filename = f'{p_name}_{res}_{p_symbol}.csv'
                    output_path = strat_folder / output_filename
                    result_df.to_csv(output_path, date_format='%Y-%m-%d %H:%M:%S')
                    logger.info(f'File saved ({output_filename}) with {len(result_df)} rows')

                logger.info(f'Aggregation ....... completed\n')
            except Exception as e:
                logger.error(f"algo_strat process failed: {e}")