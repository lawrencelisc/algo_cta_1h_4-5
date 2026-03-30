import time
import ccxt
import os
import gc
import pandas as pd
from loguru import logger
from threading import Thread
from queue import Queue
from pathlib import Path
from datetime import datetime

from core.orchestrator import DataSourceConfig
from utils.trade_record import TradeRecord
from utils.tg_wrapper import SendTGBot


class TelegramNotifier:
    def __init__(self):
        self.tg = SendTGBot()
        self.queue = Queue()
        self.worker_thread = None
        self._start_worker()

    def _start_worker(self):
        if self.worker_thread is None or not self.worker_thread.is_alive():
            self.worker_thread = Thread(target=self._worker, daemon=True)
            self.worker_thread.start()

    def _worker(self):
        while True:
            message_data = self.queue.get()
            if message_data is None: break
            txt_msg = message_data['message']
            for attempt in range(1, 4):
                try:
                    if self.tg.send_df_msg(txt_msg, timeout=20): break
                except Exception:
                    time.sleep(2)
            self.queue.task_done()

    def send(self, message: str):
        self.queue.put({'message': message})

    def wait(self, timeout=None):
        self.queue.join()


class SignalExecution:
    def __init__(self, signal_df: pd.DataFrame):
        self.signal_df = signal_df
        self.tg_notifier = TelegramNotifier()
        self.bybit_cfg = DataSourceConfig()

        # 讀取 1H 參數表
        params_path = Path(__file__).parent.parent / 'config' / 'symbol_params_1h.csv'
        try:
            self.symbol_params = pd.read_csv(params_path).set_index('symbol')
        except Exception:
            self.symbol_params = pd.DataFrame()

    def _get_exchange_for_symbol(self, symbol: str):
        try:
            api_info = self.bybit_cfg.load_bybit_api_config(symbol)
            return ccxt.bybit({
                'apiKey': api_info.get(f'{symbol}_1H_API_KEY', ''),
                'secret': api_info.get(f'{symbol}_1H_SECRET_KEY', ''),
                'enableRateLimit': True,
                'options': {'defaultType': 'swap'},
            })
        except Exception:
            return None

    def get_account_equity(self, exchange):
        try:
            # 加入 params={'type': 'unified'} 強制告訴 Bybit 我們要查 UTA 的餘額
            balance = exchange.fetch_balance(params={'type': 'unified'})

            # 優先嘗試抓取 UTA 的總保證金餘額 (Total Equity)
            try:
                total_equity = float(balance['info']['result']['list'][0]['totalEquity'])
                if total_equity > 0:
                    return total_equity
            except Exception:
                pass # 如果不是 UTA 帳戶，就退回原本的抓法

            # 如果上面抓不到，退回原本的抓法
            return float(balance['total'].get('USDT', 0.0))
        except Exception as e:
            logger.error(f"獲取餘額失敗: {e}")
            return 0.0

    def get_latest_atr(self, symbol: str):
        try:
            # 確保去 1h 的資料夾抓最新的 ATR 波動率
            target_files = list((Path(__file__).parent.parent / 'data' / 'StratData' / '1h').glob(f'*_{symbol}.csv'))
            if not target_files: return 0
            return float(pd.read_csv(target_files[0])['ATR_14'].iloc[-1])
        except Exception:
            return 0

    def calculate_position_size(self, symbol: str, current_price: float, exchange):
        if symbol not in self.symbol_params.index: return 0, 0
        p = self.symbol_params.loc[symbol]
        equity = self.get_account_equity(exchange)
        atr = self.get_latest_atr(symbol)
        if equity <= 0 or atr <= 0: return 0, 0
        sl_distance = atr * float(p['sl_atr_multi'])
        return ((equity * float(p['risk_pct'])) / sl_distance), sl_distance

    def get_current_position(self, symbol: str, side: str, exchange):
        try:
            for pos in exchange.fetch_positions([f"{symbol}/USDT:USDT"]):
                if pos['side'] == side and float(pos['contracts']) > 0:
                    return float(pos['contracts'])
            return 0.0
        except Exception:
            return 0.0

    # 加入 None-Safe 防空值保護，防止剛下單時交易所回傳 null 導致當機
    # 獲取最新倉位詳細變化資訊 (包含 P&L)
    def get_position_details(self, symbol: str, exchange):
        try:
            # 使用 'or 0.0' 來防止 Bybit API 回傳 None 導致 float() 當機
            for pos in exchange.fetch_positions([f"{symbol}/USDT:USDT"]):
                contracts = float(pos.get('contracts') or 0.0)
                if contracts > 0:
                    entry_price = float(pos.get('entryPrice') or 0.0)
                    upnl = float(pos.get('unrealizedPnl') or 0.0)
                    info = pos.get('info', {})
                    try:
                        rpnl = float(info.get('cumRealisedPnl') or 0.0)
                    except Exception:
                        rpnl = 0.0
                    val = float(pos.get('notional') or 0.0)
                    if val == 0.0:
                        val = entry_price * contracts
                    entry_str = f"{entry_price:.4f}" if entry_price < 10 else f"{entry_price:.2f}"
                    return entry_str, val, upnl, rpnl
            return "0.0", 0.0, 0.0, 0.0
        except Exception as e:
            logger.error(f"獲取倉位細節失敗 {symbol}: {e}")
            return "0.0", 0.0, 0.0, 0.0

    def prev_signal_df(self):
        prev_path = Path(__file__).parent.parent / 'data' / 'Signal' / 'prev_signal_table.csv'

        # 如果有舊檔案，讀取後必須改名成 _s1
        if os.path.exists(prev_path):
            df = pd.read_csv(prev_path)
            return df.rename(columns={'date': 'date_s1', 'signal': 'signal_s1'})[['date_s1', 'signal_s1']]

        # 如果沒有舊檔案（第一次執行），產生預設值
        df = self.signal_df.copy()
        df['signal'] = 0
        return df.rename(columns={'date': 'date_s1', 'signal': 'signal_s1'})[['date_s1', 'signal_s1']]

    # 🌟 1H 專用帶有 TP 停利邏輯與自動槓桿的進場函數
    def execute_entry_order(self, symbol: str, action: str, exchange):
        market_symbol = f"{symbol}/USDT:USDT"
        try:
            exchange.load_markets()

            # ==========================================
            # 🌟 新增功能：進場前自動依照 CSV 設定槓桿
            lev = 1  # 預設防呆值
            if symbol in self.symbol_params.index:
                lev = int(self.symbol_params.loc[symbol, 'leverage'])
                try:
                    exchange.set_leverage(lev, market_symbol)
                    logger.info(f"✅ {symbol} 槓桿成功校正為 {lev}x")
                except Exception as e:
                    if "not modified" not in str(e).lower():
                        logger.debug(f"⚠️ {symbol} 槓桿設定提示: {e}")
            # ==========================================

            entry_price = exchange.fetch_ticker(market_symbol)['ask' if action == 'long' else 'bid']
            side = 'buy' if action == 'long' else 'sell'

            size, sl_distance = self.calculate_position_size(symbol, entry_price, exchange)
            if size <= 0: return 0, 0, 0

            # ==========================================
            # 🛡️ 終極防護：保證金購買力上限檢查 (Margin Safety Cap)
            bal = self.get_account_equity(exchange)
            max_notional = bal * lev * 0.95
            max_size = max_notional / entry_price

            if size > max_size:
                logger.warning(f"⚠️ {symbol} 數量 ({size}) 超過保證金購買力上限！自動縮小為 ({max_size})")
                size = max_size
            # ==========================================

            p = self.symbol_params.loc[symbol]
            tp_multi = float(p.get('tp_atr_multi', 3.0))
            atr = self.get_latest_atr(symbol)
            tp_distance = atr * tp_multi

            size = float(exchange.amount_to_precision(market_symbol, size))

            if action == 'long':
                stop_loss_price = float(exchange.price_to_precision(market_symbol, entry_price - sl_distance))
                take_profit_price = float(exchange.price_to_precision(market_symbol, entry_price + tp_distance))
            else:
                stop_loss_price = float(exchange.price_to_precision(market_symbol, entry_price + sl_distance))
                take_profit_price = float(exchange.price_to_precision(market_symbol, entry_price - tp_distance))

            exchange.create_order(
                symbol=market_symbol, type='limit', side=side, amount=size, price=entry_price,
                params={'stopLoss': str(stop_loss_price), 'takeProfit': str(take_profit_price)}
            )

            # 傳送 4H 專屬排版的 Telegram 報捷訊息
            self.tg_notifier.send(
                f"🚀 SIGNAL: {side.upper()} {symbol}\n🧮 QTY: {size}\n⚙️ LEVERAGE: {lev}x\n🈹️ SL: {stop_loss_price}\n💵 ENTRY: {entry_price}\n🤑 TP: {take_profit_price}")
            return size, entry_price, stop_loss_price
        except Exception as e:
            logger.error(f"進場錯誤 {symbol}: {e}")
            return 0, 0, 0

    def execute_close_order(self, symbol: str, action: str, exchange):
        market_symbol = f"{symbol}/USDT:USDT"
        side, pos_side = ('sell', 'long') if action == 'long' else ('buy', 'short')
        current_size = self.get_current_position(symbol, pos_side, exchange)
        if current_size <= 0:
            logger.info(f"🐻‍ {symbol} 已無倉位，自動歸零")
            return -1, 0

        try:
            close_price = exchange.fetch_ticker(market_symbol)['bid' if action == 'long' else 'ask']
            exchange.create_order(symbol=market_symbol, type='market', side=side, amount=current_size,
                                  params={'reduceOnly': True})
            self.tg_notifier.send(f"🈹️ 平倉: {pos_side.upper()} {symbol}\n🧮 數量: {current_size}\n💵 價格: {close_price}")
            return current_size, close_price
        except Exception:
            return 0.0, 0.0

    def create_market_order(self):
        signal_df = self.signal_df
        trade = TradeRecord(self.signal_df)
        result_df = pd.concat([signal_df.reset_index(drop=True), self.prev_signal_df().reset_index(drop=True)], axis=1)
        result_df['signal_plus'] = result_df['signal_s1'].astype(str) + result_df['signal'].astype(str)

        INITIAL_TOTAL_BAL = 500
        total_current_bal = 0.0

        report_lines = [f"🏧 <b>1H CTA 提款機器</b>"]
        report_lines.append(f"時間: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        report_lines.append("------------------------------------------------------------")
        report_lines.append(f"💰 INITIAL BAL: USDT {INITIAL_TOTAL_BAL} 💰")
        report_lines.append("------------------------------------------------------------")

        status_map = {
            '00': '💎 空手觀望',
            '11': '❤️ 持有多單',
            '-1-1': '🎈️ 持有空單',
            '01': '🚀 觸發進場 (LONG)',
            '0-1': '🚀 觸發進場 (SHORT)',
            '10': '🈹️ 觸發平倉 (LONG)',
            '-10': '🈹️ 觸發平倉 (SHORT)',
            '1-1': '🦞 觸發反手 (LONG > SHORT)',
            '-11': '🦞 觸發反手 (SHORT > LONG)'
        }

        # 2. 迴圈檢查每個幣種並下單
        for index, row in result_df.iterrows():
            symbol = row['symbol']
            exchange = self._get_exchange_for_symbol(symbol)
            if not exchange:
                report_lines.append(f"💥 {symbol}: 連線失敗")
                continue

            # 確保重置這個標記
            was_auto_synced = False

            if str(row['signal_s1']) in ['1', '-1']:
                check_side = 'long' if str(row['signal_s1']) == '1' else 'short'
                actual_pos = self.get_current_position(symbol, check_side, exchange)
                # 發現實際已被平倉 (打到TP/SL)
                if actual_pos <= 0:
                    logger.info(f"🐻‍ {symbol} 已無倉位，自動歸零")
                    result_df.at[index, 'signal_s1'] = 0
                    result_df.at[index, 'signal'] = 0
                    result_df.at[index, 'signal_plus'] = '00'
                    self.signal_df.loc[self.signal_df['symbol'] == symbol, 'signal'] = 0
                    was_auto_synced = True

            signal_plus = str(result_df.at[index, 'signal_plus'])
            status_text = status_map.get(signal_plus, signal_plus)

            # 如果是被自動歸零的，把那句提示語加在「空手觀望」後面！
            if was_auto_synced:
                status_text += " (🎈️ 已觸發 TP/SL 離場，空倉確認)"

            if signal_plus == '01':
                size, entry, sl = self.execute_entry_order(symbol, 'long', exchange)
                if size > 0:
                    trade.open_long(symbol, size, entry, sl)
                else:
                    self.signal_df.loc[self.signal_df['symbol'] == symbol, 'signal'] = 0
                    status_text += " (💥 下單失敗，退回觀望)"

            elif signal_plus == '10':
                size, price = self.execute_close_order(symbol, 'long', exchange)
                if size > 0:
                    trade.close_long(symbol, size, price, 0.0)
                elif size == -1:
                    status_text += " (🎈️ 已觸發 TP/SL 離場，空倉確認)"
                    self.signal_df.loc[self.signal_df['symbol'] == symbol, 'signal'] = 0
                else:
                    self.signal_df.loc[self.signal_df['symbol'] == symbol, 'signal'] = 1

            elif signal_plus == '0-1':
                size, entry, sl = self.execute_entry_order(symbol, 'short', exchange)
                if size > 0:
                    trade.open_short(symbol, size, entry, sl)
                else:
                    self.signal_df.loc[self.signal_df['symbol'] == symbol, 'signal'] = 0
                    status_text += " (💥 下單失敗，退回觀望)"

            elif signal_plus == '-10':
                size, price = self.execute_close_order(symbol, 'short', exchange)
                if size > 0:
                    trade.close_short(symbol, size, price, 0.0)
                elif size == -1:
                    status_text += " (🎈️ 已觸發 TP/SL 離場，空倉確認)"
                    self.signal_df.loc[self.signal_df['symbol'] == symbol, 'signal'] = 0
                else:
                    self.signal_df.loc[self.signal_df['symbol'] == symbol, 'signal'] = -1

            elif signal_plus == '1-1':
                size, price = self.execute_close_order(symbol, 'long', exchange)
                if size > 0: trade.close_long(symbol, size, price, 0.0)
                time.sleep(2)
                size, entry, sl = self.execute_entry_order(symbol, 'short', exchange)
                if size > 0:
                    trade.open_short(symbol, size, entry, sl)
                else:
                    self.signal_df.loc[self.signal_df['symbol'] == symbol, 'signal'] = 0

            elif signal_plus == '-11':
                size, price = self.execute_close_order(symbol, 'short', exchange)
                if size > 0: trade.close_short(symbol, size, price, 0.0)
                time.sleep(2)
                size, entry, sl = self.execute_entry_order(symbol, 'long', exchange)
                if size > 0:
                    trade.open_long(symbol, size, entry, sl)
                else:
                    self.signal_df.loc[self.signal_df['symbol'] == symbol, 'signal'] = 0

            report_lines.append(f"🏅️ {symbol}: {status_text}")

            final_signal = str(self.signal_df.loc[self.signal_df['symbol'] == symbol, 'signal'].values[0])

            # 取得該子帳戶的餘額並累加至總結餘
            bal = self.get_account_equity(exchange)
            total_current_bal += bal

            if final_signal in ['1', '-1']:
                entry_p, val, upnl, rpnl = self.get_position_details(symbol, exchange)
                report_lines.append(f"  💵 BAL: {bal:.2f} | VALUE: {val:.2f}")
                report_lines.append(f"  💵 UN-R_P&L: {upnl:+.2f} | R_P&L: {rpnl:+.2f}")
            else:
                report_lines.append(f"💵 BAL: {bal:.2f}")

        report_lines.append("------------------------------------------------------------")
        report_lines.append(f"💰 CURRENT BAL: USDT {total_current_bal:.2f} 💰")
        report_lines.append("------------------------------------------------------------")
        report_lines.append("✅ 本輪 1H 掃描執行完畢")

        self.tg_notifier.send('\n'.join(report_lines))
        self.signal_df.to_csv(Path(__file__).parent.parent / 'data' / 'Signal' / 'prev_signal_table.csv', index=False)
        self.tg_notifier.wait(timeout=60)
        gc.collect()