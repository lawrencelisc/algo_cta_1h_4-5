import time
import schedule
from loguru import logger
from datetime import datetime

# 匯入我們的四大部門
from core.datacenter import DataCenterSrv
from core.algo_strat import AlgoStrategy
from core.orchestrator import DataSourceConfig
from strategy.strat_method import CreateSignal
from core.execution import SignalExecution
from utils.tg_wrapper import SendTGBot
from core.wfa_engine import WFAOptimizer

def run_cta_pipeline():
    """系統核心流水線：依序喚醒各部門執行工作"""
    logger.info(f"\n{'=' * 50}\n🔥 啟動 1H CTA 策略執行 | 時間: {datetime.now()}\n{'=' * 50}")

    try:
        # 0. 載入配置 (Config)
        ds = DataSourceConfig()
        strat_df = ds.load_info_dict()

        # 1. 抓取數據 (DataCenter)
        logger.info(">>> STEP 1: 正在抓取最新 1H K線數據...")
        dc = DataCenterSrv(strat_df)
        dc.create_df()

        # 2. 計算指標 (AlgoStrategy)
        logger.info(">>> STEP 2: 正在計算技術指標 (200MA/ATR/VIX)...")
        algo = AlgoStrategy(strat_df)
        algo.data_collect()

        # 3. 產生訊號 (CreateSignal)
        logger.info(">>> STEP 3: 正在判斷動能突破訊號...")
        gen_signal = CreateSignal(strat_df)
        signal_df = gen_signal.split_sub()

        # 4. 執行下單與風控 (SignalExecution)
        logger.info(">>> STEP 4: 正在核對部位與派發實盤訂單...")
        executor = SignalExecution(signal_df)
        executor.create_market_order()

        logger.info(f"\n{'=' * 50}\n🎉 1H CTA 執行順利完成\n{'=' * 50}\n")

    except Exception as e:
        error_msg = f"💥 執行發生嚴重錯誤: {e}"
        logger.error(error_msg)
        # 如果系統發生中斷級別的錯誤，立刻發送 Telegram 通知
        try:
            tg = SendTGBot()
            tg.send_df_msg(error_msg)
        except:
            pass

def run_wfa_pipeline():
    # 週末 WFA 最佳化專用流水線
    logger.info("🔥週末例行公事：開始 WFA 參數窮舉最佳化...")
    try:
        opt = WFAOptimizer()
        opt.optimize_all()
        tg = SendTGBot()
        tg.send_df_msg("🎯 週末 1H WFA 最佳化完成，本週黃金參數已更新！下週我們將用最強狀態迎戰市場。")
    except Exception as e:
        logger.error(f"💥 WFA 執行失敗: {e}")

def main():
    logger.info("🔥 1H CTA 動能系統啟動")
    try:
        tg = SendTGBot()
        tg.send_df_msg("🔥 1H CTA 伺服器已啟動，開始監控突破策略")
    except Exception as e:
        pass

    # ==========================================
    # 🌟 1. 設定 1H 交易排程
    # ==========================================

    # True (bypass) 啟動時先強制跑一次測試 (若不需要可註解此行)
    if True: run_cta_pipeline()

    # 每個小時的 00分15秒 觸發一次 (例如 01:00:15, 02:00:15...)
    schedule.every().hour.at("00:15").do(run_cta_pipeline)

    # WFA 最佳化排程 (每週日深夜 23:45 執行)
    schedule.every().sunday.at("23:45:00").do(run_wfa_pipeline)


    # 進入無限迴圈等待排程
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.warning('接收到鍵盤中斷信號；程式已終止')

if __name__ == "__main__":
    main()