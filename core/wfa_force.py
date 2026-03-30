from loguru import logger
from core.wfa_engine import WFAOptimizer

if __name__ == "__main__":
    logger.info("🔥 啟動強制 WFA 歷史回測與參數最佳化...")
    try:
        opt = WFAOptimizer()
        opt.optimize_all()  # 這行會觸發全幣種的暴力回測
        logger.info("✅ WFA 回測計算完畢！請去檢查 config/wfa_best_params.csv 的最新數值。")
    except Exception as e:
        logger.error(f"💥 WFA 執行失敗: {e}")