/**
 * Tips system for trend analysis education
 */

/**
 * Tips library with educational messages
 */
export const TIPS_LIBRARY = {
    TOUCH_TL: '📍 线是区，不是点：用 ATR 给线留呼吸空间。',
    BREAK_TL: '⚡ 突破 ≠ 延续；接受（回踩不破）才算数。',
    BREAKOUT_ACCEPTED: '✅ 回踩确认！突破已被市场接受。',
    FAILED_BREAKDOWN: '🔥 失败跌破！假突破=最集中的错误仓位。',
    FAILED_BREAKOUT: '⚠️ 上破失败！假突破=最集中的错误仓位。',
    RISKY_NTH_TOUCH: '⚠️ 第 N 次触碰同一条线，风险显著上升。',
    TIMEFRAME_CHANGE: '🔄 周期不是让你看得更清楚，而是让错误更少地发生。',
    VOLUME_PULLBACK: '📉 回踩缩量是健康，回踩放量是危险。',
    NO_VOLUME_BREAKOUT: '⚠️ 缩量突破，谨慎对待。',
    STRUCTURE_BREAK: '🚨 结构破坏！止损基于结构失效，不是基于线。',

    // Downtrend Tips
    DOWNTREND_KNIFE: '🔪 下跌趋势不言底，左侧交易是接飞刀。',
    DOWNTREND_WAIT: '🛑 下跌不言底，等待结构破坏再进场。',
    DOWNTREND_RETEST: '📉 下跌趋势中的反弹往往是诱多，关注压力位。',
    DOWNTREND_DONT_ADD: '⛔ 亏损加仓 = 破产加速器。只在盈利时加仓！',
    DOWNTREND_CUT_LOSS: '✂️ 扛单是爆仓的开始。错了就要认，挨打要立正。',
    DOWNTREND_NO_HOPE: '💭 别幻想反弹解套，市场不关心你的成本。',

    // Position Management / Mindset Tips (New)
    OPPORTUNITY_FILTER: '🛡️ 你现在看到的机会，90%都不是最好的那个。',
    POSITION_CONFIRMATION: '💰 用更多钱，押“市场已经证明我对了”。',
    MARKET_VALIDATION: '✅ “如果我是对的，市场自然会给我机会多买。”',
    SURVIVAL_FIRST: '💀 剩余仓位只有在“你还活着”的前提下才有意义。',
    POSITION_PURPOSE: '🎯 剩余仓位不是用来填坑，是用来扩大战果。',
    POSITION_REWARD: '🎁 仓位是奖励“正确”，不是补救“错误”。',
    NO_AVERAGING_DOWN: '⛔ 剩余仓位不是为“下跌加仓”准备的。',

    // Anti-Prediction Tips
    PREDICT_TOP: '🔮 只有神知道顶在哪里。做跟随者，不做预言家。',
    PREDICT_BOTTOM: '🕳️ 抄底是接飞刀的代名词。底部是走出来的，不是猜出来的。',
    PREDICT_REVERSAL: '✋ 别试图阻挡趋势列车。右侧交易虽迟但稳。',

    // Uptrend Tips
    UPTREND_CHASE: '🚀 上涨不言顶，但连续大涨后切勿追高。',
    UPTREND_PULLBACK: '⏳ 强势股回调是机会，但不要在加速赶顶时接盘。',
    UPTREND_RISK: '⚠️ 乖离率过大，此时追高盈亏比极差。',
};

export const PERMANENT_TIP = '趋势线不是进场按钮，反应才是信号。';

/**
 * Tips Engine class for managing tip display
 */
export class TipsEngine {
    constructor() {
        this.lastTipTime = {};
        this.cooldownMs = 60000;
        this.tipContainer = null;
        this.permanentTipEl = null;
    }

    /**
     * Initialize the tips container
     */
    init() {
        this.tipContainer = document.createElement('div');
        this.tipContainer.id = 'tips-container';
        this.tipContainer.style.cssText = `
            position: absolute; top: 0; left: 0; width: 100%; height: 100%;
            pointer-events: none; z-index: 30; overflow: hidden;
        `;
        document.getElementById('main-chart').appendChild(this.tipContainer);

        this.permanentTipEl = document.createElement('div');
        this.permanentTipEl.style.cssText = `
            position: absolute; top: 40px; right: 10px;
            background: rgba(236, 72, 153, 0.15); color: #ec4899;
            padding: 6px 10px; border-radius: 6px; margin-bottom: 6px;
            border-left: 3px solid #ec4899; font-size: 11px;
            pointer-events: auto;
        `;
        this.permanentTipEl.textContent = PERMANENT_TIP;
        this.tipContainer.appendChild(this.permanentTipEl);
    }

    /**
     * Check if a tip should be shown (cooldown check)
     * @param {string} tipKey 
     * @returns {boolean}
     */
    shouldShowTip(tipKey) {
        const now = Date.now();
        const lastTime = this.lastTipTime[tipKey] || 0;
        if (now - lastTime > this.cooldownMs) {
            this.lastTipTime[tipKey] = now;
            return true;
        }
        return false;
    }

    /**
     * Show a tip for the given event type
     * @param {string} eventType 
     */
    showTip(eventType) {
        if (!this.tipContainer) return;
        if (!this.shouldShowTip(eventType)) return;

        const text = TIPS_LIBRARY[eventType];
        if (!text) return;

        const tipEl = document.createElement('div');

        // Random position within 10% - 60% of width/height to avoid edges and clutter
        const top = 15 + Math.random() * 50;
        const left = 10 + Math.random() * 60;

        tipEl.style.cssText = `
            position: absolute;
            top: ${top}%;
            left: ${left}%;
            background: rgba(41, 98, 255, 0.85); 
            color: #ffffff;
            padding: 8px 12px; 
            border-radius: 6px; 
            font-size: 12px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            animation: tipFloatIn 0.5s ease-out;
            border-left: 3px solid #ffffff;
            pointer-events: auto;
            max-width: 200px;
            z-index: 100;
        `;
        tipEl.textContent = text;
        this.tipContainer.appendChild(tipEl);

        // Auto remove after 6 seconds
        setTimeout(() => {
            tipEl.style.opacity = '0';
            tipEl.style.transform = 'translateY(-10px)';
            tipEl.style.transition = 'all 0.5s';
            setTimeout(() => tipEl.remove(), 500);
        }, 6000);
    }

    /**
     * Show a random tip from a list of keys
     * @param {string[]} keys 
     */
    showRandomTip(keys) {
        const key = keys[Math.floor(Math.random() * keys.length)];
        this.showTip(key);
    }
}

// Singleton instance
export const tipsEngine = new TipsEngine();
