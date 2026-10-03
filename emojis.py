"""
emojis.py
─────────
Premium Telegram Custom Emoji definitions and helpers.
Uses Telegram HTML format: <tg-emoji emoji-id="ID">fallback</tg-emoji>
"""

def pe(emoji_id: str, fallback: str) -> str:
    """Return HTML string for Telegram custom emoji with fallback."""
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


# ── Premium Custom Emojis Catalog ──────────────────────────────────────────────
E_FLAME_BUTTERFLY = pe("6001449118000487326", "🦋")  # Flame Butterfly (Purple-Blue Fire)
E_WHITE_BUTTERFLY = pe("5084613633418199991", "🦋")  # White Butterfly
E_ARC_REACTOR     = pe("6001440193058444284", "⚙️")  # Arc Reactor / Iron Core Circle
E_CONFETTI        = pe("6282977077427702833", "🎉")  # Color Confetti Sparkle
E_SPARKLES        = pe("6023660820544623088", "✨")  # Multi Sparkles / Celebration
E_LIGHTNING       = pe("6026367225466720832", "⚡")  # Yellow Lightning Bolt
E_PINK_BOW        = pe("6066395745139824604", "🎀")  # Neon Pink Bow
E_COLOR_DOTS      = pe("5971944878815317190", "💫")  # Floating Color Dots
E_NEON_RINGS      = pe("5971837723676249096", "🌀")  # Neon Circle Rings
E_RING_LOADER     = pe("5974235702701853774", "⏳")  # Triple Ring Loader
E_GOLDEN_MAZE     = pe("4949560993840629085", "👑")  # Golden Maze / Mind Circle
E_RED_WOLF        = pe("6127636064610818291", "🐺")  # Red Neon Wolf
E_BLUE_WOLF       = pe("6127636064610818291", "🐺")  # Blue Neon Wolf
E_DARK_SHADOW     = pe("6026236216079290036", "🖤")  # Dark Shadow Face
E_BLACK_MASK      = pe("6025929233291809651", "😈")  # Black Mask Face
E_HEART_BORDER    = pe("5352918496642604333", "❤️")  # Black Heart Neon Border
E_BROKEN_HEART    = pe("6078087767106001151", "💔")  # Broken Purple Heart
E_ARROW           = pe("6285315214673975495", "➡️")  # Neon Arrow Right
E_WARNING         = pe("5420323339723881652", "⚠️")  # Red Warning Triangle
E_HEART_PULSE     = pe("5352727529511723136", "💓")  # Red Heart Pulse / ECG
E_SKULL           = pe("5253539825360843975", "💀")  # Skull / Ghost Style
E_BABY_NEON       = pe("6226493198013830325", "🍼")  # Baby Neon Text
E_DARK_CAT        = pe("6057466460886799210", "😼")  # Dark Cat Face

# ── Extended Set ─────────────────────────────────────────────────────────
E_HEART_RED       = pe("5352727529511723136", "💖")  # Premium Pulsing Glow Heart (VERIFIED)
E_HEART_FIRE      = pe("5352918496642604333", "❤️‍🔥")  # Neon Heart on Fire (VERIFIED)
E_CHECK_MARK      = "✅"
E_CROSS_MARK      = "❌"
E_FIRE_FLAME      = "🔥"
E_ROCKET          = "🚀"
E_DIAMOND         = "💎"
E_CHART_BAR       = "📊"
E_SHIELD          = "🛡️"
E_STAR_GLOW       = "⭐"
E_CLOCK_TIME      = "🕐"
E_PIN_LINK        = pe("6285315214673975495", "🔗")  # Link Pin (VERIFIED)
E_TV_SCREEN       = pe("5971837723676249096", "📺")  # TV Screen Channel (VERIFIED)
