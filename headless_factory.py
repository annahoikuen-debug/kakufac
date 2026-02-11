import os
import json
import time
import datetime
import re
import random
import zipfile
import io
import sqlite3
import smtplib
import asyncio
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from google import genai
from google.genai import types

# ==========================================
# 0. 設定 & モデル厳格分離
# ==========================================
API_KEY = os.environ.get("GEMINI_API_KEY")
GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_PASS = os.environ.get("GMAIL_PASS")
TARGET_EMAIL = os.environ.get("GMAIL_USER")

# --- Model Configuration ---
# 【プロット用 (APIコール数: 全2回固定)】
# 複雑な構成と長文脈理解が必要なため Gemini 2.0 Flash を使用
MODEL_ARCHITECT = "gemini-2.0-flash" 

# 【執筆・リライト・QA用 (その他すべて)】
# 指示従順性が高く、日本語能力に優れた Gemma 3 27B を使用
MODEL_WRITER = "gemma-3-27b-it"

# 【トレンド分析・高速処理用】
# 軽量な Gemma 3 12B を使用
MODEL_FAST = "gemma-3-12b-it" 

DB_FILE = "factory_run_gemma_parallel.db"

# ==========================================
# 1. 文体定義 & Prompt Assets (完全流用)
# ==========================================
STYLE_DEFINITIONS = {
    "style_serious_fantasy": {
        "name": "無職転生風（理不尽な孫の手）",
        "instruction": "【文体模倣: 伝記的・内省的】\n1. 地の文は「過去を回想する手記」のような落ち着いたトーンで記述せよ。\n2. 主人公の心理描写は、自身の弱さや欲望を隠さず、赤裸々に、しかしどこか客観的に描け。\n3. 世界観の説明は、五感（食事の味、土の匂い）を通じて生活感を持たせろ。\n4. 戦闘描写よりも、その結果としての「成長」や「後悔」に焦点を当てろ。"
    },
    "style_psychological_loop": {
        "name": "リゼロ風（長月達平）",
        "instruction": "【文体模倣: 感情爆発・心理極地】\n1. 絶望的な状況では、息継ぎのできないような長文と、畳み掛けるような短文を織り交ぜろ。\n2. 「――ッ！」や「ぁ、あ……」といった、言葉にならない悲鳴や呼吸音を多用せよ。\n3. 心理描写は、自己嫌悪と他者への執着を、粘着質に、執拗に繰り返せ。\n4. キャラクターのセリフは感情全開で、叫び、泣き、懇願するトーンを維持せよ。"
    },
    "style_military_rational": {
        "name": "幼女戦記風（カルロ・ゼン）",
        "instruction": "【文体模倣: 合理的・論理的】\n1. 語彙は極めて硬質に。「認識」「最適化」「費用対効果」などの熟語を多用せよ。\n2. 地の文は、感情を排した「報告書」や「論文」のようなドライな文体を維持せよ。\n3. 神や運命を冷笑し、物理法則と経済合理性のみを信じる視点を貫け。\n4. カタカナ語（シカゴ学派、ドクトリン等）を漢字の中に混ぜ、インテリジェンスな雰囲気を醸成せよ。"
    },
    "style_magic_engineering": {
        "name": "魔法科風（佐島勤）",
        "instruction": "【文体模倣: 設定資料・解説的】\n1. アクションシーンであっても、「現象の物理的・魔法的なメカニズム」を詳細に解説せよ。\n2. 感情よりも「事象」を優先し、魔法の発動プロセス（CAD、起動式、事象改変）を論理的に記述せよ。\n3. インデントや改行を整然とし、説明文のような読みやすさを意識せよ。\n4. 主人公は常に冷静で、周囲の驚きをよそに淡々と最適解を実行させろ。"
    },
    "style_comedy_speed": {
        "name": "このすば風（暁なつめ）",
        "instruction": "【文体模倣: 軽快・漫才】\n1. 地の文は最小限にし、セリフの掛け合い（テンポ）で物語を進行させよ。\n2. 主人公のツッコミは鋭く、かつ情けなく。「おい待て」「ふざけんな」等の口語体を地の文にも混ぜろ。\n3. シリアスな雰囲気は3行以上続けるな。必ずオチや台無しにする要素を入れろ。\n4. 擬音（カエルの鳴き声、爆裂音）をコミカルに表現せよ。"
    },
    "style_overlord": {
        "name": "オバロ風（丸山くがね）",
        "instruction": "【文体模倣: 荘厳・アンジャッシュ】\n1. 主人公の独白（小心者）と、周囲からの視点（絶対的な支配者）のギャップを強調せよ。\n2. 配下の描写は、宗教的なまでの崇拝と、過剰な敬語を用いて記述せよ。\n3. 戦闘は「圧倒的な蹂躙」として描き、相手の絶望を詳細に描写せよ。\n4. 組織運営や政治的な駆け引きの描写を重厚に差し込め。"
    },
    "style_slime_nation": {
        "name": "転スラ風（伏瀬）",
        "instruction": "【文体模倣: 会議・スキルログ】\n1. 問題解決は「スキルの獲得」や「進化」によって行われるプロセスを明確にせよ（《告。〜を獲得しました》）。\n2. 深刻になりすぎず、「なんとかなるだろう」という楽観的なトーンを維持せよ。\n3. 会議シーンでは、複数の部下がそれぞれの専門分野から意見を述べる形式を多用せよ。\n4. 主人公は親しみやすく、部下からは過剰に愛されている状況を描け。"
    },
    "style_spider_chaos": {
        "name": "蜘蛛ですが風（馬場翁）",
        "instruction": "【文体模倣: 意識流・独り言】\n1. 主人公の思考を「〜じゃね？」「ナイワ〜」といった軽い口語体で、絶え間なく垂れ流せ。\n2. 状況分析はゲーム的・数値的だが、それを茶化すようなノリで記述せよ。\n3. 視点変更（Sサイド、Kサイド等）を効果的に使い、主人公の客観的な恐ろしさを強調せよ。\n4. 鑑定結果やスキル説明を本文中に頻繁にインサートせよ。"
    },
    "style_vrmmo_introspection": {
        "name": "SAO風（川原礫）",
        "instruction": "【文体模倣: 仮想現実・内省】\n1. UI、HPバー、ポリゴン破砕エフェクトなどの「ゲーム的視覚情報」を、五感の一部として描写せよ。\n2. 戦闘中は「思考の加速」を描き、コンマ数秒の間に戦術を組み立てる思考プロセスを挿入せよ。\n3. ヒロインや相棒との精神的な繋がりを、少しセンチメンタルな筆致で描け。\n4. 「黒い剣士」「閃光」のような二つ名を効果的に使え。"
    },
    "style_bookworm_daily": {
        "name": "本好き風（香月美夜）",
        "instruction": "【文体模倣: 生活密着・文化的】\n1. 食事、掃除、服作りなどの「生活の細部」を、手順を追って丁寧に描写せよ。\n2. 貴族社会のしきたりや、平民との常識のギャップを、主人公の失敗を通じて描け。\n3. 家族や周囲の人々との温かい（あるいは厳しい）交流を物語の主軸に置け。\n4. 派手な魔法よりも、知識と工夫によるささやかな成功を喜ぶ描写をせよ。"
    },
    "style_action_heroic": {
        "name": "ダンまち風（大森藤ノ）",
        "instruction": "【文体模倣: 熱血・神話】\n1. 戦闘のクライマックスでは、情熱的で叙情的な表現（「英雄への階梯」「魂の輝き」）を使用せよ。\n2. オノマトペ（『ズドン！』『ギャアアア！』）を効果的に使い、迫力を出せ。\n3. 主人公が極限状態で立ち上がる様を、周囲の観衆の視点も交えてドラマチックに描け。\n4. 憧れや純粋な想いを原動力とする、直球の感情描写を行え。"
    },
    "style_otome_misunderstand": {
        "name": "はめふら風（山口悟）",
        "instruction": "【文体模倣: 脳内会議・鈍感】\n1. 主人公の脳内で、複数の人格（議長、弱気、強気など）が会議をする描写を入れろ。\n2. 破滅フラグを回避するために必死な行動が、周囲には「慈愛」として勘違いされる様子を描け。\n3. 恋愛感情には徹底的に鈍感で、全てを「友情」や「生存戦略」として解釈させろ。\n4. 雰囲気は明るく、ドロドロした展開もコミカルに消化せよ。"
    },
    "style_dark_hero": {
        "name": "ありふれ風（白米良）",
        "instruction": "【文体模倣: 徹底的暴力・デレ】\n1. 敵に対しては一切の慈悲を見せず、残酷かつ効率的に排除する描写を行え。\n2. 「――邪魔をするなら殺すだけだ」といった、断定的な強い言葉を使わせろ。\n3. 一転して、ヒロインたちとの会話は甘く、イチャイチャした雰囲気を隠すな。\n4. 戦闘力やステータスのインフレを、派手な演出とともに肯定的に描け。"
    },
    "style_average_gag": {
        "name": "平均値風（FUNA）",
        "instruction": "【文体模倣: メタ・パロディ】\n1. 「日本の古いテレビネタ」や「ネットミーム」を、異世界人が理解できない形でボケとして使え。\n2. トラブルは主人公のチート能力であっさり解決し、その後の「やったった感」を楽しげに描け。\n3. シリアスな空気になりかけたら、すぐにギャグで茶化して雰囲気をリセットせよ。\n4. 「ま、いっか！」というポジティブな諦観で物語を進めろ。"
    },
    "style_romcom_cynical": {
        "name": "俺ガイル風（渡航）",
        "instruction": "【文体模倣: 屈折・哲学的】\n1. 主人公の独白は、社会や青春に対する皮肉（ひねくれ）から入り、独自の理屈を展開せよ。\n2. 会話文は、言葉の裏にある「本音」や「空気」を探り合うような、緊張感のあるものにせよ。\n3. 比喩表現を多用し、抽象的な概念（「本物」「共依存」）について議論させろ。\n4. ラストは明確な答えを出さず、苦味を含んだ余韻を残せ。"
    },
    "style_chat_log": {
        "name": "掲示板・配信回風（一般的Web様式）",
        "instruction": "【文体模倣: 掲示板・レス】\n1. 文章ではなく、「名前：名無しさん」「>>1 おつ」のような掲示板形式で進行せよ。\n2. ネットスラング（草、ｗ、～杉、神）を多用し、独特のノリとスピード感を出せ。\n3. 主人公の行動に対する「視聴者の掌返し（批判→絶賛）」をリアルタイムで描け。\n4. 考察班、アンチ、信者など、複数の役割を持ったレスを混在させろ。"
    },
    "style_villainess_elegant": {
        "name": "悪役令嬢・宮廷風",
        "instruction": "【文体模倣: 優雅・耽美】\n1. 言葉遣いは「〜ですわ」「〜あそばせ」といった極めて丁寧な貴族言葉（役割語）を使用せよ。\n2. ドレスの素材、紅茶の香り、宝石の輝きなど、物理的な「美しさ」を詳細に描写せよ。\n3. 敵対者との会話は、笑顔の裏に侮蔑を込めるような、高度な皮肉の応酬にせよ。\n4. 恋愛感情は、激しさよりも「胸の痛み」「頬の熱」といった慎み深い表現を用いろ。"
    },
    "style_slow_life": {
        "name": "スローライフ風（一般的Web様式）",
        "instruction": "【文体模倣: 牧歌的・ストレスフリー】\n1. トラブルや敵対者は登場させず（あるいは即和解し）、平穏な日常を崩すな。\n2. 収穫した野菜の瑞々しさや、作った料理の美味しさを、擬音を交えて幸せそうに描け。\n3. もふもふした動物（聖獣）との触れ合いを、癒やしの要素として重点的に描写せよ。\n4. 「こういうのでいいんだよ」という、満ち足りた心情を強調せよ。"
    },
    "style_web_standard": {
        "name": "なろうテンプレ標準",
        "instruction": "【文体模倣: Web標準・高速】\n1. 1文は短く、難しい漢字は避けろ。改行を頻繁に入れ、スマホでの読みやすさを最優先せよ。\n2. 状況説明は省き、「鑑定」や「ステータス画面」の表示で情報を補完せよ。\n3. 主人公への称賛（「さすがです！」「ありえない！」）を周囲に語らせろ。\n4. テンポよく物語を進め、停滞する内面描写はカットせよ。"
    }
}

# ==========================================
# 2. Pydantic Schemas
# ==========================================
class SceneDetail(BaseModel):
    location: str = Field(..., description="シーンの場所")
    action: str = Field(..., description="シーン内で起きる具体的な出来事")
    dialogue_point: str = Field(..., description="主要な会話の内容")
    role: str = Field(..., description="シーンの役割（伏線/アクション/感情）")

class PlotEpisode(BaseModel):
    ep_num: int
    title: str
    setup: str
    conflict: str
    climax: str
    next_hook: str 
    tension: int
    stress: int = Field(default=0)
    catharsis: int = Field(default=0)
    scenes: List[SceneDetail]

class CharacterRegistry(BaseModel):
    name: str
    tone: str
    personality: str
    ability: str
    monologue_style: str
    pronouns: str 
    keyword_dictionary: str
    relations: str = Field(default="{}")

    def get_context_prompt(self) -> str:
        return f"""【CHARACTER REGISTRY】
■ {self.name} (主人公)
  - Tone: {self.tone}
  - Personality: {self.personality}
  - Ability: {self.ability}
  - Monologue Style: {self.monologue_style}
  - Pronouns: {self.pronouns}
  - Relations: {self.relations}
"""

class QualityReport(BaseModel):
    is_consistent: bool
    fatal_errors: List[str]
    consistency_score: int
    cliffhanger_score: int
    kakuyomu_appeal_score: int
    stress_level: int
    catharsis_level: int
    improvement_advice: str
    suggested_diff: str = Field(default="")

class MarketingAssets(BaseModel):
    catchcopies: List[str]
    tags: List[str]

class NovelStructure(BaseModel):
    title: str
    concept: str
    synopsis: str
    mc_profile: CharacterRegistry
    plots: List[PlotEpisode]
    marketing_assets: MarketingAssets

class Phase2Structure(BaseModel):
    plots: List[PlotEpisode]

class WorldState(BaseModel):
    settings: str = Field(default="{}")
    revealed: List[str] = Field(default_factory=list)
    revealed_mysteries: List[str] = Field(default_factory=list)
    pending_foreshadowing: List[str] = Field(default_factory=list)
    dependency_graph: str = Field(default="{}")

# ==========================================
# 3. Prompt Manager (Full Assets)
# ==========================================
class PromptManager:
    TEMPLATES = {
        "system_rules": """# SYSTEM RULES: STRICT ADHERENCE REQUIRED
【キャラクター定義の絶対遵守】
以下のキャラクター設定を物語の最後まで**固定**せよ。途中で口調や一人称を変更することは「重大なエラー」とみなす。

1. **主人公名**: {mc_name}
2. **基本口調**: 「{mc_tone}」
3. **性格特性**: {mc_personality}
4. **一人称・二人称**: {pronouns}
   ※「俺」設定なのに「僕」や「私」を使うことを固く禁ずる。
   ※相手への呼び方（お前、あんた、貴様など）も固定せよ。

5. [KEYWORD DICTIONARY] 以下の用語・ルビ・特殊呼称を必ず使用せよ: {keywords}
6. [MONOLOGUE STYLE] 独白・心理描写は以下の癖を反映せよ: {monologue_style}
   ※単なる状況説明ではなく、主人公のフィルターを通した『歪んだ世界観』として情景を記述せよ。
7. [RELATIONSHIPS] 現在の他者との関係性を口調や態度に反映せよ: {relations}
8. [NUMBERS] 金額・回数・ステータス値などの数量は「算用数字（1, 2, 100）」を使用し、四字熟語や慣用句（一石二鳥、百戦錬磨など）は「漢数字」を使用せよ。

【日本語作法・厳格なルール】
1. **三点リーダー**: 「……」と必ず2個（偶数個）セットで記述せよ。「…」や「...」は禁止。
2. **感嘆符・疑問符**: 「！」や「？」の直後には必ず全角スペースを1つ空けよ（文末の閉じ括弧直前を除く）。
   - OK: 「なんだと！？　ふざけるな！」
   - NG: 「なんだと!?ふざけるな!」
3. **改行の演出**: 
   - 場面転換や衝撃的な瞬間の前には、空白行を挟んで「溜め」を作れ。
   - セリフだけで進行せず、適度な改行でリズムを整えよ。

【文体指定: {style_name}】
{style_instruction}

【Gemma Writing Protocol】
You are Gemma, a creative AI writer operating in the highest tier of literary quality.
Your output must be emotional, logically consistent, and deeply immersive.
""",
        "writing_rules": """
【執筆プロトコル: 一括生成モード】
以下のルールを厳守し、1回の出力で物語の1エピソード（導入から結末まで）を完結させよ。

1. **出力文字数**:
   - 必ず **1,500文字〜2,000文字** の範囲に収めること。
   - 短すぎず、長すぎて出力が途切れないように調整せよ。

2. **構成（起承転結）**:
   - 1度の出力の中に「導入・展開・クライマックス・結末（引き）」の抑揚をつけよ。
   - **重要: 解決（Resolution）を禁止する。** 物語を安易に解決させず、必ず「Next Hook（次への引き）」で終わること。読者に「ここで終わるのか！？」という欠乏感を与えよ。

3. **密度**:
   - 「〜ということがあった」のようなあらすじ要約を厳禁とする。
   - 情景描写、五感、セリフ、内面描写を交え、読者が没入できる小説形式で記述せよ。
   - 会話文だけで進行させず、必ず地の文での状況描写を挟むこと。

4. **演出と強調（カクヨム記法）**:
   - **決め台詞（キラーフレーズ）の直前と直後には必ず空行を入れ、独立させよ。**
   - **重要な名詞やキーワードには、カクヨム記法の傍点 《《対象》》 を自ら付与せよ。**
""",
        "cliffhanger_protocol": """
【究極の「引き」生成ロジック: Cliffhanger Protocol】
各エピソードの結末は、文脈に応じて最も効果的な「引き」を自律的に判断し、**「読者が次を読まずにいられない状態」**を強制的に作り出せ。

1. **逆算式・ゴール地点固定**:
   - あなたは「結末の衝撃」から逆算して伏線を張る構成作家である。
   - 本文執筆前に、その話の**「最悪、あるいは最高の結末（最後の一行）」**を確定せよ。
   - 結末をぼかさないこと。予定調和な終わり方をしないこと。

2. **テンション・カタストロフィ**:
   - あなたは解決の1秒前に筆を置く、冷酷なディレクターである。
   - 絶体絶命の瞬間、あるいは秘密が暴かれる**「直前」で物語を強制終了**せよ。
   - 読者が「救い」や「納得」を得る記述を一切排除せよ。安心させず、解決しきらないこと。
""",
        "trend_analysis_prompt": """
あなたはカクヨム市場分析のプロフェッショナルです。
「カクヨムの週間ランキング上位100作品」の傾向を模倣し、**現在（2026年）Web小説で最もヒットする可能性が高い**「ジャンル・キーワード・設定」の組み合わせを一つ生成してください。

特に以下の4要素とトレンドの掛け合わせを優先すること:
1. **追放・ざまぁ**: 理不尽な追放からの圧倒的逆転
2. **現代ダンジョン**: 配信、探索、現実世界での無双
3. **悪役令嬢**: 断罪回避、内政、溺愛
4. **異世界転生**: チート能力、知識チート、スローライフ

JSON形式で以下のキーを含めて出力せよ:
- genre: ジャンル
- keywords: 3つの主要キーワード
- personality: 主人公の性格（詳細に）
- tone: 主人公の口調（一人称）
- hook_text: 読者を惹きつける「一行あらすじ」
- style: 最適な文体スタイルキー（STYLE_DEFINITIONSから選択）
""",
        "qa_evaluation_prompt": """
あなたはカクヨムランキング1位を目指すための総合検閲エンジンです。
以下のエピソード本文と「Bible（世界設定）」を照合し、**一回の推論で**品質レポートを作成せよ。

【Bible Settings】
{settings}

【Episode Content】
{content}

以下の項目を厳しく評価し、JSONで出力せよ:
1. **整合性(Consistency)**: 設定矛盾はないか？ (0-100)
2. **クリフハンガー(Cliffhanger)**: 続きを読ませる引きの強さ (0-100)。80点未満はリライト対象。
3. **カクヨム訴求力(Appeal)**: 「ざまぁ」「無双」「尊さ」など、カクヨム読者に刺さる要素の強さ (0-100)。
4. **ストレス/カタルシス**: 読者が感じるストレス度とカタルシス度を数値化せよ。
5. **書き換え提案(Suggested Diff)**: スコアが低い場合、または改善の余地がある場合、具体的にどの文章をどう書き換えるべきか、修正済みのテキスト案（差分）を提示せよ。

JSON出力形式:
{{
    "is_consistent": true/false,
    "fatal_errors": ["..."],
    "consistency_score": 80,
    "cliffhanger_score": 80,
    "kakuyomu_appeal_score": 70,
    "stress_level": 20,
    "catharsis_level": 80,
    "improvement_advice": "具体的な改善指示...",
    "suggested_diff": "【修正案】\\n原文「...」\\n↓\\n修正「...」\\n理由: ..."
}}
""",
        "plot_phase1": """
あなたはWeb小説の神級プロットアーキテクトです。
ジャンル「{genre}」で、カクヨム読者を熱狂させる**全50話完結の物語構造**を作成してください。

【ユーザー指定の絶対条件】
1. 文体: 「{style_name}」
2. 主人公: 性格{mc_personality}, 口調「{mc_tone}」
3. テーマ: {keywords}

【Task: Phase 1 (Ep 1-25)】
作品設定、前半パートである**第1話〜第25話**の詳細プロット、マーケティングアセットを作成せよ。
前半のクライマックス（第25話）に向けて、テンションを高めていくこと。
**重要: 各エピソードは「Resolution（解決）」ではなく「Next Hook（次への引き）」で終わらせる構成にせよ。**
**重要: Scenesフィールドは SceneDetail オブジェクトのリストとして定義すること。**

Output strictly in JSON format following this schema:
{schema}
""",
        "plot_phase2": """
あなたはWeb小説の神級プロットアーキテクトです。
現在、第25話まで執筆が完了しました。
最新の世界状態（Bible）と物語の展開に基づき、**第26話〜第50話（最終話）**のプロットを再構成してください。

【Recent Story Flow (Summaries)】
{history_summ}

【Task: Phase 2 (Ep 26-50)】
後半の展開を劇的に、かつ整合性が取れるように作成せよ。
**重要: 各エピソードは「Next Hook」で終わらせること。**
**重要: Scenesフィールドは SceneDetail オブジェクトのリストとして定義すること。**

Output strictly in JSON format following this schema:
{schema}
""",
        "episode_writer": """
{system_rules}
{entity_context}
{writing_rules}
{cliffhanger_protocol}

【Pacing Instruction】
{pacing_instruction}

【Role: Novelist (Gemma 3)】
以下のプロットに基づき、**第{ep_num}話**の本文を一括執筆し、結果をJSON形式で出力せよ。
1. `content`: 本文 (1500-2000文字)
2. `summary`: 次話へ繋ぐための要約
3. `next_world_state`: この話で確定した設定・変化した状態・解決した謎・新たな伏線を反映した最新のBible状態

【前話からの文脈】
{bible_context}

【今回のプロット】
{episode_plot_text}

【Rewrite Instruction (Chain of Thought)】
{rewrite_instruction}

Output Format:
```json
{{
    "content": "本文...",
    "summary": "要約...",
    "next_world_state": {{ ... }}
}}
""", "rewrite_critique": """ 【重要: 執筆やり直し命令】 先ほどの出力は品質基準を満たさなかったため、却下されました。 以下の「却下された原稿」と「品質保証レポート（Critique）」を熟読し、同じ過ちを犯さないように論理的に思考した上で、最高品質のエピソードを再執筆せよ。

[QA Critique]:

Improvement Advice: {improvement_advice}

Suggested Diff: {suggested_diff}

上記の指摘を反映し、特に「引きの強さ」と「カクヨム読者への訴求力」を飛躍的に高めた原稿を作成せよ。 """ }

def get(self, name, **kwargs):
    return self.TEMPLATES[name].format(**kwargs)
==========================================
4. Database & Utility
==========================================
class DatabaseManager: def init(self, db_path): self.db_path = db_path self.queue = asyncio.Queue()

async def start(self):
    asyncio.create_task(self._worker())
    await self._init_tables()

async def _init_tables(self):
    queries = [
        "CREATE TABLE IF NOT EXISTS books (id INTEGER PRIMARY KEY, title TEXT, genre TEXT, style_dna TEXT, marketing_data TEXT, synopsis TEXT, special_ability TEXT, concept TEXT, target_eps INTEGER, status TEXT, created_at TEXT)",
        "CREATE TABLE IF NOT EXISTS bible (id INTEGER PRIMARY KEY, book_id INTEGER, settings TEXT, version INTEGER, last_updated TEXT)",
        "CREATE TABLE IF NOT EXISTS plot (book_id INTEGER, ep_num INTEGER, title TEXT, setup TEXT, conflict TEXT, climax TEXT, resolution TEXT, scenes TEXT, tension INTEGER, stress INTEGER, catharsis INTEGER, status TEXT, main_event TEXT, PRIMARY KEY(book_id, ep_num))",
        "CREATE TABLE IF NOT EXISTS chapters (book_id INTEGER, ep_num INTEGER, title TEXT, content TEXT, summary TEXT, ai_insight TEXT, world_state TEXT, created_at TEXT, PRIMARY KEY(book_id, ep_num))",
        "CREATE TABLE IF NOT EXISTS characters (id INTEGER PRIMARY KEY, book_id INTEGER, name TEXT, role TEXT, registry_data TEXT, monologue_style TEXT)"
    ]
    for q in queries: await self.execute(q)

async def execute(self, query, params=()):
    future = asyncio.get_running_loop().create_future()
    await self.queue.put((query, params, future))
    return await future

async def fetch_one(self, query, params=()):
    def _fetch():
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return dict(row) if (row := conn.execute(query, params).fetchone()) else None
    return await asyncio.to_thread(_fetch)

async def fetch_all(self, query, params=()):
    def _fetch():
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query, params).fetchall()]
    return await asyncio.to_thread(_fetch)

async def _worker(self):
    conn = sqlite3.connect(self.db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    while True:
        query, params, future = await self.queue.get()
        try:
            cursor = conn.execute(query, params)
            if query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "REPLACE")):
                conn.commit()
                future.set_result(cursor.lastrowid)
            else:
                future.set_result(None)
        except Exception as e:
            future.set_exception(e)
        finally:
            self.queue.task_done()
db = DatabaseManager(DB_FILE)

==========================================
5. Core Classes (Gemini / Gemma Logic)
==========================================
class TrendAnalyst: """Gemma 3 (12B) を使用してトレンド分析""" def init(self, engine): self.engine = engine

async def get_dynamic_seed(self) -> dict:
    print(f"TrendAnalyst: Scanning Trends via {MODEL_FAST}...")
    prompt = self.engine.prompt_manager.get("trend_analysis_prompt")
    try:
        data = await self.engine._generate_json(MODEL_FAST, prompt)
        print(f"★ Trend Detected: {data.get('genre')} - {data.get('hook_text')}")
        return data
    except Exception as e:
        print(f"Trend Analysis Failed: {e}. Using fallback.")
        return {
            "genre": "現代ダンジョン", "keywords": "配信, 無双", 
            "personality": "冷静", "tone": "俺", "style": "style_web_standard", "hook_text": "配信切り忘れで世界最強"
        }
class QualityAssuranceEngine: """Gemma 3 (27B) を使用して品質チェック""" def init(self, engine): self.engine = engine

async def evaluate(self, content: str, settings_json: str) -> QualityReport:
    prompt = self.engine.prompt_manager.get(
        "qa_evaluation_prompt",
        settings=settings_json,
        content=content[:4000]
    )
    try:
        # 高性能な 27B モデルで検閲
        data = await self.engine._generate_json(MODEL_WRITER, prompt)
        return QualityReport.model_validate(data)
    except Exception as e:
        print(f"QA Failed: {e}")
        return QualityReport(
            is_consistent=True, fatal_errors=[], consistency_score=50,
            cliffhanger_score=50, kakuyomu_appeal_score=50, stress_level=0, catharsis_level=0,
            improvement_advice="QA Error", suggested_diff=""
        )
class UltraEngine: def init(self, api_key): self.client = genai.Client(api_key=api_key) self.prompt_manager = PromptManager() self.trend_analyst = TrendAnalyst(self) self.qa_engine = QualityAssuranceEngine(self)

async def _generate_with_retry(self, model, contents, config=None):
    retries = 0
    while retries < 5:
        try:
            return await self.client.aio.models.generate_content(
                model=model, contents=contents, config=config
            )
        except Exception as e:
            print(f"⚠️ API Error ({model}): {e}. Retrying...")
            await asyncio.sleep(2 + (2 ** retries))
            retries += 1
    raise Exception(f"Failed to generate with {model}")

async def _generate_json(self, model, prompt) -> dict:
    """Gemma 3 向けの強力なJSON抽出"""
    config = types.GenerateContentConfig(response_mime_type="application/json")
    res = await self._generate_with_retry(model, prompt, config)
    text = res.text.strip()
    
    # Markdown削除
    text = re.sub(r'^```json\s*', '', text, flags=re.MULTILINE)
    text = re.sub(r'```$', '', text, flags=re.MULTILINE)
    
    # RegexでJSONオブジェクトを抽出
    match = re.search(r'(\{.*\})', text, re.DOTALL)
    if match: text = match.group(1)
    
    # 制御文字削除
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)

    try:
        return json.loads(text)
    except:
        # 失敗時はテキスト全体を返す簡易救済
        if "content" not in text: # プロット生成などの場合
            raise ValueError(f"JSON Parse Failed: {text[:100]}...")
        return {"content": text, "summary": "Parse Error", "next_world_state": {}}

# ----------------------------------------------------------------
# 1. Plot Generation (GEMINI ONLY - 2 Calls Total)
# ----------------------------------------------------------------
async def generate_phase1_plot(self, seed: dict):
    """【API Call 1】Geminiによるプロット生成 (前半)"""
    print(f"Step 1: Plotting Phase 1 (Ep1-25) using {MODEL_ARCHITECT}...")
    
    schema = json.dumps(NovelStructure.model_json_schema(), ensure_ascii=False)
    style_def = STYLE_DEFINITIONS.get(seed['style'], STYLE_DEFINITIONS['style_web_standard'])
    
    prompt = self.prompt_manager.get(
        "plot_phase1",
        genre=seed['genre'], keywords=seed['keywords'],
        mc_personality=seed['personality'], mc_tone=seed['tone'],
        style_name=style_def['name'], schema=schema
    )
    
    data = await self._generate_json(MODEL_ARCHITECT, prompt)
    return NovelStructure.model_validate(data)

async def generate_phase2_plot(self, book_id: int):
    """【API Call 2】Geminiによるプロット生成 (後半)"""
    print(f"Step 3: Plotting Phase 2 (Ep26-50) using {MODEL_ARCHITECT}...")
    
    # コンテキスト収集
    summ_rows = await db.fetch_all("SELECT summary FROM chapters WHERE book_id=? ORDER BY ep_num DESC LIMIT 10", (book_id,))
    history_summ = "\n".join([f"- {r['summary']}" for r in summ_rows[::-1]])
    
    schema = json.dumps(Phase2Structure.model_json_schema(), ensure_ascii=False)
    prompt = self.prompt_manager.get(
        "plot_phase2",
        history_summ=history_summ, schema=schema
    )
    
    data = await self._generate_json(MODEL_ARCHITECT, prompt)
    return Phase2Structure.model_validate(data)

# ----------------------------------------------------------------
# 2. Episode Writing & QA Loop (GEMMA 3 ONLY) - PARALLEL MODE
# ----------------------------------------------------------------
async def write_episodes(self, book_id, plots, mc_profile, style_dna):
    """Gemma 3による執筆・検閲・リライトループ（並列処理版）"""
    
    # ★同時執筆数（推奨: 3〜5）
    semaphore = asyncio.Semaphore(5) 
    
    style_def = STYLE_DEFINITIONS.get(style_dna, STYLE_DEFINITIONS['style_web_standard'])
    
    # Registry構築
    registry = CharacterRegistry(**mc_profile)
    
    system_rules = self.prompt_manager.get(
        "system_rules",
        mc_name=registry.name, mc_tone=registry.tone,
        mc_personality=registry.personality, pronouns=registry.pronouns,
        keywords=registry.keyword_dictionary, monologue_style=registry.monologue_style,
        relations=registry.relations,
        style_name=style_def['name'], style_instruction=style_def['instruction']
    )
    entity_context = registry.get_context_prompt()

    async def _worker(plot):
        async with semaphore:
            ep_num = plot['ep_num']
            print(f"  > Gemma Writing Ep {ep_num}: {plot['title']} (Start)...")
            
            # コンテキスト取得
            # 並列時は最新の更新が間に合わない場合もあるが、DBからその時点での最新設定を取得
            bible = await db.fetch_one("SELECT settings FROM bible WHERE book_id=?", (book_id,))
            bible_ctx = bible['settings'] if bible else "{}"
            
            rewrite_instruction = "なし"
            max_retries = 2
            retry_count = 0
            
            while retry_count <= max_retries:
                # プロンプト構築
                prompt = self.prompt_manager.get(
                    "episode_writer",
                    system_rules=system_rules,
                    entity_context=entity_context,
                    writing_rules=self.prompt_manager.get("writing_rules"),
                    cliffhanger_protocol=self.prompt_manager.get("cliffhanger_protocol"),
                    pacing_instruction="Tension: High" if plot.get('tension', 50) > 70 else "Normal Pacing",
                    ep_num=ep_num,
                    episode_plot_text=json.dumps(plot, ensure_ascii=False),
                    bible_context=bible_ctx,
                    rewrite_instruction=rewrite_instruction
                )

                # 執筆実行 (Gemma 3 27B)
                try:
                    ep_data = await self._generate_json(MODEL_WRITER, prompt)
                    content = ep_data['content']
                    
                    # QA Check (Gemma 3 27B)
                    qa = await self.qa_engine.evaluate(content, bible_ctx)
                    
                    # 評価判定 (クリフハンガー or 訴求力が低い場合)
                    if (qa.cliffhanger_score < 70 or qa.kakuyomu_appeal_score < 70) and retry_count < max_retries:
                        print(f"    ⚠️ Low Quality Ep{ep_num} (Score: {qa.cliffhanger_score}). Rewriting...")
                        rewrite_instruction = self.prompt_manager.get(
                            "rewrite_critique",
                            improvement_advice=qa.improvement_advice,
                            suggested_diff=qa.suggested_diff
                        )
                        retry_count += 1
                        continue # ループ先頭へ
                    
                    # 成功: 保存
                    await db.execute(
                        "INSERT OR REPLACE INTO chapters (book_id, ep_num, title, content, summary, world_state, created_at) VALUES (?,?,?,?,?,?,?)",
                        (book_id, ep_num, plot['title'], content, ep_data.get('summary', ''), json.dumps(ep_data.get('next_world_state', {})), datetime.datetime.now().isoformat())
                    )
                    # Bible更新 (並列時は競合の可能性がありますが、最新の上書きを許容します)
                    if ep_data.get('next_world_state'):
                         await db.execute("UPDATE bible SET settings=? WHERE book_id=?", (json.dumps(ep_data['next_world_state'].get('settings', bible_ctx)), book_id))
                    
                    print(f"  ✅ Finished Ep {ep_num}")
                    break

                except Exception as e:
                    print(f"    ❌ Error Ep{ep_num}: {e}")
                    retry_count += 1

    # タスクを一括生成して並列実行
    tasks = [_worker(plot) for plot in plots]
    await asyncio.gather(*tasks)

# ----------------------------------------------------------------
# 3. Helpers
# ----------------------------------------------------------------
async def save_structure(self, structure: NovelStructure, genre: str, style: str):
    # Book保存
    bid = await db.execute(
        "INSERT INTO books (title, genre, style_dna, synopsis, marketing_data, created_at) VALUES (?,?,?,?,?,?)",
        (structure.title, genre, style, structure.synopsis, json.dumps(structure.marketing_assets.model_dump(), ensure_ascii=False), datetime.datetime.now().isoformat())
    )
    # キャラクター保存
    mc = structure.mc_profile
    await db.execute(
        "INSERT INTO characters (book_id, name, role, registry_data, monologue_style) VALUES (?,?,?,?,?)",
        (bid, mc.name, "主人公", json.dumps(mc.model_dump(), ensure_ascii=False), mc.monologue_style)
    )
    # 初期Bible保存
    await db.execute("INSERT INTO bible (book_id, settings, version) VALUES (?, ?, 0)", (bid, "{}"))
    # Plot保存
    await self.save_plots(bid, structure.plots)
    return bid

async def save_plots(self, book_id, plots: List[PlotEpisode]):
    for p in plots:
        await db.execute(
            "INSERT OR REPLACE INTO plot (book_id, ep_num, title, setup, conflict, climax, resolution, scenes, tension) VALUES (?,?,?,?,?,?,?,?,?)",
            (book_id, p.ep_num, p.title, p.setup, p.conflict, p.climax, p.next_hook, json.dumps([s.model_dump() for s in p.scenes], ensure_ascii=False), p.tension)
        )
==========================================
6. Packaging & Export
==========================================
async def create_zip_package(book_id, title): print("Packing ZIP...") buffer = io.BytesIO()

# DBから全データ取得
chapters = await db.fetch_all("SELECT * FROM chapters WHERE book_id=? ORDER BY ep_num", (book_id,))
book = await db.fetch_one("SELECT * FROM books WHERE id=?", (book_id,))
plots = await db.fetch_all("SELECT * FROM plot WHERE book_id=? ORDER BY ep_num", (book_id,))

with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
    # 設定ファイル
    info = f"タイトル: {title}\nあらすじ: {book['synopsis']}\n"
    z.writestr("00_作品情報.txt", info)
    
    # プロット
    plot_txt = "【全話プロット】\n\n"
    for p in plots:
        plot_txt += f"第{p['ep_num']}話: {p['title']}\nEvent: {p['main_event']}\n\n"
    z.writestr("00_プロット.txt", plot_txt)

    # 各話本文
    for ch in chapters:
        fname = f"chapters/{ch['ep_num']:02d}_{ch['title']}.txt"
        # 不正文字除去
        fname = re.sub(r'[\\/:*?"<>|]', '', fname)
        z.writestr(fname, ch['content'])

buffer.seek(0)
return buffer.getvalue()
def send_email(zip_data, title): if not GMAIL_USER or not GMAIL_PASS: print("Skipping Email: Credentials not found.") return

print(f"Sending Email to {TARGET_EMAIL}...")
msg = MIMEMultipart()
msg['Subject'] = f"【AI Novel Factory】{title} (Completed)"
msg['From'] = GMAIL_USER
msg['To'] = TARGET_EMAIL

part = MIMEBase('application', 'zip')
part.set_payload(zip_data)
encoders.encode_base64(part)
clean_title = re.sub(r'[\\/:*?"<>|]', '', title)
part.add_header('Content-Disposition', f'attachment; filename="{clean_title}.zip"')
msg.attach(part)

try:
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_PASS)
        server.send_message(msg)
    print("Email Sent Successfully!")
except Exception as e:
    print(f"Email Failed: {e}")
==========================================
Main Pipeline
==========================================
async def main(): if not API_KEY: print("Error: GEMINI_API_KEY is missing.") return

await db.start()
engine = UltraEngine(API_KEY)

print("=== AI Novel Factory (Gemini Architect + Gemma Writer) ===")

# 0. Trend Analysis (Gemma 12B)
seed = await engine.trend_analyst.get_dynamic_seed()

# 1. Phase 1 Plot (Gemini 2.0 Flash: Call 1/2)
struct_p1 = await engine.generate_phase1_plot(seed)
book_id = await engine.save_structure(struct_p1, seed['genre'], seed['style'])
print(f"Phase 1 Plots Saved. Book ID: {book_id}")

# 2. Phase 1 Writing (Gemma 27B Loop / Parallel)
mc_profile = struct_p1.mc_profile.model_dump()
plots_p1 = [p.model_dump() for p in struct_p1.plots]
await engine.write_episodes(book_id, plots_p1, mc_profile, seed['style'])

# 3. Phase 2 Plot (Gemini 2.0 Flash: Call 2/2)
# これ以降 Gemini は呼び出されない
struct_p2 = await engine.generate_phase2_plot(book_id)
await engine.save_plots(book_id, struct_p2.plots)
print(f"Phase 2 Plots Saved.")

# 4. Phase 2 Writing (Gemma 27B Loop / Parallel)
plots_p2 = [p.model_dump() for p in struct_p2.plots]
await engine.write_episodes(book_id, plots_p2, mc_profile, seed['style'])

# 5. Export
zip_bytes = await create_zip_package(book_id, struct_p1.title)
send_email(zip_bytes, struct_p1.title)

print("All processes completed.")
if name == "main": asyncio.run(main())