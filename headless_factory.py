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
import math
import asyncio
from typing import List, Optional, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from google import genai
from google.genai import types

# ==========================================
# 0. 設定 & 2026年仕様 (Headless / Embeddingなし)
# ==========================================
API_KEY = os.environ.get("GEMINI_API_KEY")
GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_PASS = os.environ.get("GMAIL_PASS")
TARGET_EMAIL = os.environ.get("GMAIL_USER")

# モデル設定 (2026年仕様: Gemma 3 Limits Optimized)
MODEL_ULTRALONG = "gemini-3-flash-preview"
MODEL_LITE = "gemma-3-12b-it"
MODEL_PRO = "gemma-3-27b-it" # QAと重要回で使用
MODEL_MICRO = "gemma-3-4b-it" # 文体校正用
MODEL_MARKETING = "gemini-2.5-flash-lite"

DB_FILE = "factory_run.db"

# ==========================================
# 文体定義 & サンプルデータ
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
    "style_trpg_hardboiled": {
        "name": "ゴブスレ風（蝸牛くも）",
        "instruction": "【文体模倣: ハードボイルド・即物的】\n1. 形容詞を削ぎ落とし、「彼は剣を振った。ゴブリンが死んだ。」のように事実を短文で積み重ねろ。\n2. 感情描写よりも、装備の点検や戦術の確認といった「プロフェッショナルな動作」を詳細に描け。\n3. 「運命（ダイス）」「神々」といったTRPG的な概念を、俯瞰的な視点として挿入せよ。\n4. グロテスクな描写も、日常の一部として淡々と記述せよ。"
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
# Pydantic Schemas (構造化出力用)
# ==========================================
class PlotScene(BaseModel):
    setup: str = Field(..., description="導入")
    conflict: str = Field(..., description="展開")
    climax: str = Field(..., description="結末")
    next_hook: str = Field(..., description="次への引き（クリフハンガー）。物語を解決させず、次を読ませるための新たな謎や危機")

class PlotEpisode(BaseModel):
    ep_num: int
    title: str
    setup: str
    conflict: str
    climax: str
    next_hook: str # Resolutionを廃止し、Next Hookへ変更
    tension: int
    scenes: List[str]

class CharacterRegistry(BaseModel):
    name: str
    tone: str
    personality: str
    ability: str
    monologue_style: str
    pronouns: str = Field(..., description="JSON string mapping keys (e.g., '一人称', '二人称') to values")
    keyword_dictionary: str = Field(..., description="JSON string mapping unique terms to their reading or definition")

    def to_dict(self):
        return self.model_dump()

    def get_context_prompt(self) -> str:
        p_json = {}
        try: p_json = json.loads(self.pronouns) if isinstance(self.pronouns, str) else self.pronouns
        except: pass
        
        k_json = {}
        try: k_json = json.loads(self.keyword_dictionary) if isinstance(self.keyword_dictionary, str) else self.keyword_dictionary
        except: pass

        prompt = "【CHARACTER REGISTRY】\n"
        prompt += f"■ {self.name} (主人公)\n"
        prompt += f"  - Tone: {self.tone}\n"
        prompt += f"  - Personality: {self.personality}\n"
        prompt += f"  - Ability: {self.ability}\n"
        prompt += f"  - Monologue Style: {self.monologue_style}\n"
        prompt += f"  - Pronouns: {json.dumps(p_json, ensure_ascii=False)}\n"
        return prompt

class NovelStructure(BaseModel):
    title: str
    concept: str
    synopsis: str
    mc_profile: CharacterRegistry
    plots: List[PlotEpisode]

class Phase2Structure(BaseModel):
    plots: List[PlotEpisode]

class WorldState(BaseModel):
    settings: str = Field(..., description="JSON string representing all world settings (Merged Immutable/Mutable)")
    revealed: List[str] = Field(default_factory=list, description="読者に開示済みの設定リスト")
    revealed_mysteries: List[str] = Field(default_factory=list, description="解明済みの伏線リスト")
    pending_foreshadowing: List[str] = Field(default_factory=list, description="未回収の伏線リスト")
    dependency_graph: str = Field(default="{}", description="JSON mapping of foreshadowing ID to target ep_num for resolution")

class EvaluationResult(BaseModel):
    is_consistent: bool = Field(..., description="設定矛盾がないか")
    fatal_errors: List[str] = Field(default_factory=list, description="致命的な矛盾")
    retention_score: int = Field(..., description="読者維持率予測スコア(0-100)")
    improvement_advice: str = Field(..., description="改善アドバイス")

class MarketingAssets(BaseModel):
    evaluations: List[EvaluationResult] # 統合された評価オブジェクトを使用（リスト形式で保持する場合は互換性のため）
    marketing_assets: str = Field(..., description="JSON string containing marketing assets like catchcopies and tags")

class EpisodeResponse(BaseModel):
    content: str = Field(..., description="エピソード本文 (1500-2000文字)")
    summary: str = Field(..., description="次話への文脈用要約 (300文字程度)")
    next_world_state: WorldState = Field(..., description="この話の結果更新された世界状態")
    cliffhanger_self_score: int = Field(..., description="結末の引きの強さを0-10点で自己採点せよ。7点未満はリライト対象。")

class TrendSeed(BaseModel):
    genre: str
    keywords: str
    personality: str
    tone: str
    hook_text: str
    style: str

# ==========================================
# プロンプト集約 (PROMPT_TEMPLATES)
# ==========================================
PROMPT_TEMPLATES = {
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
--------------------------------------------------
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
   
4. **自己評価 (Cliffhanger Score)**:
    - 生成した結末が、どれほど読者の興味を惹くかを「cliffhanger_self_score」として0-10点で厳しく採点せよ。
    - 7点未満（平凡な終わり方）の場合、システムは自動的にリライトを要求する。最初からクライマックスで終わるよう意識せよ。
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
"""
}

# ==========================================
# Formatter Class (LLM-based)
# ==========================================
class TextFormatter:
    def __init__(self, engine):
        self.engine = engine

    async def format(self, text, k_dict=None):
        if not text: return ""
        
        prompt = f"""
あなたはWeb小説専門の校正AIです。以下のテキストを、指定されたルールに従って校正してください。

【校正ルール】
1. **日本語作法**: 三点リーダー（……）、感嘆符・疑問符の後のスペース、閉じ括弧の位置などを出版基準で統一せよ。
2. **視点（POV）**: 原文の一人称/三人称の視点ブレがあれば、物語のメイン視点に合わせて修正せよ。
3. **ルビ振り**: 以下のキーワード辞書にある単語が初出の場合、必ず《ルビ》を振れ。
   辞書: {json.dumps(k_dict, ensure_ascii=False) if k_dict else "{}"}
4. **可読性**: 意味を変えずに、スマホで読みやすい改行位置に調整せよ。MarkdownやJSONタグは全て削除し、純粋なテキストのみ出力せよ。
5. **文体維持**: 元の文章の「味（文体）」を壊さないこと。

【対象テキスト】
{text[:10000]} # Limit context window if necessary
"""
        try:
            res = await self.engine._generate_with_retry(
                model=MODEL_MICRO,
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0.1) # Low temp for correction
            )
            return res.text.strip()
        except Exception as e:
            print(f"Formatter Error: {e}")
            return text # Fallback to raw text

# ==========================================
# 1. データベース管理
# ==========================================
class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.queue = asyncio.Queue()
        self._worker_task = None

    async def start(self):
        self._worker_task = asyncio.create_task(self._worker())
        await self._init_tables_async()

    async def _init_tables_async(self):
        await self.execute('''
                CREATE TABLE IF NOT EXISTS books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, genre TEXT, concept TEXT,
                    synopsis TEXT, catchcopy TEXT, target_eps INTEGER, style_dna TEXT,
                    target_audience TEXT, special_ability TEXT DEFAULT '',
                    status TEXT DEFAULT 'active', created_at TEXT, marketing_data TEXT, sub_plots TEXT
                );
            ''')
        # Bibleテーブル更新: version追加
        await self.execute('''
                CREATE TABLE IF NOT EXISTS bible (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, book_id INTEGER, 
                    settings TEXT, revealed TEXT,
                    revealed_mysteries TEXT, pending_foreshadowing TEXT,
                    dependency_graph TEXT,
                    version INTEGER DEFAULT 0,
                    last_updated TEXT
                );
            ''')
        await self.execute('''
                CREATE TABLE IF NOT EXISTS plot (
                    book_id INTEGER, ep_num INTEGER, title TEXT, summary TEXT,
                    main_event TEXT, sub_event TEXT, pacing_type TEXT,
                    tension INTEGER DEFAULT 50, cliffhanger_score INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'planned', 
                    setup TEXT, conflict TEXT, climax TEXT, resolution TEXT,
                    scenes TEXT,
                    PRIMARY KEY(book_id, ep_num)
                );
            ''')
        await self.execute('''
                CREATE TABLE IF NOT EXISTS chapters (
                    book_id INTEGER, ep_num INTEGER, title TEXT, content TEXT,
                    score_story INTEGER, killer_phrase TEXT, reader_retention_score INTEGER,
                    ending_emotion TEXT, discomfort_score INTEGER DEFAULT 0, tags TEXT,
                    ai_insight TEXT, retention_data TEXT, summary TEXT, world_state TEXT,
                    created_at TEXT, PRIMARY KEY(book_id, ep_num)
                );
            ''')
        await self.execute('''
                CREATE TABLE IF NOT EXISTS characters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, book_id INTEGER, name TEXT, role TEXT, registry_data TEXT, monologue_style TEXT
                );
            ''')

    async def execute(self, query, params=()):
        future = asyncio.get_running_loop().create_future()
        await self.queue.put((query, params, future))
        return await future

    async def _worker(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        while True:
            query, params, future = await self.queue.get()
            try:
                is_write = query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE", "REPLACE", "CREATE", "DROP", "ALTER"))
                cursor = conn.execute(query, params)
                if is_write:
                    conn.commit()
                    future.set_result(cursor.lastrowid)
                else:
                    future.set_result(None) 
            except Exception as e:
                future.set_exception(e)
            finally:
                self.queue.task_done()

    async def fetch_all(self, query, params=()):
        def _fetch():
            with sqlite3.connect(self.db_path, check_same_thread=False) as conn:
                conn.row_factory = sqlite3.Row
                return [dict(row) for row in conn.execute(query, params).fetchall()]
        return await asyncio.to_thread(_fetch)
            
    async def fetch_one(self, query, params=()):
        def _fetch():
            with sqlite3.connect(self.db_path, check_same_thread=False) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(query, params).fetchone()
                return dict(row) if row else None
        return await asyncio.to_thread(_fetch)

db = DatabaseManager(DB_FILE)

# ==========================================
# 2. Dynamic Bible Manager (Optimistic Locking)
# ==========================================
class DynamicBibleManager:
    def __init__(self, book_id):
        self.book_id = book_id
        self._cache = {} 
    
    async def get_current_state(self) -> (WorldState, int):
        # Always fetch fresh for locking context
        row = await db.fetch_one("SELECT * FROM bible WHERE book_id=? ORDER BY id DESC LIMIT 1", (self.book_id,))
        if not row:
            state = WorldState(settings="{}", revealed=[], revealed_mysteries=[], pending_foreshadowing=[], dependency_graph="{}")
            return state, 0
        try:
            state = WorldState(
                settings=row['settings'] if row['settings'] else "{}",
                revealed=json.loads(row['revealed']) if row['revealed'] else [],
                revealed_mysteries=json.loads(row['revealed_mysteries']) if row.get('revealed_mysteries') else [],
                pending_foreshadowing=json.loads(row['pending_foreshadowing']) if row.get('pending_foreshadowing') else [],
                dependency_graph=row['dependency_graph'] if row['dependency_graph'] else "{}"
            )
            return state, row.get('version', 0)
        except:
            state = WorldState(settings="{}", revealed=[], revealed_mysteries=[], pending_foreshadowing=[], dependency_graph="{}")
            return state, 0

    async def update_state(self, new_state: WorldState, expected_version: int):
        # Optimistic Locking Check
        current_head = await db.fetch_one("SELECT id, version FROM bible WHERE book_id=? ORDER BY id DESC LIMIT 1", (self.book_id,))
        
        current_ver = current_head['version'] if current_head else 0
        
        if current_ver != expected_version:
            # Conflict detected
            print(f"⚠️ Bible Conflict: Expected v{expected_version}, but found v{current_ver}. Merge logic required.")
            # In a real factory, we might merge. Here we fail or force update depending on policy.
            # For this code, we simply note it and append as new version based on *current* DB head to avoid corruption,
            # effectively 'Last Write Wins' but logically consistent sequence.
            # Ideally re-read and merge, but for now we increment from current.
            new_version = current_ver + 1
        else:
            new_version = expected_version + 1

        await db.execute(
            "INSERT INTO bible (book_id, settings, revealed, revealed_mysteries, pending_foreshadowing, dependency_graph, version, last_updated) VALUES (?,?,?,?,?,?,?,?)",
            (
                self.book_id,
                new_state.settings,      
                json.dumps(new_state.revealed, ensure_ascii=False),
                json.dumps(new_state.revealed_mysteries, ensure_ascii=False),
                json.dumps(new_state.pending_foreshadowing, ensure_ascii=False),
                new_state.dependency_graph,
                new_version,
                datetime.datetime.now().isoformat()
            )
        )

    async def get_prompt_context(self) -> str:
        state, ver = await self.get_current_state()
        return f"""
【WORLD STATE (Current v{ver})】
[SETTINGS]: {state.settings}
[REVEALED]: {json.dumps(state.revealed, ensure_ascii=False)}
[SOLVED MYSTERIES]: {json.dumps(state.revealed_mysteries, ensure_ascii=False)}
[PENDING FORESHADOWING]: {json.dumps(state.pending_foreshadowing, ensure_ascii=False)}
[DEPENDENCY GRAPH (Resolution Plan)]: {state.dependency_graph}
"""

# ==========================================
# 3. Adaptive Rate Limiter
# ==========================================
class AdaptiveRateLimiter:
    def __init__(self, initial_limit=5, min_limit=1):
        self.semaphore = asyncio.Semaphore(initial_limit)
        
    async def acquire(self):
        await self.semaphore.acquire()

    def release(self):
        self.semaphore.release()
    
    async def run_with_retry(self, func, *args, **kwargs):
        retries = 0
        max_retries = 5
        base_delay = 2.0
        
        while retries <= max_retries:
            await self.acquire()
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                error_str = str(e)
                if "429" in error_str or "ResourceExhausted" in error_str:
                    delay = (base_delay * (2 ** retries)) + random.uniform(0.1, 1.0) # Jitter
                    print(f"⚠️ Quota Exceeded. Retry {retries+1}/{max_retries} in {delay:.2f}s...")
                    await asyncio.sleep(delay)
                    retries += 1
                    if retries > max_retries:
                        raise e
                else:
                    raise e
            finally:
                self.release()

# ==========================================
# 4. New Classes (TrendAnalyst, QA, Pacing)
# ==========================================

class TrendAnalyst:
    def __init__(self, engine):
        self.engine = engine

    async def get_dynamic_seed(self) -> dict:
        print("TrendAnalyst: Scanning Global Trends via API...")
        # Since we cannot use actual Google Search API in this restricted env,
        # we utilize the model's grounding or internal knowledge to simulate 'Trend Extraction'.
        prompt = """
あなたは市場分析のプロフェッショナルです。
Google TrendやSNSのトレンドを分析したと仮定し、**現在（2026年）Web小説で最もヒットする可能性が高い**「ジャンル・キーワード・設定」の組み合わせを一つ生成してください。
既存のテンプレートに頼らず、意外性のあるフック（Hook）を考案すること。

JSON形式で以下のキーを含めて出力せよ:
- genre: ジャンル
- keywords: 3つの主要キーワード
- personality: 主人公の性格（詳細に）
- tone: 主人公の口調（一人称）
- hook_text: 読者を惹きつける「一行あらすじ」
- style: 最適な文体スタイルキー（以下のいずれか: style_serious_fantasy, style_psychological_loop, style_military_rational, style_magic_engineering, style_comedy_speed, style_overlord, style_slime_nation, style_spider_chaos, style_vrmmo_introspection, style_bookworm_daily, style_action_heroic, style_otome_misunderstand, style_dark_hero, style_average_gag, style_romcom_cynical, style_trpg_hardboiled, style_chat_log, style_villainess_elegant, style_slow_life, style_web_standard）
"""
        try:
            res = await self.engine._generate_with_retry(
                model=MODEL_MARKETING,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=TrendSeed
                )
            )
            seed = json.loads(res.text)
            print(f"★ Trend Detected: {seed['genre']} - {seed['hook_text']}")
            return seed
        except Exception as e:
            print(f"Trend Analysis Failed: {e}. Fallback to default.")
            return {
                "genre": "現代ダンジョン",
                "keywords": "配信, 事故, 無双",
                "personality": "冷静沈着",
                "tone": "俺",
                "hook_text": "配信切り忘れで世界最強がバレる",
                "style": "style_web_standard"
            }

class QualityAssuranceEngine:
    def __init__(self, engine):
        self.engine = engine

    async def evaluate(self, content: str, bible_manager: DynamicBibleManager) -> EvaluationResult:
        state, _ = await bible_manager.get_current_state()
        prompt = f"""
あなたは物語の品質保証（QA）エンジンです。
以下のエピソード本文と「Bible（世界設定）」を照合し、**一回の推論で**以下の2点を同時に評価せよ。

1. **論理的整合性 (Consistency)**:
   - 設定矛盾、死者の蘇生、能力違反はないか？
2. **読者維持率予測 (Retention)**:
   - このエピソードで読者が離脱する確率は？（0-100スコア、高いほど安全＝面白い）

【Bible Settings】
{state.settings}

【Episode Content】
{content[:4000]}...

JSON形式で出力せよ。
"""
        try:
            # Gemma 3 27B (MODEL_PRO) を使用してQAチェックを行うよう変更
            res = await self.engine._generate_with_retry(
                model=MODEL_PRO, 
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=EvaluationResult
                )
            )
            return EvaluationResult.model_validate_json(res.text)
        except Exception as e:
            print(f"QA Check Failed: {e}")
            return EvaluationResult(is_consistent=True, fatal_errors=[], retention_score=50, improvement_advice="Error in QA")

class PacingGraph:
    @staticmethod
    async def analyze(book_id: int, current_ep: int) -> Dict[str, Any]:
        # Fetch last 5 episodes' tension
        rows = await db.fetch_all(
            "SELECT tension FROM plot WHERE book_id=? AND ep_num < ? ORDER BY ep_num DESC LIMIT 5",
            (book_id, current_ep)
        )
        tensions = [r['tension'] for r in rows][::-1]
        
        # Default Logic
        if not tensions:
            return {"type": "normal", "temperature": 0.8, "instruction": "標準的なペースで進行せよ。"}
        
        avg_tension = sum(tensions) / len(tensions)
        
        if avg_tension > 80:
            return {
                "type": "climax_burnout", 
                "temperature": 0.85, 
                "instruction": "【Pacing: High-Speed】クライマックスの余韻、または畳み掛けるような展開。短文を多用し、スピード感を維持せよ。"
            }
        elif avg_tension < 40:
            return {
                "type": "slow_buildup",
                "temperature": 0.7,
                "instruction": "【Pacing: Slow-Life】溜めの回。日常描写や会話を丁寧に描き、次の波乱へのコントラストを作れ。"
            }
        else:
            return {
                "type": "normal",
                "temperature": 0.8,
                "instruction": "【Pacing: Standard】標準的な物語進行。情報の開示とアクションのバランスを取れ。"
            }

# ==========================================
# 5. ULTRA Engine (Autopilot)
# ==========================================
class UltraEngine:
    def __init__(self, api_key):
        self.client = genai.Client(api_key=api_key) if api_key else None
        self.rate_limiter = AdaptiveRateLimiter(initial_limit=5)
        self.safety_settings = [
            types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="BLOCK_NONE"),
        ]
        self.trend_analyst = TrendAnalyst(self)
        self.qa_engine = QualityAssuranceEngine(self)
        self.formatter = TextFormatter(self)

    def _generate_system_rules(self, char_registry: CharacterRegistry, style="style_web_standard"):
        style_def = STYLE_DEFINITIONS.get(style, STYLE_DEFINITIONS["style_web_standard"])
        
        return PROMPT_TEMPLATES["system_rules"].format(
            mc_name=char_registry.name,
            mc_tone=char_registry.tone,
            mc_personality=char_registry.personality,
            pronouns=char_registry.pronouns, 
            keywords=char_registry.keyword_dictionary, 
            monologue_style=char_registry.monologue_style,
            style_name=style_def["name"],
            style_instruction=style_def["instruction"]
        )

    async def _generate_with_retry(self, model, contents, config):
        async def _call():
            return await self.client.aio.models.generate_content(
                model=model, 
                contents=contents, 
                config=config
            )
        return await self.rate_limiter.run_with_retry(_call)

    # ---------------------------------------------------------
    # Core Logic
    # ---------------------------------------------------------

    async def generate_universe_blueprint_phase1(self, genre, style, mc_personality, mc_tone, keywords):
        """第1段階: 1-25話のプロット生成"""
        print("Step 1: Hyper-Resolution Plot Generation Phase 1 (Ep 1-25)...")
        
        style_name = STYLE_DEFINITIONS.get(style, {"name": style}).get("name")

        prompt = f"""
あなたはWeb小説の神級プロットアーキテクトです。
ジャンル「{genre}」で、読者を熱狂させる**全50話完結の物語構造**を作成してください。

【ユーザー指定の絶対条件】
1. 文体: 「{style_name}」
2. 主人公: 性格{mc_personality}, 口調「{mc_tone}」
3. テーマ: {keywords}

【Task: Phase 1 (Ep 1-25)】
作品設定と、前半パートである**第1話〜第25話**の詳細プロットを作成せよ。
前半のクライマックス（第25話）に向けて、テンションを高めていくこと。
**重要: 各エピソードは「Resolution（解決）」ではなく「Next Hook（次への引き）」で終わらせる構成にせよ。**

注: CharacterRegistry内の pronouns と keyword_dictionary は有効なJSON文字列として出力すること。
"""
        try:
            res = await self._generate_with_retry(
                model=MODEL_ULTRALONG,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=NovelStructure,
                    safety_settings=self.safety_settings
                )
            )
            data = json.loads(res.text)
            
            # Pydanticバリデーション前にデータ補正 (JSON文字列化)
            if 'mc_profile' in data:
                 if isinstance(data['mc_profile'].get('pronouns'), dict):
                     data['mc_profile']['pronouns'] = json.dumps(data['mc_profile']['pronouns'], ensure_ascii=False)
                 if isinstance(data['mc_profile'].get('keyword_dictionary'), dict):
                     data['mc_profile']['keyword_dictionary'] = json.dumps(data['mc_profile']['keyword_dictionary'], ensure_ascii=False)

            return data
        except Exception as e:
            print(f"Plot Phase 1 Error: {e}")
            return None

    async def regenerate_future_plots(self, book_id, current_ep=25):
        """第26話以降のプロットを動的に再構成する"""
        print(f"Regenerating Future Plots (Ep {current_ep+1}-50) based on current Bible state...")
        
        bible_manager = DynamicBibleManager(book_id)
        bible_context = await bible_manager.get_prompt_context()
        
        chapters = await db.fetch_all(f"SELECT summary FROM chapters WHERE book_id=? AND ep_num <= ? ORDER BY ep_num", (book_id, current_ep))
        history_summ = "\n".join([f"- {c['summary']}" for c in chapters[-5:]]) # 直近5話分のみ

        prompt = f"""
あなたはWeb小説の神級プロットアーキテクトです。
現在、第{current_ep}話まで執筆が完了しました。
最新の世界状態（Bible）と物語の展開に基づき、**第{current_ep+1}話〜第50話（最終話）**のプロットを再構成してください。

【Current Bible State】
{bible_context}

【Recent Story Flow】
{history_summ}

【Task】
後半の展開を劇的に、かつ整合性が取れるように作成せよ。
**重要: 各エピソードは「Next Hook」で終わらせること。**
"""
        try:
            res = await self._generate_with_retry(
                model=MODEL_ULTRALONG,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=Phase2Structure,
                    safety_settings=self.safety_settings
                )
            )
            return json.loads(res.text)
        except Exception as e:
            print(f"Regenerate Plots Error: {e}")
            return None

    # evaluate_consistency deleted and merged into QualityAssuranceEngine

    async def write_episodes(self, book_data, start_ep, end_ep, style_dna_str="style_web_standard", target_model=MODEL_LITE, rewrite_instruction=None, semaphore=None):
        """
        1エピソード1リクエスト化: 本文・要約・Bible更新を一括実行
        Cliffhangerスコアによる自動リライトループ搭載
        """
        all_plots = sorted(book_data['plots'], key=lambda x: x.get('ep_num', 999))
        target_plots = [p for p in all_plots if start_ep <= p.get('ep_num', -1) <= end_ep]
        if not target_plots: return None

        full_chapters = []
        bible_manager = DynamicBibleManager(book_data['book_id'])
        
        # CharacterRegistry 構築
        try:
            char_registry = CharacterRegistry(**book_data['mc_profile'])
        except:
            char_registry = CharacterRegistry(name="主人公", tone="標準", personality="", ability="", monologue_style="", pronouns="{}", keyword_dictionary="{}")
        
        # 前話の文脈取得
        prev_ep_row = await db.fetch_one("SELECT content, summary FROM chapters WHERE book_id=? AND ep_num=? ORDER BY ep_num DESC LIMIT 1", (book_data['book_id'], start_ep - 1))
        prev_context_text = prev_ep_row['content'][-500:] if prev_ep_row and prev_ep_row['content'] else "（物語開始）"

        system_rules = self._generate_system_rules(char_registry, style=style_dna_str)
        
        for plot in target_plots:
            ep_num = plot['ep_num']
            print(f"Hyper-Narrative Engine Writing Ep {ep_num} (Integrated Cliffhanger Mode)...")
            
            # Pacing Graph Analysis
            pacing_data = await PacingGraph.analyze(book_data['book_id'], ep_num)
            pacing_instruction = pacing_data['instruction']
            gen_temp = pacing_data['temperature']

            current_model = target_model
            if ep_num == 1 or ep_num == 50 or plot.get('tension', 50) >= 80:
                current_model = MODEL_PRO
            
            episode_plot_text = f"""
【Episode Title】{plot['title']}
【Setup】 {plot.get('setup', '')}
【Conflict】 {plot.get('conflict', '')}
【Climax】 {plot.get('climax', '')}
【Next Hook (No Resolution)】 {plot.get('next_hook', '')}
"""
            # Optimistic Lock: Get Version BEFORE writing
            world_state, expected_version = await bible_manager.get_current_state()
            bible_context = await bible_manager.get_prompt_context() # Context uses current state but prompt text
            
            entity_context = char_registry.get_context_prompt()

            # Dependency Graphから今回回収すべき伏線を抽出
            must_resolve = []
            try:
                dep_graph = json.loads(world_state.dependency_graph)
                for fs_id, target_ep in dep_graph.items():
                    if target_ep == ep_num:
                        must_resolve.append(fs_id)
            except: pass
            
            must_resolve_instruction = ""
            if must_resolve:
                must_resolve_instruction = f"\n【IMPORTANT: Fulfilling Foreshadowing】\n以下の伏線を本エピソードで必ず回収・言及せよ: {', '.join(must_resolve)}"

            # リライトループ用変数
            retry_count = 0
            max_retries = 2
            current_rewrite_instruction = rewrite_instruction

            async with semaphore:
                while retry_count <= max_retries:
                    write_prompt = f"""
{system_rules}
{entity_context}
{PROMPT_TEMPLATES["writing_rules"]}
{PROMPT_TEMPLATES["cliffhanger_protocol"]}

【Pacing Instruction】
{pacing_instruction}

【Role: Novelist ({current_model})】
以下のプロットに基づき、**第{ep_num}話**の本文を一括執筆し、結果をJSON形式で出力せよ。
1. `content`: 本文 (1500-2000文字)
2. `summary`: 次話へ繋ぐための要約
3. `next_world_state`: この話で確定した設定・変化した状態・解決した謎・新たな伏線を反映した最新のBible状態
4. `cliffhanger_self_score`: 結末の「引き」の強さ (0-10)

【Pending Foreshadowing (Priority)】
{json.dumps(world_state.pending_foreshadowing, ensure_ascii=False)}
{must_resolve_instruction}

【前話からの文脈】
...{prev_context_text}

【今回のプロット】
{episode_plot_text}

【World Context (Bible v{expected_version})】
{bible_context}

【Rewrite Instruction】
{current_rewrite_instruction if current_rewrite_instruction else "なし"}
"""
                    try:
                        res = await self._generate_with_retry(
                            model=current_model, 
                            contents=write_prompt,
                            config=types.GenerateContentConfig(
                                response_mime_type="application/json",
                                response_schema=EpisodeResponse,
                                temperature=gen_temp, # Pacing applied
                                safety_settings=self.safety_settings
                            )
                        )
                        ep_data = json.loads(res.text)
                        
                        score = ep_data.get('cliffhanger_self_score', 0)
                        
                        # Cliffhanger判定ループ
                        if score < 7 and retry_count < max_retries:
                            print(f"⚠️ Low Cliffhanger Score ({score}/10). Retrying Auto-Rewrite...")
                            current_rewrite_instruction = "【自己評価フィードバック】直前の出力は結末の引きが弱かった（スコア7未満）。解決させず、より衝撃的で、読者が絶望するか興奮するクリフハンガーで終わるよう、ラスト300文字を全面的に書き換えよ。"
                            retry_count += 1
                            continue # 再生成へ
                        
                        # 成功またはリトライ切れ
                        full_content = ep_data['content']
                        ep_summary = ep_data['summary']
                        
                        # Bible Sync (Optimistic Lock)
                        if 'next_world_state' in ep_data:
                            ns = ep_data['next_world_state']
                            new_state = WorldState(
                                settings=ns.get('settings', "{}"),
                                revealed=ns.get('revealed', []),
                                revealed_mysteries=ns.get('revealed_mysteries', []),
                                pending_foreshadowing=ns.get('pending_foreshadowing', []),
                                dependency_graph=ns.get('dependency_graph', "{}")
                            )
                            await bible_manager.update_state(new_state, expected_version)
                        
                        prev_context_text = f"（第{ep_num}話要約）{ep_summary}\n（直近の文）{full_content[-200:]}"

                        full_chapters.append({
                            "ep_num": ep_num,
                            "title": plot['title'],
                            "content": full_content,
                            "summary": ep_summary,
                            "world_state": ep_data.get('next_world_state', {})
                        })
                        break # ループ脱出

                    except Exception as e:
                        print(f"Writing Error Ep{ep_num}: {e}")
                        if retry_count == max_retries:
                            full_chapters.append({
                                "ep_num": ep_num,
                                "title": plot['title'],
                                "content": "（生成エラーが発生しました）",
                                "summary": "エラー",
                                "world_state": {}
                            })
                        retry_count += 1

        return {"chapters": full_chapters}

    async def analyze_and_create_assets(self, book_id):
        """Replaced with QA Engine Logic"""
        print("Running Quality Assurance & Retention Analysis...")
        
        # chaptersテーブルから取得
        chapters = await db.fetch_all("SELECT ep_num, title, summary, content FROM chapters WHERE book_id=? ORDER BY ep_num", (book_id,))
        if not chapters: return [], [], None

        qa_results = []
        rewrite_targets = []
        bible_manager = DynamicBibleManager(book_id)

        # 1. Per-Episode QA Check
        for ch in chapters:
            res = await self.qa_engine.evaluate(ch['content'], bible_manager)
            qa_results.append({
                "ep_num": ch['ep_num'],
                "total_score": res.retention_score, # Mapping for compatibility
                "improvement_point": res.improvement_advice,
                "retention_score": res.retention_score
            })
            if not res.is_consistent or res.retention_score < 60:
                rewrite_targets.append(ch['ep_num'])

        # 2. Marketing Assets Generation (Simple Prompt)
        prompt = f"以下の物語要約に基づき、マーケティング用アセット（JSON）を作成せよ。\n" + "\n".join([f"Ep{c['ep_num']}: {c['summary']}" for c in chapters])
        # ... (simplified call to model for catchcopies etc.)
        # For brevity, reusing old schema structure filling logic
        try:
             res = await self._generate_with_retry(
                model=MODEL_MARKETING,
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=MarketingAssets)
             )
             m_data = MarketingAssets.model_validate_json(res.text)
             marketing_assets_dict = {}
             try: marketing_assets_dict = json.loads(m_data.marketing_assets)
             except: pass
             return qa_results, rewrite_targets, marketing_assets_dict
        except:
             return qa_results, rewrite_targets, {}

    async def rewrite_target_episodes(self, book_data, target_ep_ids, evaluations, style_dna_str="style_web_standard"):
        """リライト処理 - Uses QA Engine within write loop via instruction"""
        rewritten_count = 0
        semaphore = asyncio.Semaphore(1) 
        eval_map = {e['ep_num']: e for e in evaluations}
        tasks = []
        
        for ep_id in target_ep_ids:
            eval_data = eval_map.get(ep_id, {})
            instruction = f"【品質保証(QA)からの修正命令】\n{eval_data.get('improvement_point', '')}"
            
            tasks.append(self.write_episodes(
                book_data, ep_id, ep_id, 
                style_dna_str=style_dna_str, 
                target_model=MODEL_PRO, 
                rewrite_instruction=instruction,
                semaphore=semaphore
            ))
            
        results = await asyncio.gather(*tasks)
        for res in results:
            if res and 'chapters' in res:
                await self.save_chapters_to_db(book_data['book_id'], res['chapters'])
                rewritten_count += 1
        return rewritten_count

    async def save_blueprint_to_db(self, data, genre, style_dna_str):
        if isinstance(data, dict): data_dict = data
        else: data_dict = data.model_dump()
        
        dna = json.dumps({
            "tone": data_dict['mc_profile']['tone'], 
            "personality": data_dict['mc_profile'].get('personality', ''),
            "style_mode": style_dna_str,
            "pov_type": "一人称"
        }, ensure_ascii=False)
        
        ability_val = data_dict['mc_profile'].get('ability', '')
        
        # target_eps を 50 に設定
        bid = await db.execute(
            "INSERT INTO books (title, genre, synopsis, concept, target_eps, style_dna, status, special_ability, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (data_dict['title'], genre, data_dict['synopsis'], data_dict['concept'], 50, dna, 'active', ability_val, datetime.datetime.now().isoformat())
        )
        
        # CharacterRegistry を使用して保存
        registry_json = json.dumps(data_dict['mc_profile'], ensure_ascii=False)
        monologue_val = data_dict['mc_profile'].get('monologue_style', '')
        
        await db.execute("INSERT INTO characters (book_id, name, role, registry_data, monologue_style) VALUES (?,?,?,?,?)", 
                         (bid, data_dict['mc_profile']['name'], '主人公', registry_json, monologue_val))
        
        await db.execute("INSERT INTO bible (book_id, settings, revealed, revealed_mysteries, pending_foreshadowing, dependency_graph, version, last_updated) VALUES (?,?,?,?,?,?,?,?)",
                    (bid, "{}", "[]", "[]", "[]", "{}", 0, datetime.datetime.now().isoformat()))

        saved_plots = []
        for p in data_dict['plots']:
            full_title = f"第{p['ep_num']}話 {p['title']}"
            main_ev = f"{p.get('setup','')}->{p.get('climax','')}"
            scenes_json = json.dumps(p.get('scenes', []), ensure_ascii=False)
            await db.execute(
                """INSERT INTO plot (book_id, ep_num, title, main_event, setup, conflict, climax, resolution, tension, status, scenes)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (bid, p['ep_num'], full_title, main_ev, 
                 p.get('setup'), p.get('conflict'), p.get('climax'), p.get('next_hook'), # Store Next Hook in resolution col for schema compat or use new col
                 p.get('tension', 50), 'planned', scenes_json)
            )
            saved_plots.append(p)
        return bid, saved_plots

    async def save_additional_plots_to_db(self, book_id, data_p2):
        saved_plots = []
        for p in data_p2['plots']:
            full_title = f"第{p['ep_num']}話 {p['title']}"
            main_ev = f"{p.get('setup','')}->{p.get('climax','')}"
            scenes_json = json.dumps(p.get('scenes', []), ensure_ascii=False)
            await db.execute(
                """INSERT INTO plot (book_id, ep_num, title, main_event, setup, conflict, climax, resolution, tension, status, scenes)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (book_id, p['ep_num'], full_title, main_ev, 
                 p.get('setup'), p.get('conflict'), p.get('climax'), p.get('next_hook'), 
                 p.get('tension', 50), 'planned', scenes_json)
            )
            saved_plots.append(p)
        return saved_plots

    async def save_chapters_to_db(self, book_id, chapters_list):
        count = 0
        if not chapters_list: return 0
        
        # Fetch keywords for formatting
        mc = await db.fetch_one("SELECT registry_data FROM characters WHERE book_id=? AND role='主人公'", (book_id,))
        k_dict = {}
        if mc and mc['registry_data']:
             try: 
                 reg = json.loads(mc['registry_data'])
                 k_str = reg.get('keyword_dictionary', '{}')
                 k_dict = json.loads(k_str) if isinstance(k_str, str) else k_str
             except: pass

        for ch in chapters_list:
            # New LLM-based Formatter
            content = await self.formatter.format(ch['content'], k_dict=k_dict)
            w_state = json.dumps(ch.get('world_state', {}), ensure_ascii=False) if ch.get('world_state') else ""
            await db.execute(
                """INSERT OR REPLACE INTO chapters (book_id, ep_num, title, content, summary, ai_insight, world_state, created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (book_id, ch['ep_num'], ch.get('title', f"第{ch['ep_num']}話"), content, ch.get('summary', ''), '', w_state, datetime.datetime.now().isoformat())
            )
            await db.execute("UPDATE plot SET status='completed' WHERE book_id=? AND ep_num=?", (book_id, ch['ep_num']))
            count += 1
        return count

# ==========================================
# Task Functions
# ==========================================
async def task_write_batch(engine, bid, start_ep, end_ep):
    book_info = await db.fetch_one("SELECT * FROM books WHERE id=?", (bid,))
    plots = await db.fetch_all("SELECT * FROM plot WHERE book_id=? ORDER BY ep_num", (bid,))
    mc = await db.fetch_one("SELECT * FROM characters WHERE book_id=? AND role='主人公'", (bid,))

    try:
        style_dna_json = json.loads(book_info['style_dna'])
        saved_style = style_dna_json.get('style_mode', 'style_web_standard')
    except:
        saved_style = 'style_web_standard'
    
    # Retrieve Profile from Registry Data
    if mc and mc['registry_data']:
        try:
            mc_profile = json.loads(mc['registry_data'])
        except:
             mc_profile = {"name":"主人公", "tone":"標準", "personality":"", "ability":"", "monologue_style":"", "pronouns":"{}", "keyword_dictionary":"{}"}
    else:
        mc_profile = {"name":"主人公", "tone":"標準", "personality":"", "ability":"", "monologue_style":"", "pronouns":"{}", "keyword_dictionary":"{}"}

    for p in plots:
        if p.get('scenes'):
            try: p['scenes'] = json.loads(p['scenes'])
            except: pass
        # Map DB 'resolution' col to 'next_hook' for logical use
        if 'resolution' in p:
             p['next_hook'] = p['resolution']

    full_data = {"book_id": bid, "title": book_info['title'], "mc_profile": mc_profile, "plots": [dict(p) for p in plots]}
    
    semaphore = asyncio.Semaphore(3) 

    tasks = []
    print(f"Starting Machine-Gun Parallel Writing (Ep {start_ep} - {end_ep})...")

    target_plots = [p for p in plots if start_ep <= p['ep_num'] <= end_ep]

    for p in target_plots:
        ep_num = p['ep_num']
        
        tasks.append(engine.write_episodes(
            full_data, 
            ep_num, 
            ep_num, 
            style_dna_str=saved_style, 
            target_model=MODEL_LITE, 
            semaphore=semaphore
        ))

    results = await asyncio.gather(*tasks)

    total_count = 0
    for res_data in results:
        if res_data and 'chapters' in res_data:
            c = await engine.save_chapters_to_db(bid, res_data['chapters'])
            total_count += c
            
    print(f"Batch Done (Ep {start_ep}-{end_ep}). Total Episodes Written: {total_count}")
    return total_count, full_data, saved_style

async def task_analyze_marketing(engine, bid):
    print("Analyzing & Creating Marketing Assets...")
    evals, rewrite_targets, assets = await engine.analyze_and_create_assets(bid)
    return evals, rewrite_targets, assets

async def task_rewrite(engine, full_data, rewrite_targets, evals, saved_style):
    if not rewrite_targets: return 0
    print(f"Rewriting {len(rewrite_targets)} Episodes (Consistency & Quality Check)...")
    c = await engine.rewrite_target_episodes(full_data, rewrite_targets, evals, style_dna_str=saved_style)
    return c

# ==========================================
# 3. Main Logic
# ==========================================
# load_seed Removed and replaced by TrendAnalyst logic in Main

async def create_zip_package(book_id, title, marketing_data):
    print("Packing ZIP...")
    buffer = io.BytesIO()

    current_book = await db.fetch_one("SELECT * FROM books WHERE id=?", (book_id,))
    db_chars = await db.fetch_all("SELECT * FROM characters WHERE book_id=?", (book_id,))
    db_plots = await db.fetch_all("SELECT * FROM plot WHERE book_id=? ORDER BY ep_num", (book_id,))
    chapters = await db.fetch_all("SELECT * FROM chapters WHERE book_id=? ORDER BY ep_num", (book_id,))

    def clean_filename_title(t):
        return re.sub(r'[\\/:*?"<>|]', '', re.sub(r'^第\d+話[\s　]*', '', t)).strip()

    keyword_dict = {}
    mc_char = next((c for c in db_chars if c['role'] == '主人公'), None)
    if mc_char:
        try:
            reg_data = json.loads(mc_char['registry_data'])
            if reg_data:
                k_str = reg_data.get('keyword_dictionary', '{}')
                keyword_dict = json.loads(k_str) if isinstance(k_str, str) else k_str
        except: pass

    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        reg_info = f"【タイトル】\n{title}\n\n【あらすじ】\n{current_book.get('synopsis', '')}\n"
        z.writestr("00_作品登録用データ.txt", reg_info)

        setting_txt = f"【世界観・特殊能力設定】\n{current_book.get('special_ability', 'なし')}\n\n"
        setting_txt += "【キャラクター設定】\n"
        for char in db_chars:
            setting_txt += f"■ {char['name']} ({char['role']})\n"
            if char.get('monologue_style'):
                setting_txt += f"  - モノローグ癖: {char['monologue_style']}\n"
            try:
                setting_txt += f"  - Registry Data: {char['registry_data']}\n"
            except:
                setting_txt += "\n"
            setting_txt += "\n"
        z.writestr("00_キャラクター・世界観設定資料.txt", setting_txt)

        plot_txt = f"【タイトル】{title}\n【全話プロット構成案】\n\n"
        for p in db_plots:
            plot_txt += f"--------------------------------------------------\n"
            plot_txt += f"第{p['ep_num']}話：{p['title']}\n"
            plot_txt += f"--------------------------------------------------\n"
            plot_txt += f"・メインイベント: {p.get('main_event', '')}\n"
            plot_txt += f"・導入 (Setup): {p.get('setup', '')}\n"
            plot_txt += f"・展開 (Conflict): {p.get('conflict', '')}\n"
            plot_txt += f"・見せ場 (Climax): {p.get('climax', '')}\n"
            plot_txt += f"・引き (Next Hook): {p.get('resolution', '')}\n" # Note: mapped to Resolution col
            plot_txt += f"・テンション: {p.get('tension', '-')}/100\n\n"
        z.writestr("00_全話プロット構成案.txt", plot_txt)

        for ch in chapters:
            clean_title = clean_filename_title(ch['title'])
            fname = f"chapters/{ch['ep_num']:02d}_{clean_title}.txt"
            # Formatter already applied on save, just raw dump
            z.writestr(fname, ch['content'])
        
        if marketing_data:
            meta = f"【タイトル】\n{title}\n\n"
            meta += f"【キャッチコピー】\n" + "\n".join(marketing_data.get('catchcopies', [])) + "\n\n"
            meta += f"【検索タグ】\n{' '.join(marketing_data.get('tags', []))}\n\n"
            z.writestr("marketing_assets.txt", meta)
            
            try:
                z.writestr("marketing_raw.json", json.dumps(marketing_data, ensure_ascii=False))
            except: pass

    buffer.seek(0)
    return buffer.getvalue()

def send_email(zip_data, title):
    if not GMAIL_USER or not GMAIL_PASS:
        print("Skipping Email: Credentials not found.")
        return

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

async def main():
    if not API_KEY:
        print("Error: GEMINI_API_KEY is missing.")
        return

    await db.start() 
    engine = UltraEngine(API_KEY)

    print("Starting Factory Pipeline (Async / One-Shot Mode)...")

    try:
        # Step 0: Trend Analysis (Replaced Load Seed)
        seed = await engine.trend_analyst.get_dynamic_seed()
        
        # Step 1: 1-25話プロット
        print("Step 1a: Generating Plot Phase 1 (Ep 1-25)...")
        data1 = await engine.generate_universe_blueprint_phase1(
            seed['genre'], seed['style'], seed['personality'], seed['tone'], seed['keywords']
        )
        
        if not data1: 
            print("Plot Gen Phase 1 failed.")
            return

        bid, plots_p1 = await engine.save_blueprint_to_db(data1, seed['genre'], seed['style'])
        print(f"Phase 1 Saved. ID: {bid}")
        
        print("Step 2: Execution - Phase 1 Writing (Ep 1-25)...")
        
        # Phase 1 執筆 (1-25話)
        count_p1, full_data_p1, saved_style = await task_write_batch(engine, bid, start_ep=1, end_ep=25)
        
        # Phase 2 プロット生成 (再生性)
        print("Step 3: Regenerating Future Plots (Ep 26-50)...")
        data2 = await engine.regenerate_future_plots(bid, current_ep=25)
        if data2 and 'plots' in data2:
            saved_plots_p2 = await engine.save_additional_plots_to_db(bid, data2)
            print(f"Phase 2 Plots Saved ({len(saved_plots_p2)} eps).")
        else:
            print("Phase 2 Generation Failed.")
        
        print("Step 4: Execution - Phase 2 Writing (Ep 26-50)...")

        # Phase 2 執筆 (26-50話)
        count_p2, full_data_final, _ = await task_write_batch(engine, bid, start_ep=26, end_ep=50)
        
        full_data = full_data_final 

        evals, rewrite_targets, assets = await task_analyze_marketing(engine, bid)
        print(f"Rewriting Targets (Consistency & Low Score): {rewrite_targets}")

        if rewrite_targets:
            await task_rewrite(engine, full_data, rewrite_targets, evals, saved_style)

        book_info = await db.fetch_one("SELECT title FROM books WHERE id=?", (bid,))
        title = book_info['title']
        
        zip_bytes = await create_zip_package(bid, title, assets)
        send_email(zip_bytes, title)
        print(f"Mission Complete: {title}. System shutting down.")
        
    except Exception as e:
        print(f"Pipeline Critical Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())