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
from typing import List, Optional, Dict, Any, Type, Union
from enum import Enum
from pydantic import BaseModel, Field, ValidationError
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

# モデル設定
MODEL_ULTRALONG = "gemini-3-flash-preview"
MODEL_LITE = "gemma-3-12b-it"
MODEL_PRO = "gemma-3-27b-it" 
MODEL_MARKETING = "gemini-2.5-flash-lite"

DB_FILE = "factory_run.db"

# ==========================================
# 文体定義 & サンプルデータ (Few-Shot形式に変更)
# ==========================================
STYLE_DEFINITIONS = {
    "style_serious_fantasy": {
        "name": "無職転生風（理不尽な孫の手）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「俺は、ただ生きたかっただけなのかもしれない。前世の記憶、あのゴミのような日々。だが、この世界なら……。土の匂いがする。泥にまみれた手が、妙にリアルだった。」\n\n【指針】\n上記のように、回想的かつ内省的なトーンで記述せよ。五感を通じた生活感を重視し、弱さを隠さない客観的な心理描写を行え。"
    },
    "style_psychological_loop": {
        "name": "リゼロ風（長月達平）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「――ッ！　熱い、熱い熱い熱い！　喉が、焼けるように……ぁ、あ……。死にたくない、死にたくない死にたくない！　『愛してる』だと……？　ふざけるな、ふざけるなよぉぉぉ！」\n\n【指針】\n上記のように、呼吸音や絶叫を交え、切迫した心理を畳み掛けろ。自己嫌悪と他者への執着を、粘着質に繰り返せ。"
    },
    "style_military_rational": {
        "name": "幼女戦記風（カルロ・ゼン）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「合理的判断に基づけば、この損耗率は許容範囲内だ。存在Xの悪意など、物理法則の前では無意味に等しい。当大隊は直ちに敵左翼を包囲、殲滅戦へ移行する。これは戦争ではない、駆除作業だ。」\n\n【指針】\n上記のように、感情を排した報告書的な文体を維持せよ。硬質な語彙（熟語、カタカナ語）を用い、徹底的な合理主義を貫け。"
    },
    "style_magic_engineering": {
        "name": "魔法科風（佐島勤）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「CADが起動シークエンスに入る。事象改変速度、0.05秒。術式解凍、展開。対象の座標情報を上書きし、分解魔法を発動。物理的な破壊ではなく、情報の消去。彼は表情一つ変えず、ただ『プロセス完了』と呟いた。」\n\n【指針】\n上記のように、魔法を技術体系として論理的に解説せよ。感情よりもメカニズムを優先し、整然とした説明文のような文体で記述せよ。"
    },
    "style_comedy_speed": {
        "name": "このすば風（暁なつめ）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「おい待て、それを撃つな！　バカかお前は！」「ふっ、我が爆裂魔法の餌食になりたいようね……」「なりません！　カエルだぞ相手は！　ヌルヌルしてんだぞ！」\n\n【指針】\n上記のように、地の文を最小限にし、テンポの良い会話劇で進行せよ。シリアスは即座にギャグで台無しにし、鋭いツッコミを入れろ。"
    },
    "style_overlord": {
        "name": "オバロ風（丸山くがね）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「流石はアインズ様……！　その深淵なる御考え、このデミウルゴス、感服いたしました」「（えっ、何それ……適当に言っただけなんだけど）」\n\n【指針】\n上記のように、絶対支配者としての外面と、小心者の内面のギャップを描け。配下の過剰な崇拝と、残酷な蹂躙描写を重厚に記述せよ。"
    },
    "style_slime_nation": {
        "name": "転スラ風（伏瀬）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「《告。個体名リムルがスキル『暴食之王』を獲得しました》　……なんか凄そうなの手に入れたな。まあいいか、これで皆を守れるなら。会議を始めよう。『主君、この件ですが――』」\n\n【指針】\n上記のように、スキル獲得ログを明示し、楽観的なトーンで進行せよ。会議シーンを多用し、部下からの全肯定と親しみやすさを描け。"
    },
    "style_spider_chaos": {
        "name": "蜘蛛ですが風（馬場翁）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「ナイワ〜。マジでないわ〜。このステータスで勝てと？　無理ゲーじゃん！　……鑑定先生、出番ですよ。ふむふむ、弱点は火、と。なら燃やすしかなくね？」\n\n【指針】\n上記のように、女子高生風の軽い口語体での脳内独り言を垂れ流せ。数値的分析を茶化しながら行い、意識の流れをそのまま文章化せよ。"
    },
    "style_vrmmo_introspection": {
        "name": "SAO風（川原礫）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「右手の剣が青い光跡を描く。システムアシストではない、俺自身の反射神経。コンマ1秒の思考加速。あいつを守る、その想いだけで、俺は仮想世界の物理法則を超越した。」\n\n【指針】\n上記のように、UIやエフェクト等のゲーム的情報を五感として描け。戦闘中の高速思考と、センチメンタルな決意を交錯させよ。"
    },
    "style_bookworm_daily": {
        "name": "本好き風（香月美夜）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「今日は紙漉きの日です！　繊維を煮込んで、ドロドロにして……うぅ、臭い。でも、本のためなら我慢です。ルッツ、そこもっと強く絞って！」「はいはい、わかったよマイン」\n\n【指針】\n上記のように、生活の細部や作業工程を丁寧に描写せよ。周囲との温かい交流と、本（目的）への異常な執着を対比させよ。"
    },
    "style_action_heroic": {
        "name": "ダンまち風（大森藤ノ）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「――舞え！　『ファイアボルト』！！　ドゴォォォン！！　爆炎を突き破り、僕は走る。憧れに、届くために。英雄になりたいと、魂が叫んでいるんだ！」\n\n【指針】\n上記のように、熱血で叙情的な表現と、迫力あるオノマトペを多用せよ。純粋な憧れを原動力とした、直球の英雄譚を描け。"
    },
    "style_otome_misunderstand": {
        "name": "はめふら風（山口悟）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「（よし、土いじりで足腰を鍛えて、追放エンドに備えるのよ！）『カタリナ様、なんて慈悲深い……お庭の手入れまでご自身で……』　あれ？　また何か勘違いされてる？」\n\n【指針】\n上記のように、脳内会議と行動のズレによる勘違いを描け。破滅回避のための奇行が、周囲には善行として解釈される様をコミカルに記述せよ。"
    },
    "style_dark_hero": {
        "name": "ありふれ風（白米良）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「……殺すぞ？　俺の邪魔をするなら、神だろうが殺す。それだけだ。ユエ、行くぞ」「……ん。ハジメ、大好き」\n\n【指針】\n上記のように、敵には容赦ない断定的な暴力性を、身内には甘いデレを見せろ。厨二病的なカッコよさとステータス無双を肯定的に描け。"
    },
    "style_average_gag": {
        "name": "平均値風（FUNA）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「それなんてエロゲ？　……って、通じないか異世界だもんね！　『古き契約に従い……』って長ーい！　ドーン！　はい、終了！　あー、またやっちゃった？」\n\n【指針】\n上記のように、パロディやメタ発言をボケとして入れろ。チートで問題を瞬殺し、「ま、いっか」と軽く流すポジティブな諦観で進めろ。"
    },
    "style_romcom_cynical": {
        "name": "俺ガイル風（渡航）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「青春とは嘘であり、悪だ。彼らは失敗すら美談にする。だが、俺は騙されない。その笑顔の裏にある欺瞞を、共依存という名のシステムを、俺だけは知っている。」\n\n【指針】\n上記のように、ひねくれた視点からの哲学的・社会学的考察から入れ。会話の裏にある本音を探り合う、緊張感と苦味のある青春を描け。"
    },
    "style_chat_log": {
        "name": "掲示板・配信回風（一般的Web様式）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「\n234：名無しさん\n　>>1 おつ\n　え、今の魔法ヤバすぎワロタｗｗ\n235：名無しさん\n　これは神回\n　主人公強すぎだろ、修正はよ\n」\n\n【指針】\n上記のように、掲示板形式（レス番、名前、本文）で記述せよ。ネットスラングを多用し、掌返しやライブ感を演出せよ。"
    },
    "style_villainess_elegant": {
        "name": "悪役令嬢・宮廷風",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「あら、ごきげんよう。随分と安っぽいドレスですこと。……ふふ、冗談ですわ。ただ、わたくしの視界に入らないでいただけます？　汚らわしいので」\n\n【指針】\n上記のように、優雅な敬語（お嬢様言葉）で毒を吐け。ドレスや宝石の美しさを描写しつつ、高度な皮肉の応酬を行え。"
    },
    "style_slow_life": {
        "name": "スローライフ風（一般的Web様式）",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「採れたてのトマト、真っ赤でツヤツヤだ。ガブリ。……うん、甘い！　太陽の味がする。フェンリルも食べるか？　『ワフッ！』　よしよし、平和だなぁ」\n\n【指針】\n上記のように、ストレスフリーな日常を描け。食事の美味しさや動物との触れ合いを重視し、「これでいい」という充足感を強調せよ。"
    },
    "style_web_standard": {
        "name": "なろうテンプレ標準",
        "instruction": "【文体サンプル（Few-Shot）】\n例：「鑑定！　……うわ、ステータスがカンストしてる。これならドラゴンも余裕か？　『ギャオオオ！』　ズドン。一撃で倒してしまった。……また俺、何かやっちゃいました？」\n\n【指針】\n上記のように、短文と改行を多用し、スマホでの読みやすさを最優先せよ。主人公への称賛とチート能力の結果を淡々と、かつ爽快に描け。"
    }
}

# ==========================================
# Pydantic Schemas (構造化出力用)
# ==========================================

class SceneDetail(BaseModel):
    location: str = Field(..., description="シーンの場所")
    action: str = Field(..., description="シーン内で起きる具体的な出来事")
    dialogue_point: str = Field(..., description="主要な会話の内容")
    role: str = Field(..., description="シーンの役割（伏線/アクション/感情）")

class PlotEpisode(BaseModel):
    ep_num: int
    title: str
    detailed_blueprint: str = Field(..., description="物語の設計図。500文字以上で、具体的な会話、情景、アクションの流れ、前話からの接続を記述すること。")
    setup: str
    conflict: str
    climax: str
    next_hook: str 
    tension: int
    stress: int = Field(default=0, description="読者のストレス度(0-100)。理不尽な展開やヘイト溜め。")
    catharsis: int = Field(default=0, description="カタルシス度(0-100)。ざまぁ、逆転、無双。")
    scenes: List[SceneDetail] = Field(..., description="各シーンの定義。")

class CharacterRegistry(BaseModel):
    name: str
    tone: str
    personality: str
    ability: str
    monologue_style: str
    pronouns: str = Field(..., description="JSON string mapping keys (e.g., '一人称', '二人称') to values")
    keyword_dictionary: str = Field(..., description="JSON string mapping unique terms to their reading or definition")
    relations: str = Field(default="{}", description="JSON string mapping character names to relationship status/feelings (e.g. {'ヒロインA': '好意(90)', 'ライバルB': '敵対(80)'})")
    dialogue_samples: str = Field(default="{}", description="JSON string mapping specific situations/emotions to sample dialogue lines.")

    def to_dict(self):
        return self.model_dump()

    def get_context_prompt(self) -> str:
        p_json = {}
        try: p_json = json.loads(self.pronouns) if isinstance(self.pronouns, str) else self.pronouns
        except: pass
        
        k_json = {}
        try: k_json = json.loads(self.keyword_dictionary) if isinstance(self.keyword_dictionary, str) else self.keyword_dictionary
        except: pass

        r_json = {}
        try: r_json = json.loads(self.relations) if isinstance(self.relations, str) else self.relations
        except: pass

        d_json = {}
        try: d_json = json.loads(self.dialogue_samples) if isinstance(self.dialogue_samples, str) else self.dialogue_samples
        except: pass

        prompt = "【CHARACTER REGISTRY: ABSOLUTE RULES】\n"
        prompt += f"■ {self.name} (主人公)\n"
        prompt += f"  - Tone: {self.tone}\n"
        prompt += f"  - Personality: {self.personality}\n"
        prompt += f"  - Ability: {self.ability}\n"
        prompt += f"  - Monologue Style: {self.monologue_style}\n"
        prompt += f"  - Pronouns: {json.dumps(p_json, ensure_ascii=False)}\n"
        prompt += f"  - Relations: {json.dumps(r_json, ensure_ascii=False)}\n"
        prompt += f"  - Dialogue Samples (Must Mimic): {json.dumps(d_json, ensure_ascii=False)}\n"
        return prompt

class QualityReport(BaseModel):
    is_consistent: bool = Field(..., description="設定矛盾がないか")
    fatal_errors: List[str] = Field(default_factory=list, description="致命的な矛盾")
    consistency_score: int = Field(..., description="整合性スコア(0-100)")
    cliffhanger_score: int = Field(..., description="引きの強さ(0-100)")
    kakuyomu_appeal_score: int = Field(..., description="カクヨム読者への訴求力(0-100)")
    stress_level: int = Field(..., description="読者が感じるストレスレベル(0-100)")
    catharsis_level: int = Field(..., description="読者が感じるカタルシスレベル(0-100)")
    improvement_advice: str = Field(..., description="改善アドバイス")
    suggested_diff: str = Field(default="", description="具体的な修正済み原稿の差分提案。品質不足と判断した箇所の書き換え案。")

class MarketingAssets(BaseModel):
    catchcopies: List[str] = Field(..., description="読者を惹きつけるキャッチコピー案（3つ以上）")
    tags: List[str] = Field(..., description="検索用タグ（5つ以上）")

class WorldState(BaseModel):
    # 指令に基づき、settingsのDiff更新を廃止。new_facts (Facts List) 方式に変更。
    # LLMはsettings全体を返すのではなく、新事実のリストのみを返す。
    new_facts: List[str] = Field(default_factory=list, description="本エピソードで新たに判明した事実・設定のリスト (Append Only)")
    revealed_mysteries: Optional[List[str]] = Field(default=None, description="新たに解明された伏線リスト (Append Only)")
    pending_foreshadowing: Optional[List[str]] = Field(default=None, description="新たに追加された伏線リスト (Append Only)")
    dependency_graph: Optional[str] = Field(default=None, description="JSON mapping of foreshadowing ID to target ep_num (Diff only)")

class AnchorResponse(BaseModel):
    ep_num: int = Field(..., description="対象となる話数")
    summary: str = Field(..., description="あらすじ（500文字程度）")
    world_state: WorldState = Field(..., description="この時点での世界状態")

# 【2段階生成用モデル定義】
class WorldBible(BaseModel):
    title: str
    concept: str
    synopsis: str
    mc_profile: CharacterRegistry
    marketing_assets: MarketingAssets
    anchors: List[AnchorResponse]

class PlotBlueprint(BaseModel):
    plots: List[PlotEpisode]

# 統合モデル（既存互換 + anchors追加）
class NovelStructure(BaseModel):
    title: str
    concept: str
    synopsis: str
    mc_profile: CharacterRegistry
    plots: List[PlotEpisode]
    marketing_assets: MarketingAssets
    anchors: Optional[List[AnchorResponse]] = None # 追加: 2段階生成で得たアンカーを保持

class EpisodeResponse(BaseModel):
    content: str = Field(..., description="エピソード本文 (1500-2000文字)")
    summary: str = Field(..., description="次話への文脈用要約 (300文字程度)")
    self_evaluation_score: int = Field(..., description="このエピソードの面白さの自己採点 (0-100)。")
    low_quality_reason: Optional[str] = Field(default=None, description="点数が低い場合の理由。")
    next_world_state: WorldState = Field(default_factory=WorldState, description="この話の結果更新された世界状態 (Fact Append)")

class TrendSeed(BaseModel):
    genre: str
    keywords: str
    personality: str
    tone: str
    hook_text: str
    style: str

# ==========================================
# Prompt Manager (ContextBuilder実装)
# ==========================================
class PromptManager:
    TEMPLATES = {
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
        "generate_world_bible": """
あなたはWeb小説の神級プロットアーキテクト（設定・構成担当）です。
ジャンル「{genre}」で、カクヨム読者を熱狂させる物語の「世界観設定（Bible）」と「章ごとのマイルストーン（Anchor）」を作成してください。

【ユーザー指定条件】
1. 文体: 「{style_name}」
2. 主人公: 性格{mc_personality}, 口調「{mc_tone}」
3. テーマ: {keywords}

【Task 1: Character & World Concept】
主人公の設定（Registry）と、作品のコンセプト、あらすじ、マーケティング要素を定義せよ。

【Task 2: Anchors (Chapter Milestones)】
物語の整合性を保つため、以下の話数終了時点での「到達状態（あらすじと世界状態）」を確定させよ。
対象話数: [15, 25, 35, 45, 50]
※これらは物語の「チェックポイント」となる。各Anchorには必ず `ep_num` を含めること。

Output strictly in JSON format following this schema:
{schema}
""",
        "generate_plot_flow": """
あなたはWeb小説の神級プロットアーキテクト（ストーリー構成担当）です。
以下の「確定した世界観・マイルストーン」に基づき、スタートからゴールまでを繋ぐ**全50話のプロットフロー**を作成してください。

【既知の設定とマイルストーン（World Bible）】
{world_bible_json}

【Task: Plot Flow Generation (Ep 1-50)】
アンカー（目的地）に矛盾なく到達するように、間のエピソード（1〜50話）のタイトルと簡潔なあらすじ（detailed_blueprint）を埋めよ。
※出力トークン節約のため、各話のblueprintは「次に何が起きるか」がわかる程度の簡潔さ（150〜200文字程度）に留めること。
※アンカーで定義されたイベントは必ずその話数付近で発生させること。

Output strictly in JSON format following this schema:
{schema}
""",
        "anchor_generator": """
あなたは物語のシミュレーターです。
以下のプロットに基づき、第{target_ep}話終了時点での「あらすじ」と「世界の状態（WorldState）」を予測生成してください。
これは物語のアンカーポイント（章の区切り）として使用されます。

【既知の設定】
{bible_context}

【第{target_ep}話までのプロット】
{plot_summary}

出力は以下のJSON形式で厳密に行え:
{{
  "ep_num": {target_ep},
  "summary": "第{target_ep}話時点のあらすじ（500文字程度）",
  "world_state": {{
    "new_facts": ["この時点までに判明している主要な事実リスト"],
    "revealed_mysteries": ["解明された謎"],
    "pending_foreshadowing": ["残っている伏線"],
    "dependency_graph": "{{}}"
  }}
}}
""",
        # テンプレートから断片的なルール変数を削除し、build_writing_promptで動的に構築する形に変更
        "episode_writer_core": """
[SYSTEM]
OUTPUT STRICTLY IN JSON FORMAT.

【ROLE: High-Performance Novelist ({current_model})】
以下の詳細な設計図（Blueprint）に基づき、**Chain of Thought (CoT)** プロセスを用いて最高品質の**第{ep_num}話**を執筆せよ。

【STEP 1: DRAFTING】
- Blueprintに従い、本文のドラフトを作成する。
- 文体指示、キャラクター設定、禁止事項を遵守する。

【STEP 2: SELF-REFLECTION & QA】
- 作成したドラフトを自己評価せよ。
  - ストレス度は適切か？ カタルシスは十分か？
  - 設定矛盾（Bibleとの不整合）はないか？
  - 「引き」は強力か？
- 評価に基づき、ドラフトを修正・強化する。

【STEP 3: FINAL JSON OUTPUT】
- 修正済みの最終原稿のみをJSON形式で出力せよ。

【ネタバレ注意：まだ書いてはいけない裏設定リスト】
{pending_foreshadowing}

{must_resolve_instruction}

【Bridge Context (前話からの接続・必須)】
以下の文脈から1秒も時間を飛ばさず、直結するように書き始めよ。
{prev_context_text}

【今回の設計図 (Detailed Blueprint)】
{episode_plot_text}
※もしDetailed Blueprintが空の場合は、以下の標準構成に従え：
[導入] 状況の提示と前話からの接続
[展開] トラブル発生またはイベントの進行
[結末] 衝撃的な事実の発覚または絶体絶命のピンチ（次話への引き）

【World Context (Bible v{expected_version})】
{bible_context}

OUTPUT STRICTLY IN JSON FORMAT.
Schema:
{{
  "content": "修正済みの最終エピソード本文 (1500-2000文字)",
  "summary": "次話への文脈用要約 (300文字)",
  "self_evaluation_score": 0,
  "low_quality_reason": "点数が低い場合の理由",
  "next_world_state": {{
    "new_facts": ["本エピソードで確定した新しい事実や設定のリスト (Append Only)"],
    "revealed_mysteries": ["新しく解明された謎"],
    "pending_foreshadowing": ["新しく追加された伏線"],
    "dependency_graph": "更新された依存グラフJSON文字列"
  }}
}}
"""
    }

    def get(self, name, **kwargs):
        if name not in self.TEMPLATES:
            raise ValueError(f"Template '{name}' not found.")
        return self.TEMPLATES[name].format(**kwargs)

    def apply_style(self, style_key: str) -> str:
        """指定されたスタイルのFew-Shot指示文を取得する"""
        style_def = STYLE_DEFINITIONS.get(style_key, STYLE_DEFINITIONS["style_web_standard"])
        return f"【Target Style: {style_def['name']}】\n{style_def['instruction']}"

    def build_writing_prompt(self, 
                             mc_name, mc_tone, pronouns, relations, mc_dialogue_samples, # System Rules params
                             style_instruction, entity_context, # Context params
                             pacing_instruction, pacing_graph,
                             prev_last_sentence=None, # 強制接続用
                             **kwargs # Template params
                             ) -> str:
        """
        断片的なルールを統合し、最適な順序（Recency Bias考慮）でプロンプトを構築する ContextBuilder。
        """
        
        # 1. System Rules (Hard strict rules)
        system_rules = f"""# SYSTEM RULES: STRICT ADHERENCE REQUIRED
【キャラクター・ロック（絶対遵守）】
以下のキャラクター定義から1ミリでも逸脱してはならない。
1. **主人公名**: {mc_name}
2. **基本口調**: 「{mc_tone}」
3. **一人称・二人称**: {pronouns}
   ※「俺」設定なら必ず「俺」を使え。
4. **関係性の固定**:
   {relations}
   ※上記の関係性（好意、敵対、恐怖など）に基づく態度を維持せよ。
5. **口調サンプル**:
   {mc_dialogue_samples}
   ※このサンプルのニュアンスを全ての会話で再現せよ。

【知識の遮断 (Knowledge Cutoff)】
あなたは物語の書き手だが、**主人公の知識レベル**を超越してはならない。
- Bibleに書かれている「世界の真実」や「ラスボスの正体」を、物語上で開示されるまで地の文やセリフに出すな。
- 全ては「主人公の視点」から見た限定的な情報として描写せよ。

【日本語作法・厳格なルール】
1. **三点リーダー**: 「……」と必ず2個（偶数個）セットで記述せよ。
2. **感嘆符・疑問符**: 「！」や「？」の直後には必ず全角スペースを1つ空けよ（文末の閉じ括弧直前を除く）。
3. **改行の演出**: 場面転換や衝撃的な瞬間の前には、空白行を挟んで「溜め」を作れ。

【執筆プロトコル: 一括生成モード】
以下のルールを厳守し、1回の出力で物語の1エピソード（導入から結末まで）を完結させよ。

1. **情報開示制限（Spoiler Guard）**:
   - **「Detailed Blueprint」に書かれていない新キャラ、新設定、新展開を勝手に創作することを固く禁ずる。**
   - 設計図にあるイベントだけを忠実に描写せよ。

2. **ブリッジ・コンテキスト（前話との接続）**:
   - **直前のシーンから1秒も時間を飛ばさずに書き始めよ。**
   - 前話のラストで提示された感情、場所、状況を冒頭の1行目で必ず引き継げ。

3. **出力文字数**:
   - 必ず **1,500文字〜2,000文字** の範囲に収めること。

4. **構成（起承転結）**:
   - **重要: 解決（Resolution）を禁止する。** 物語を安易に解決させず、必ず「Next Hook（次への引き）」で終わること。

5. **【最重要】カクヨム・メソッド（リアクション）**:
    - 主人公の行動に対する「周囲の反応」を必ず描写せよ。

【究極の「引き」生成ロジック: Cliffhanger Protocol】
各エピソードの結末は、文脈に応じて最も効果的な「引き」を自律的に判断し、**「読者が次を読まずにいられない状態」**を強制的に作り出せ。
"""

        # 強制接続ルールの追加
        if prev_last_sentence:
            system_rules = f"""
【絶対ルール：書き出しの指定】
書き出しのルール：以下の文から書き始めよ『{prev_last_sentence}』
※この文を冒頭に置くことで、前話からの連続性を物理的に維持せよ。

{system_rules}"""

        # 2. Core Template Interpolation
        base_prompt = self.get("episode_writer_core", **kwargs)

        # 3. Assemble Final Prompt with Recency Bias (Style & Entity at the end)
        # 構造: [System Rules] -> [Pacing Info] -> [Base Prompt (Blueprint/Context)] -> [Entity/Style Instructions (Recency)]
        
        final_prompt = f"""
{system_rules}

【PACING & EMOTION GRAPH (Current Flow)】
{pacing_graph}
Instruction: {pacing_instruction}

{base_prompt}

【IMPORTANT: STYLE & CHARACTER ENFORCEMENT】
Gemmaモデルは以下の指示を最優先で実行せよ。
{entity_context}

{style_instruction}

【MANDATORY SELF-EVALUATION】
執筆後、以下の基準で厳しく自己採点し、JSONの `self_evaluation_score` に記入せよ。
- 80-100点: 読者が続きを読みたくてたまらない「引き」があり、感情が揺さぶられる。
- 60-79点: 普通。矛盾はないが、盛り上がりに欠ける。
- 0-59点: 退屈、説明過多、またはキャラの性格が崩壊している。

**重要: もし自信がなければ低い点数をつけよ。基準点未満をつけると、自動的にリトライが行われる。**
"""
        return final_prompt

# ==========================================
# Formatter Class (Regex-based)
# ==========================================
class TextFormatter:
    def __init__(self, engine):
        self.engine = engine # 互換性のために保持するが使用しない

    def force_connect(self, text, prev_last_sentence):
        """前話の終わりの一文と重複している場合、それを削除して接続を滑らかにする"""
        if not text or not prev_last_sentence:
            return text
        
        clean_text = text.strip()
        clean_prev = prev_last_sentence.strip()
        
        # もし生成されたテキストが前話のラスト一文から始まっていたら、その部分を削除する
        # （読者には「前話の終わり」→「次話の始まり」と連続して表示されるため、重複を避ける）
        if clean_text.startswith(clean_prev):
            return clean_text[len(clean_prev):].strip()
        
        return clean_text

    def _remove_chat_artifacts(self, text):
        """チャット特有のノイズ（Artifacts）を除去する"""
        # 冒頭のノイズ: "はい、承知しました" "以下が小説です" "Here is the story" 等
        text = re.sub(r'^(はい|承知|了解|以下|これ|Here|Sure|Certainly|Okay).*?(\n|$)', '', text, flags=re.IGNORECASE | re.MULTILINE).strip()
        # Markdownの冒頭ブロック除去
        text = re.sub(r'^\*\*.*?\*\*\n', '', text).strip()
        
        # 末尾のノイズ: "以上です" "いかがでしたか" "End of episode" 等
        text = re.sub(r'(\n|^)(以上|End|Hope|Do you|いかが|書き終).*?$', '', text, flags=re.IGNORECASE | re.MULTILINE).strip()
        
        return text

    def _clean_kakuyomu_style(self, text):
        """カクヨム向け物理整形: 強制空行挿入"""
        # 地の文3行以上で強制空行
        lines = text.split('\n')
        formatted_lines = []
        narrative_count = 0
        
        for line in lines:
            stripped = line.strip()
            
            # 空行の場合
            if not stripped:
                formatted_lines.append(line)
                narrative_count = 0
                continue
                
            # 会話文判定（カギ括弧で始まるか）
            is_dialogue = stripped.startswith(('「', '『', '（'))
            
            if not is_dialogue:
                narrative_count += 1
            else:
                narrative_count = 0 # 会話文でリセット
            
            # 3行連続した場合、その行の前に空行を入れる（読みやすさのため）
            if narrative_count >= 3:
                formatted_lines.append('') # 空行挿入
                narrative_count = 1 # カウントリセット（この行が新たなブロックの1行目となる）
            
            formatted_lines.append(line)
            
        return "\n".join(formatted_lines)

    async def format(self, text, k_dict=None):
        if not text: return ""
        
        # 0. チャットアーティファクトの除去 (新規追加)
        text = self._remove_chat_artifacts(text)

        # 1. 三点リーダーの正規化 (…1つや...を……に)
        text = re.sub(r'…{1,}', '……', text)
        text = re.sub(r'\.{2,}', '……', text)
        # 奇数個の……を偶数個に補正（簡易的）
        text = text.replace('………', '……') 
        
        # 2. 感嘆符・疑問符の後のスペース挿入
        # 閉じ括弧の前以外で、全角スペースがない場合に挿入
        text = re.sub(r'([！？])(?![\s　」』])', r'\1　', text)
        
        # 3. 連続する空行の削除（最大1行まで）
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # 4. 行頭・行末の空白削除
        lines = [line.rstrip() for line in text.splitlines()]
        text = "\n".join(lines)
        
        # 5. 不要なMarkdown削除
        text = re.sub(r'```.*?```', '', text, flags=re.DOTALL)
        text = text.replace('**', '').replace('##', '')

        # 6. カクヨム物理整形
        text = self._clean_kakuyomu_style(text)

        return text.strip()

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
                    last_updated TEXT,
                    FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
                );
            ''')
        # plotテーブル更新: stress, catharsis追加, detailed_blueprint追加
        try:
            await self.execute('ALTER TABLE plot ADD COLUMN detailed_blueprint TEXT')
        except: pass

        await self.execute('''
                CREATE TABLE IF NOT EXISTS plot (
                    book_id INTEGER, ep_num INTEGER, title TEXT, summary TEXT,
                    main_event TEXT, sub_event TEXT, pacing_type TEXT,
                    tension INTEGER DEFAULT 50, 
                    stress INTEGER DEFAULT 0,
                    catharsis INTEGER DEFAULT 0,
                    cliffhanger_score INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'planned', 
                    setup TEXT, conflict TEXT, climax TEXT, resolution TEXT,
                    scenes TEXT, detailed_blueprint TEXT,
                    PRIMARY KEY(book_id, ep_num),
                    FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
                );
            ''')
        await self.execute('''
                CREATE TABLE IF NOT EXISTS chapters (
                    book_id INTEGER, ep_num INTEGER, title TEXT, content TEXT,
                    score_story INTEGER, killer_phrase TEXT, reader_retention_score INTEGER,
                    ending_emotion TEXT, discomfort_score INTEGER DEFAULT 0, tags TEXT,
                    ai_insight TEXT, retention_data TEXT, summary TEXT, world_state TEXT,
                    created_at TEXT, PRIMARY KEY(book_id, ep_num),
                    FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
                );
            ''')
        await self.execute('''
                CREATE TABLE IF NOT EXISTS characters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, book_id INTEGER, name TEXT, role TEXT, registry_data TEXT, monologue_style TEXT,
                    FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
                );
            ''')
        
        # インデックスの作成
        await self.execute('CREATE INDEX IF NOT EXISTS idx_plot_book_ep ON plot(book_id, ep_num);')
        await self.execute('CREATE INDEX IF NOT EXISTS idx_chapters_book_ep ON chapters(book_id, ep_num);')

    def _convert_params(self, params):
        new_params = []
        for p in params:
            if isinstance(p, BaseModel):
                new_params.append(p.model_dump_json(ensure_ascii=False))
            elif isinstance(p, (dict, list)):
                new_params.append(json.dumps(p, ensure_ascii=False))
            else:
                new_params.append(p)
        return tuple(new_params)

    async def execute(self, query, params=()):
        # パラメータの自動JSON変換
        converted_params = self._convert_params(params)
        future = asyncio.get_running_loop().create_future()
        await self.queue.put((query, converted_params, future))
        return await future

    async def save_model(self, query, params):
        """PydanticモデルやDictを自動的にJSON文字列に変換して保存する"""
        return await self.execute(query, params)

    async def load_model(self, query, params, model_class: Type[BaseModel]):
        """クエリ結果を指定されたPydanticモデルとしてロードする"""
        row = await self.fetch_one(query, params)
        if not row: return None
        # 行データ全体をモデルにマッピングできない場合（JSONカラム単体の取得など）を考慮し
        # 戻り値の形式に応じて処理を分岐
        if len(row) == 1 and list(row.keys())[0] in row: # 単一カラム取得の場合
             val = list(row.values())[0]
             if isinstance(val, str):
                 try:
                     return model_class.model_validate_json(val)
                 except: pass
        
        # 行全体を辞書として渡す
        return model_class.model_validate(dict(row))

    async def _worker(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys = ON;") # 外部キー制約の有効化
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
                conn.execute("PRAGMA foreign_keys = ON;")
                return [dict(row) for row in conn.execute(query, params).fetchall()]
        return await asyncio.to_thread(_fetch)
            
    async def fetch_one(self, query, params=()):
        def _fetch():
            with sqlite3.connect(self.db_path, check_same_thread=False) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA foreign_keys = ON;")
                row = conn.execute(query, params).fetchone()
                return dict(row) if row else None
        return await asyncio.to_thread(_fetch)

db = DatabaseManager(DB_FILE)

# ==========================================
# Repository Pattern (指令により強化)
# ==========================================
class NovelRepository:
    def __init__(self, db_manager):
        self.db = db_manager

    # --- Create / Write ---
    async def create_novel(self, data, genre, style_dna_str):
        if isinstance(data, dict): data_dict = data
        else: data_dict = data.model_dump()
        
        dna = json.dumps({
            "tone": data_dict['mc_profile']['tone'], 
            "personality": data_dict['mc_profile'].get('personality', ''),
            "style_mode": style_dna_str,
            "pov_type": "一人称"
        }, ensure_ascii=False)
        
        ability_val = data_dict['mc_profile'].get('ability', '')
        
        marketing_data_model = data.marketing_assets if isinstance(data, BaseModel) else MarketingAssets.model_validate(data_dict['marketing_assets'])

        # target_eps を 50 に設定
        bid = await self.db.save_model(
            "INSERT INTO books (title, genre, synopsis, concept, target_eps, style_dna, status, special_ability, created_at, marketing_data) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                data_dict['title'], 
                genre, 
                data_dict['synopsis'], 
                data_dict['concept'], 
                50, 
                dna, 
                'active', 
                ability_val, 
                datetime.datetime.now().isoformat(),
                marketing_data_model
            )
        )
        
        registry_json = json.dumps(data_dict['mc_profile'], ensure_ascii=False)
        monologue_val = data_dict['mc_profile'].get('monologue_style', '')
        
        await self.db.save_model("INSERT INTO characters (book_id, name, role, registry_data, monologue_style) VALUES (?,?,?,?,?)", 
                          (bid, data_dict['mc_profile']['name'], '主人公', registry_json, monologue_val))
        
        await self.db.save_model("INSERT INTO bible (book_id, settings, revealed, revealed_mysteries, pending_foreshadowing, dependency_graph, version, last_updated) VALUES (?,?,?,?,?,?,?,?)",
                             (bid, "{}", [], [], [], "{}", 0, datetime.datetime.now().isoformat()))

        saved_plots = []
        for p in data_dict['plots']:
            full_title = f"第{p['ep_num']}話 {p['title']}"
            main_ev = f"{p.get('setup','')}->{p.get('climax','')}"
            scenes_list = p.get('scenes', []) 
            
            await self.db.save_model(
                """INSERT INTO plot (book_id, ep_num, title, main_event, setup, conflict, climax, resolution, tension, stress, catharsis, status, scenes, detailed_blueprint)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (bid, p['ep_num'], full_title, main_ev, 
                 p.get('setup'), p.get('conflict'), p.get('climax'), p.get('next_hook'),
                 p.get('tension', 50), p.get('stress', 0), p.get('catharsis', 0), 'planned', scenes_list, p.get('detailed_blueprint', ''))
            )
            saved_plots.append(p)
        return bid, saved_plots

    async def add_plots(self, book_id, data_p2):
        saved_plots = []
        for p in data_p2['plots']:
            full_title = f"第{p['ep_num']}話 {p['title']}"
            main_ev = f"{p.get('setup','')}->{p.get('climax','')}"
            scenes_list = p.get('scenes', [])
            
            await self.db.save_model(
                """INSERT INTO plot (book_id, ep_num, title, main_event, setup, conflict, climax, resolution, tension, stress, catharsis, status, scenes, detailed_blueprint)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (book_id, p['ep_num'], full_title, main_ev, 
                 p.get('setup'), p.get('conflict'), p.get('climax'), p.get('next_hook'), 
                 p.get('tension', 50), p.get('stress', 0), p.get('catharsis', 0), 'planned', scenes_list, p.get('detailed_blueprint', ''))
            )
            saved_plots.append(p)
        return saved_plots

    # --- Read / Fetch Methods (Moved from Task Functions) ---
    async def get_book(self, book_id: int):
        return await self.db.fetch_one("SELECT * FROM books WHERE id=?", (book_id,))

    async def get_plots(self, book_id: int):
        return await self.db.fetch_all("SELECT * FROM plot WHERE book_id=? ORDER BY ep_num", (book_id,))

    async def get_characters(self, book_id: int):
        return await self.db.fetch_all("SELECT * FROM characters WHERE book_id=?", (book_id,))
    
    async def get_main_character(self, book_id: int):
        return await self.db.fetch_one("SELECT * FROM characters WHERE book_id=? AND role='主人公'", (book_id,))

    async def get_chapters(self, book_id: int):
        return await self.db.fetch_all("SELECT * FROM chapters WHERE book_id=? ORDER BY ep_num", (book_id,))

    async def get_latest_chapter(self, book_id: int, ep_num: int):
        """指定エピソードの直前のチャプターを取得"""
        # Note: Updated to include world_state for anchor loading
        return await self.db.fetch_one("SELECT content, summary, world_state FROM chapters WHERE book_id=? AND ep_num=? ORDER BY ep_num DESC LIMIT 1", (book_id, ep_num - 1))

    async def get_recent_plot_metrics(self, book_id: int, current_ep: int, limit: int = 5):
        """直近のエピソードのストレス/カタルシス指標を取得"""
        return await self.db.fetch_all(
            "SELECT ep_num, stress, catharsis FROM plot WHERE book_id=? AND ep_num < ? ORDER BY ep_num DESC LIMIT ?",
            (book_id, current_ep, limit)
        )

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
            state = WorldState(new_facts=[], revealed_mysteries=[], pending_foreshadowing=[], dependency_graph="{}")
            return state, 0
        try:
            state = WorldState(
                new_facts=[], # DBからのロード時は差分リストは空。revealedを累積として扱うが、WorldStateオブジェクトとしては空で初期化
                revealed_mysteries=json.loads(row['revealed_mysteries']) if row.get('revealed_mysteries') else [],
                pending_foreshadowing=json.loads(row['pending_foreshadowing']) if row.get('pending_foreshadowing') else [],
                dependency_graph=row['dependency_graph'] if row['dependency_graph'] else "{}"
            )
            # 便宜上、設定データなどは内部保持しておく（プロンプト生成用）
            self._current_settings = row['settings'] if row['settings'] else "{}"
            self._current_revealed = json.loads(row['revealed']) if row['revealed'] else []
            return state, row.get('version', 0)
        except:
            state = WorldState(new_facts=[], revealed_mysteries=[], pending_foreshadowing=[], dependency_graph="{}")
            self._current_settings = "{}"
            self._current_revealed = []
            return state, 0

    async def get_prompt_context(self) -> str:
        state, ver = await self.get_current_state()
        return f"""
【WORLD STATE (Current v{ver})】
[SETTINGS]: {self._current_settings}
[REVEALED FACTS]: {json.dumps(self._current_revealed, ensure_ascii=False)}
[SOLVED MYSTERIES]: {json.dumps(state.revealed_mysteries, ensure_ascii=False)}
[PENDING FORESHADOWING (FOR FUTURE USE ONLY)]: {json.dumps(state.pending_foreshadowing, ensure_ascii=False)}
[DEPENDENCY GRAPH (Resolution Plan)]: {state.dependency_graph}
"""

class BibleSynchronizer:
    def __init__(self, book_id):
        self.book_id = book_id
        self.bible_manager = DynamicBibleManager(book_id)

    async def save_atomic(self, chapter_data: Dict[str, Any], next_state: WorldState):
        """
        本文生成と同時にBibleとChapterをアトミックに更新する。
        Fact Append 方式に対応。Python側で安全にマージを行う。
        """
        # 1. 現在のBible状態を取得 (ロード)
        current_state_obj, current_ver = await self.bible_manager.get_current_state()
        
        # Current DB values (not just the Diff object)
        row = await db.fetch_one("SELECT * FROM bible WHERE book_id=? ORDER BY id DESC LIMIT 1", (self.book_id,))
        if row:
            curr_settings_str = row['settings']
            curr_revealed = json.loads(row['revealed']) if row['revealed'] else []
            curr_mysteries = json.loads(row['revealed_mysteries']) if row.get('revealed_mysteries') else []
            curr_foreshadowing = json.loads(row['pending_foreshadowing']) if row.get('pending_foreshadowing') else []
            curr_dep_graph_str = row['dependency_graph'] if row['dependency_graph'] else "{}"
        else:
            curr_settings_str = "{}"
            curr_revealed = []
            curr_mysteries = []
            curr_foreshadowing = []
            curr_dep_graph_str = "{}"

        # 2. Bible状態のマージ (Append Only Logic)
        
        # Facts: next_state.new_facts を existing revealed list に追加
        new_facts_list = next_state.new_facts or []
        updated_revealed = list(set(curr_revealed + new_facts_list)) # 重複排除しつつマージ

        # Mysteries & Foreshadowing: 単純追加
        updated_mysteries = list(set(curr_mysteries + (next_state.revealed_mysteries or [])))
        updated_foreshadowing = list(set(curr_foreshadowing + (next_state.pending_foreshadowing or [])))
        
        # Settings: 指令によりLLMに書き換えさせない。
        merged_settings = curr_settings_str 

        # Dependency Graph (Merge)
        merged_graph = curr_dep_graph_str
        if next_state.dependency_graph:
            try:
                curr_dep = json.loads(curr_dep_graph_str)
                next_dep = json.loads(next_state.dependency_graph) if next_state.dependency_graph else {}
                curr_dep.update(next_dep) # マージ
                merged_graph = json.dumps(curr_dep, ensure_ascii=False)
            except:
                merged_graph = next_state.dependency_graph # 失敗時は上書き

        new_version = current_ver + 1

        # 3. Formatterの適用（Chapter保存用）
        formatter = TextFormatter(None)
        # Fetch keywords for formatting
        mc = await db.fetch_one("SELECT registry_data FROM characters WHERE book_id=? AND role='主人公'", (self.book_id,))
        k_dict = {}
        if mc and mc['registry_data']:
            try: 
                reg = json.loads(mc['registry_data'])
                k_str = reg.get('keyword_dictionary', '{}')
                k_dict = json.loads(k_str) if isinstance(k_str, str) else k_str
            except: pass
        
        content_formatted = await formatter.format(chapter_data['content'], k_dict=k_dict)
        
        # 4. DBへのアトミック更新
        # Bible Insert
        await db.save_model(
            "INSERT INTO bible (book_id, settings, revealed, revealed_mysteries, pending_foreshadowing, dependency_graph, version, last_updated) VALUES (?,?,?,?,?,?,?,?)",
            (
                self.book_id,
                merged_settings,        
                updated_revealed, 
                updated_mysteries,
                updated_foreshadowing,
                merged_graph,
                new_version,
                datetime.datetime.now().isoformat()
            )
        )
        
        # Chapter Insert/Update
        await db.save_model(
            """INSERT OR REPLACE INTO chapters (book_id, ep_num, title, content, summary, ai_insight, world_state, created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (self.book_id, chapter_data['ep_num'], chapter_data.get('title', f"第{chapter_data['ep_num']}話"), content_formatted, chapter_data.get('summary', ''), '', next_state, datetime.datetime.now().isoformat())
        )
        
        # Plot Status Update
        await db.save_model("UPDATE plot SET status='completed' WHERE book_id=? AND ep_num=?", (self.book_id, chapter_data['ep_num']))

        return new_version

# ==========================================
# 4. New Classes (TrendAnalyst, QA, Pacing)
# ==========================================

class TrendAnalyst:
    def __init__(self, engine):
        self.engine = engine

    async def get_dynamic_seed(self) -> dict:
        print("TrendAnalyst: Selecting from Static Trend List (No API Call)...")
        # APIコールを廃止し、定義済みリストから選択
        trend_seeds = [
            {
                "genre": "現代ダンジョン",
                "keywords": "配信, 事故, 無双",
                "personality": "冷静沈着だが承認欲求が少しある",
                "tone": "俺",
                "hook_text": "配信切り忘れで世界最強がバレる",
                "style": "style_web_standard"
            },
            {
                "genre": "ハイファンタジー",
                "keywords": "追放, ざまぁ, スローライフ",
                "personality": "お人好しだが怒ると怖い",
                "tone": "僕",
                "hook_text": "勇者パーティを追放されたので辺境で店を開いたら、魔王が常連になった",
                "style": "style_slow_life"
            },
            {
                "genre": "異世界転生",
                "keywords": "悪役令嬢, 断罪回避, 内政",
                "personality": "合理的で少し冷徹",
                "tone": "私（わたくし）",
                "hook_text": "断罪イベントの前日に前世の記憶を取り戻した",
                "style": "style_villainess_elegant"
            }
        ]
        seed = random.choice(trend_seeds)
        print(f"★ Trend Selected: {seed.get('genre', 'Unknown')} - {seed.get('hook_text', 'No hook')}")
        return seed

class PacingGraph:
    @staticmethod
    async def analyze(book_id: int, current_ep: int, total_eps: int = 50) -> Dict[str, Any]:
        """
        構造的ペーシングロジック。
        単純な数値計算ではなく、プロット構造上の位置（起承転結）と周期的なクライマックスを重視する。
        """
        repo = NovelRepository(db)
        history = await repo.get_recent_plot_metrics(book_id, current_ep)
        history = [dict(r) for r in history][::-1] # DB行を辞書化して反転（古い順）
        
        # Graph Visualization String
        graph_lines = []
        for h in history:
            s_level = "High" if h.get('stress', 0) > 60 else "Low"
            c_level = "High" if h.get('catharsis', 0) > 60 else "Low"
            graph_lines.append(f"Ep{h['ep_num']}: [Stress:{s_level}] -> [Catharsis:{c_level}]")
        graph_visualization = " -> ".join(graph_lines) if graph_lines else "(First Episode)"

        # --- 構造的ペーシングロジック ---
        
        # 1. 位置による判定
        is_first_ep = (current_ep == 1)
        is_final_ep = (current_ep == total_eps)
        
        # 2. 周期的なクライマックス判定
        is_big_climax = (current_ep % 10 == 0) # 10話ごと（章の区切り）
        is_small_climax = (current_ep % 5 == 0) and not is_big_climax # 5話ごと（中だるみ防止）
        
        # 3. 指示内容の決定
        instruction = ""
        density_instruction = "【文章密度: 標準, 会話比率: 40%】"
        temperature = 0.8
        
        # 【序盤ロジック修正】第1話〜第3話の強制スローダウン
        if current_ep <= 3:
            instruction = "【超スローペース】世界観の説明をするな。目の前の出来事だけを、五感を使ってゆっくり描写せよ。場面転換をしてはならない。"
            density_instruction = "【情報密度: 極低】 読者が混乱するため、固有名詞は1話につき1つまで。会話と心理描写のみで進行せよ"

        elif is_final_ep:
            instruction = "【最終回】すべての伏線を回収し、最高のカタルシスを提供せよ。余韻を残すこと。"
            density_instruction = "【文章密度: 高（情緒的）, 会話比率: 30%】"
            temperature = 0.85
            
        elif is_big_climax:
            instruction = "【大クライマックス（章の締め）】物語の大きな節目となる激動の展開を描け。圧倒的なカタルシスまたは絶望的なクリフハンガーが必要。"
            density_instruction = "【文章密度: 特高（描写重視）, 会話比率: 20%】"
            temperature = 0.9
            
        elif is_small_climax:
            instruction = "【小クライマックス（中盤の山場）】物語に動きをつけ、読者を飽きさせないためのイベントを発生させよ。小気味よい解決と新たな謎の提示。"
            density_instruction = "【文章密度: 中, 会話比率: 60%（テンポ重視）】"
        
        else:
            # 通常回: 前回のストレスチェック
            consecutive_stress_eps = 0
            for h in history[::-1]:
                if h.get('stress', 0) > 60 and h.get('catharsis', 0) < 40:
                    consecutive_stress_eps += 1
                else:
                    break
            
            if consecutive_stress_eps >= 2:
                instruction = "【緊急: ストレス解消】読者のストレスが蓄積している。小さなガス抜き（カタルシス）を用意せよ。"
            else:
                instruction = "物語を着実に進行させよ。次への期待感を維持すること。"
        
        final_instruction = f"{instruction}\n{density_instruction}"

        return {
            "type": "structural",
            "temperature": temperature,
            "instruction": final_instruction,
            "force_catharsis": is_big_climax or is_small_climax,
            "graph_visualization": graph_visualization
        }

# ==========================================
# 5. ULTRA Engine (Autopilot)
# ==========================================
class UltraEngine:
    def __init__(self, api_key):
        self.client = genai.Client(api_key=api_key) if api_key else None
        self.safety_settings = [
            types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="BLOCK_NONE"),
        ]
        self.prompt_manager = PromptManager()
        self.repo = NovelRepository(db)
        self.trend_analyst = TrendAnalyst(self)
        self.formatter = TextFormatter(self)

    async def _generate_with_retry(self, model, contents, config):
        retries = 0
        max_retries = 8
        base_delay = 5.0

        while True:
            try:
                return await self.client.aio.models.generate_content(
                    model=model, 
                    contents=contents, 
                    config=config
                )
            except Exception as e:
                if retries >= max_retries:
                    raise e
                
                # Simple exponential backoff
                delay = (base_delay * (2 ** retries)) + random.uniform(0.1, 1.0)
                print(f"⚠️ API Error: {e}. Retry {retries+1}/{max_retries} in {delay:.2f}s...")
                await asyncio.sleep(delay)
                retries += 1

    def _parse_json_response(self, text: str) -> Dict[str, Any]:
        """
        AIの出力からJSONを堅牢に抽出・正規化するヘルパー関数
        json_repair ロジックおよびフォールバック処理の実装
        """
        # 1. Markdownの削除
        text = re.sub(r'^```json\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'^```\s*', '', text, flags=re.MULTILINE)
        text = re.sub(r'```$', '', text, flags=re.MULTILINE)
        text = text.strip()

        # 2. 制御文字の削除（改行・タブ以外）
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)

        data = None

        # Try Method A: Direct Parse
        try:
            data = json.loads(text, strict=False)
        except:
            # Try Method B: Regex Extraction
            match = re.search(r'(\{.*\})', text, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1), strict=False)
                except:
                    pass
        
        # Try Method C: Robust "json_repair" equivalent (簡易版)
        if data is None:
            try:
                # 一般的なエラーの修正: 末尾のカンマ、閉じ括弧不足、キーのクォート
                fixed_text = text
                # 閉じ括弧の補完
                open_braces = fixed_text.count('{')
                close_braces = fixed_text.count('}')
                fixed_text += '}' * (open_braces - close_braces)
                
                # 末尾カンマの削除 (簡易)
                fixed_text = re.sub(r',\s*}', '}', fixed_text)
                
                data = json.loads(fixed_text, strict=False)
            except:
                pass

        # 3. NOVEL FALLBACK: JSONパースに失敗したが、テキストが小説（会話文など）を含んでいる場合
        if data is None:
            # content フィールドの中身を無理やり抽出
            content_match = re.search(r'"content"\s*:\s*"(.*?)"(?=\s*,\s*"|\s*})', text, re.DOTALL)
            fallback_content = ""
            if content_match:
                fallback_content = content_match.group(1).replace('\\n', '\n').replace('\\"', '"')
            elif len(text) > 100:
                fallback_content = text # 生テキストを採用
            
            # アーティファクト除去を適用
            fallback_content = self.formatter._remove_chat_artifacts(fallback_content)

            if fallback_content:
                print("⚠️ Warning: JSON parse failed, using RegEx/Raw text fallback.")
                data = {
                    "content": fallback_content,
                    "summary": fallback_content[:200] + "...", # 簡易要約
                    "next_world_state": {} # 更新なし
                }
            else:
                # 救済不可能
                raise ValueError(f"Failed to parse JSON and text does not look like a novel snippet. Length: {len(text)}")

        # 4. キーの正規化 (Pydantic対応)
        # [settings] -> settings のように装飾を削除
        normalized_data = {}
        for k, v in data.items():
            # アルファベットとアンダースコア以外を削除して小文字化
            clean_k = re.sub(r'[^a-zA-Z0-9_]', '', k).lower()
            normalized_data[clean_k] = v
        
        # 元データとマージ（正規化キーを優先）
        final_data = data.copy()
        final_data.update(normalized_data)

        # 5. WorldState固有の修正: Dict -> WorldState Objectへのマッピング
        if 'next_world_state' in final_data:
            ws = final_data['next_world_state']
            if isinstance(ws, dict):
                ws_normalized = {}
                for k, v in ws.items():
                    # WorldState内部のキーも正規化
                    clean_k = re.sub(r'[^a-zA-Z0-9_]', '', k).lower()
                    ws_normalized[clean_k] = v
                
                # キーのゆらぎ吸収
                if 'newfacts' in ws_normalized: ws_normalized['new_facts'] = ws_normalized.pop('newfacts')

                # dependency_graphなどが辞書なら文字列化
                for field in ['dependency_graph']:
                    if field in ws_normalized and isinstance(ws_normalized[field], (dict, list)):
                        ws_normalized[field] = json.dumps(ws_normalized[field], ensure_ascii=False)
                
                final_data['next_world_state'] = ws_normalized

        return final_data

    # ---------------------------------------------------------
    # Core Logic
    # ---------------------------------------------------------

    async def generate_universe_blueprint_phase1(self, genre, style, mc_personality, mc_tone, keywords):
        """第1段階: 設定とプロットを2段階で生成"""
        print("Step 1-1: Generating World Bible (Settings & Anchors)...")
        
        style_name = STYLE_DEFINITIONS.get(style, {"name": style}).get("name")
        
        # Schema 1: WorldBible
        bible_schema = WorldBible.model_json_schema()
        
        prompt_bible = self.prompt_manager.get(
            "generate_world_bible",
            genre=genre,
            style_name=style_name,
            mc_personality=mc_personality,
            mc_tone=mc_tone,
            keywords=keywords,
            schema=json.dumps(bible_schema, ensure_ascii=False)
        )

        try:
            # Call 1
            res_bible = await self._generate_with_retry(
                model=MODEL_ULTRALONG,
                contents=prompt_bible,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    safety_settings=self.safety_settings
                )
            )
            data_bible = self._parse_json_response(res_bible.text.strip())
            # Pydantic check
            if 'mc_profile' in data_bible:
                 if isinstance(data_bible['mc_profile'].get('pronouns'), dict):
                         data_bible['mc_profile']['pronouns'] = json.dumps(data_bible['mc_profile']['pronouns'], ensure_ascii=False)
                 if isinstance(data_bible['mc_profile'].get('keyword_dictionary'), dict):
                         data_bible['mc_profile']['keyword_dictionary'] = json.dumps(data_bible['mc_profile']['keyword_dictionary'], ensure_ascii=False)
                 if isinstance(data_bible['mc_profile'].get('relations'), dict):
                         data_bible['mc_profile']['relations'] = json.dumps(data_bible['mc_profile']['relations'], ensure_ascii=False)
                 if isinstance(data_bible['mc_profile'].get('dialogue_samples'), dict):
                         data_bible['mc_profile']['dialogue_samples'] = json.dumps(data_bible['mc_profile']['dialogue_samples'], ensure_ascii=False)
            
            world_bible = WorldBible.model_validate(data_bible)
            print("World Bible Generated.")

            # Call 2
            print("Step 1-2: Generating Plot Flow (Ep 1-50)...")
            plot_schema = PlotBlueprint.model_json_schema()
            
            # Serialize WorldBible for prompt
            bible_json_str = world_bible.model_dump_json(ensure_ascii=False)
            
            prompt_plot = self.prompt_manager.get(
                "generate_plot_flow",
                world_bible_json=bible_json_str,
                schema=json.dumps(plot_schema, ensure_ascii=False)
            )
            
            res_plot = await self._generate_with_retry(
                model=MODEL_ULTRALONG,
                contents=prompt_plot,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    safety_settings=self.safety_settings
                )
            )
            data_plot = self._parse_json_response(res_plot.text.strip())
            plot_blueprint = PlotBlueprint.model_validate(data_plot)
            print("Plot Flow Generated.")

            # Merge into NovelStructure (With Anchors)
            final_structure = NovelStructure(
                title=world_bible.title,
                concept=world_bible.concept,
                synopsis=world_bible.synopsis,
                mc_profile=world_bible.mc_profile,
                marketing_assets=world_bible.marketing_assets,
                plots=plot_blueprint.plots,
                anchors=world_bible.anchors # Attach anchors
            )
            
            return final_structure

        except Exception as e:
            print(f"Plot Gen Phase 1 Error: {e}")
            import traceback
            traceback.print_exc()
            return None

    async def generate_anchor_state(self, book_data, target_ep):
        """マイルストーンとなる章末の状態を先行生成し、DBに保存する"""
        print(f"Generating Anchor State for End of Ep {target_ep}...")
        
        # 1. ターゲットまでのプロット概要を取得
        sorted_plots = sorted(book_data['plots'], key=lambda x: x['ep_num'])
        relevant_plots = [p for p in sorted_plots if p['ep_num'] <= target_ep]
        
        plot_summary = ""
        for p in relevant_plots:
            plot_summary += f"第{p['ep_num']}話: {p['title']}\n{p.get('detailed_blueprint', '')[:200]}...\n\n"
        
        bible_manager = DynamicBibleManager(book_data['book_id'])
        bible_context = await bible_manager.get_prompt_context()

        prompt = self.prompt_manager.get(
            "anchor_generator",
            target_ep=target_ep,
            bible_context=bible_context,
            plot_summary=plot_summary
        )

        try:
            res = await self._generate_with_retry(
                model=MODEL_ULTRALONG, # High context model for summarization
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    safety_settings=self.safety_settings
                )
            )
            
            data = self._parse_json_response(res.text.strip())
            
            # Format WorldState
            ws_dict = data.get('world_state', {})
            # Normalized
            if isinstance(ws_dict, dict):
                 if 'newfacts' in ws_dict: ws_dict['new_facts'] = ws_dict.pop('newfacts')
                 if 'dependency_graph' in ws_dict and isinstance(ws_dict['dependency_graph'], (dict, list)):
                     ws_dict['dependency_graph'] = json.dumps(ws_dict['dependency_graph'], ensure_ascii=False)
            
            # Save
            await self.repo.db.save_model(
                """INSERT OR REPLACE INTO chapters (book_id, ep_num, title, content, summary, ai_insight, world_state, created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    book_data['book_id'], 
                    target_ep, 
                    f"ANCHOR_EP_{target_ep}", 
                    "(ANCHOR_STATE_ONLY)", 
                    data.get('summary', ''), 
                    '', 
                    json.dumps(ws_dict, ensure_ascii=False), 
                    datetime.datetime.now().isoformat()
                )
            )
            print(f"Anchor for Ep {target_ep} Saved.")
            return True
            
        except Exception as e:
            print(f"Anchor Gen Error Ep {target_ep}: {e}")
            return False

    async def write_episodes(self, book_data, start_ep, end_ep, style_dna_str="style_web_standard", target_model=MODEL_LITE, semaphore=None):
        """
        1エピソード1リクエスト化: 本文・要約・Bible更新を一括実行
        ContextBuilderとNovelRepositoryによる最適化済み
        """
        # 使用する Repository インスタンス
        # Note: book_data is passed as dict, so we use self.repo for DB access if needed, but here we process dicts
        
        all_plots = sorted(book_data['plots'], key=lambda x: x.get('ep_num', 999))
        target_plots = [p for p in all_plots if start_ep <= p.get('ep_num', -1) <= end_ep]
        if not target_plots: return None

        full_chapters = []
        bible_synchronizer = BibleSynchronizer(book_data['book_id'])
        bible_manager = bible_synchronizer.bible_manager
        
        # CharacterRegistry 構築
        try:
            char_registry = CharacterRegistry(**book_data['mc_profile'])
        except:
            char_registry = CharacterRegistry(name="主人公", tone="標準", personality="", ability="", monologue_style="", pronouns="{}", keyword_dictionary="{}", relations="{}", dialogue_samples="{}")
        
        # 前話の文脈取得 (Repository経由)
        # Note: get_latest_chapter now fetches world_state too.
        prev_ep_row = await self.repo.get_latest_chapter(book_data['book_id'], start_ep)
        prev_context_text = prev_ep_row['content'][-500:] if prev_ep_row and prev_ep_row['content'] else "（物語開始）"

        # 前話のラスト1文（句点まで）を取得
        prev_last_sentence = ""
        if prev_ep_row and prev_ep_row['content']:
            content_str = prev_ep_row['content'].strip()
            # 簡易的に最後の句点を探す。見つからなければ末尾20文字
            match = re.search(r'[^。]+。$', content_str)
            if match:
                prev_last_sentence = match.group(0)
            else:
                prev_last_sentence = content_str[-20:]

        # Style Instructionの準備
        style_instruction = self.prompt_manager.apply_style(style_dna_str)
        
        for plot in target_plots:
            ep_num = plot['ep_num']
            print(f"Hyper-Narrative Engine Writing Ep {ep_num}...")
            
            # Pacing Graph Analysis (Structural)
            pacing_data = await PacingGraph.analyze(book_data['book_id'], ep_num, total_eps=50)
            pacing_instruction = pacing_data['instruction']
            pacing_graph = pacing_data.get('graph_visualization', '')
            gen_temp = pacing_data['temperature']

            current_model = target_model
            if (1 <= ep_num <= 5) or ep_num == 50 or plot.get('tension', 50) >= 80:
                current_model = MODEL_PRO
            
            scenes_str = ""
            if isinstance(plot.get('scenes'), list):
                for s in plot['scenes']:
                    # Handle both Dict and SceneDetail object
                    s_dict = s.model_dump() if hasattr(s, 'model_dump') else s
                    scenes_str += f"- {s_dict.get('location','')}: {s_dict.get('action','')} ({s_dict.get('dialogue_point','')} - {s_dict.get('role', '')})\n"

            # 詳細プロット(detailed_blueprint)の利用
            blueprint_str = plot.get('detailed_blueprint', '')
            
            episode_plot_text = f"""
【Episode Title】{plot['title']}
【Detailed Blueprint (500文字以上の詳細設計図)】
{blueprint_str}

【Setup】 {plot.get('setup', '')}
【Conflict】 {plot.get('conflict', '')}
【Climax】 {plot.get('climax', '')}
【Next Hook (No Resolution)】 {plot.get('next_hook', '')}
【Scenes】
{scenes_str}
"""
            
            world_state, expected_version = await bible_manager.get_current_state()
            bible_context = await bible_manager.get_prompt_context()
            
            entity_context = char_registry.get_context_prompt()

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

            async with semaphore:
                # ContextBuilderを使用してプロンプトを構築
                write_prompt = self.prompt_manager.build_writing_prompt(
                    mc_name=char_registry.name,
                    mc_tone=char_registry.tone,
                    pronouns=char_registry.pronouns,
                    relations=char_registry.relations,
                    mc_dialogue_samples=char_registry.dialogue_samples,
                    style_instruction=style_instruction,
                    entity_context=entity_context,
                    pacing_instruction=pacing_instruction,
                    pacing_graph=pacing_graph,
                    prev_last_sentence=prev_last_sentence, # 強制接続用
                    # Template params
                    current_model=current_model,
                    ep_num=ep_num,
                    pending_foreshadowing=json.dumps(world_state.pending_foreshadowing, ensure_ascii=False),
                    must_resolve_instruction=must_resolve_instruction,
                    prev_context_text=prev_context_text,
                    episode_plot_text=episode_plot_text,
                    expected_version=expected_version,
                    bible_context=bible_context
                )
                
                gen_config_args = {"temperature": gen_temp, "safety_settings": self.safety_settings}
                if "gemini" in current_model.lower() and "gemma" not in current_model.lower():
                    gen_config_args["response_mime_type"] = "application/json"
                
                # リトライループの実装 (最大5回に強化)
                retry_count = 0
                max_retries = 5
                best_attempt = None # ベストエフォート用

                while retry_count < max_retries:
                    try:
                        # TPM 対策: 実行前に少し待つ
                        await asyncio.sleep(5.0) 

                        res = await self._generate_with_retry(
                            model=current_model, 
                            contents=write_prompt,
                            config=types.GenerateContentConfig(**gen_config_args)
                        )
                        
                        text_content = res.text.strip()
                        if not text_content:
                            raise ValueError("No text content returned from API")
                        
                        ep_data = self._parse_json_response(text_content)
                        
                        # ベストエフォート更新ロジック
                        current_score = ep_data.get('self_evaluation_score', 0)
                        if best_attempt is None or current_score > best_attempt['score']:
                            best_attempt = {
                                "score": current_score,
                                "content": ep_data['content'],
                                "summary": ep_data['summary'],
                                "data": ep_data # world_state含む
                            }

                        # Quality Gate Logic (Threshold check)
                        # 90点 -> 80点 に緩和
                        threshold = 80 if 1 <= ep_num <= 5 else 70
                        
                        if current_score < threshold:
                             reason = ep_data.get('low_quality_reason', '理由不明')
                             print(f"⚠️ Low Quality Detected (Score: {current_score}/{threshold}): {reason}. Triggering Retry...")
                             # ここで例外を投げると、外側の except ブロックで捕捉され、retry_count が増えて再生成される
                             raise ValueError(f"Self-evaluated score is too low ({current_score} < {threshold}). Reason: {reason}")
                        
                        # 合格時の処理
                        full_content = ep_data['content']
                        # 強制接続: 重複排除
                        full_content = self.formatter.force_connect(full_content, prev_last_sentence)
                        ep_summary = ep_data['summary']
                        
                        # Bible Sync
                        next_state_obj = WorldState(**ep_data['next_world_state']) if isinstance(ep_data['next_world_state'], dict) else ep_data['next_world_state']
                        
                        chapter_save_data = {
                            'ep_num': ep_num,
                            'title': plot['title'],
                            'content': full_content,
                            'summary': ep_summary
                        }
                        
                        await bible_synchronizer.save_atomic(chapter_save_data, next_state_obj)
                        
                        prev_context_text = f"（第{ep_num}話要約）{ep_summary}\n（直近の文）{full_content[-200:]}"
                        content_str = full_content.strip()
                        match = re.search(r'[^。]+。$', content_str)
                        if match:
                            prev_last_sentence = match.group(0)
                        else:
                            prev_last_sentence = content_str[-20:]

                        full_chapters.append({
                            "ep_num": ep_num,
                            "title": plot['title'],
                            "content": full_content,
                            "summary": ep_summary,
                            "world_state": ep_data.get('next_world_state', {})
                        })
                        
                        # 成功したらループを抜ける
                        break

                    except Exception as e:
                        retry_count += 1
                        print(f"Writing Error Ep{ep_num} (Attempt {retry_count}/{max_retries}): {e}")
                        
                        if retry_count >= max_retries:
                            # 上限到達時：ベストエフォートがあれば採用 (これが救済措置)
                            if best_attempt:
                                print(f"⚠️ Adopting Best Effort (Score: {best_attempt['score']}) for Ep {ep_num}")
                                full_content = best_attempt['content']
                                full_content = self.formatter.force_connect(full_content, prev_last_sentence)
                                ep_summary = best_attempt['summary']
                                
                                next_state_data = best_attempt['data'].get('next_world_state', {})
                                next_state_obj = WorldState(**next_state_data) if isinstance(next_state_data, dict) else next_state_data
                                
                                chapter_save_data = {
                                    'ep_num': ep_num,
                                    'title': plot['title'],
                                    'content': full_content,
                                    'summary': ep_summary
                                }
                                
                                await bible_synchronizer.save_atomic(chapter_save_data, next_state_obj)
                                
                                # コンテキスト更新 (次話のため)
                                prev_context_text = f"（第{ep_num}話要約）{ep_summary}\n（直近の文）{full_content[-200:]}"
                                content_str = full_content.strip()
                                match = re.search(r'[^。]+。$', content_str)
                                if match:
                                    prev_last_sentence = match.group(0)
                                else:
                                    prev_last_sentence = content_str[-20:]

                                full_chapters.append({
                                    "ep_num": ep_num,
                                    "title": plot['title'],
                                    "content": full_content,
                                    "summary": ep_summary,
                                    "world_state": best_attempt['data'].get('next_world_state', {})
                                })
                            else:
                                # 本当に何も生成できなかった場合（JSONエラー等でベストエフォートすらない場合）
                                # DBにエラー情報を保存する (改善策1)
                                await self.repo.db.save_model(
                                    """INSERT OR REPLACE INTO chapters (book_id, ep_num, title, content, summary, ai_insight, world_state, created_at)
                                    VALUES (?,?,?,?,?,?,?,?)""",
                                    (self.repo.db.db_path, ep_num, plot['title'], "（生成エラー：リトライ上限到達）", "エラー", '', json.dumps({}, ensure_ascii=False), datetime.datetime.now().isoformat())
                                )
                                
                                full_chapters.append({
                                    "ep_num": ep_num,
                                    "title": plot['title'],
                                    "content": "（生成エラーが発生しました）",
                                    "summary": "エラー",
                                    "world_state": {}
                                })
                        else:
                            # 即座にエラー埋め込みを行わず、待機して再試行
                            await asyncio.sleep(2)

        return {"chapters": full_chapters}

    async def save_blueprint_to_db(self, data, genre, style_dna_str):
        # Delegate to Repository
        return await self.repo.create_novel(data, genre, style_dna_str)

    async def save_additional_plots_to_db(self, book_id, data_p2):
        # Delegate to Repository
        return await self.repo.add_plots(book_id, data_p2)

# ==========================================
# Task Functions (Updated to use Repository)
# ==========================================
async def task_write_batch(engine, bid, start_ep, end_ep):
    repo = engine.repo # Use engine's repo
    book_info = await repo.get_book(bid)
    plots = await repo.get_plots(bid)
    mc = await repo.get_main_character(bid)

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
             mc_profile = {"name":"主人公", "tone":"標準", "personality":"", "ability":"", "monologue_style":"", "pronouns":"{}", "keyword_dictionary":"{}", "relations":"{}", "dialogue_samples":"{}"}
    else:
        mc_profile = {"name":"主人公", "tone":"標準", "personality":"", "ability":"", "monologue_style":"", "pronouns":"{}", "keyword_dictionary":"{}", "relations":"{}", "dialogue_samples":"{}"}

    # plots前処理 (Dict化)
    processed_plots = []
    for p in plots:
        p_dict = dict(p)
        if p_dict.get('scenes'):
            try: p_dict['scenes'] = json.loads(p_dict['scenes'])
            except: pass
        if 'resolution' in p_dict:
             p_dict['next_hook'] = p_dict['resolution']
        processed_plots.append(p_dict)

    full_data = {"book_id": bid, "title": book_info['title'], "mc_profile": mc_profile, "plots": processed_plots}
    
    # 【指令対応】ダイナミック・アンカー・スケジューリング & 並列化
    # リスト: [15, 25, 35, 45, 55...]
    anchors = [15] + [i for i in range(25, 201, 10)]
    
    # 今回の範囲に含まれるアンカーを抽出 (start_ep ~ end_ep)
    relevant_anchors = [a for a in anchors if start_ep <= a < end_ep] 
    
    # 1. アンカー状態の先行生成 (もしDBになければ生成)
    # 2段階生成で既にアンカーがDBに入っている可能性がある。
    # しかし、Call 1 で生成したアンカーは `chapters` テーブルに保存されているはず。
    # ここでは「DBにない場合のみ生成」するロジックにするのが安全。
    # generate_anchor_state will overwrite, which is acceptable if we want to refresh,
    # but since we generated them in phase 1, we might skip.
    # However, Phase 1 only generated anchors up to Ep 50 (based on Prompt). 
    # If this batch goes beyond, we need generation.
    # Also, Phase 1 saves anchors to `data1.anchors` which `main` saves to DB.
    # So we assume they are in DB. We can skip generation if they exist.
    # For simplicity and robustness, we can let it run (it might be redundant but safe).
    # Actually, `generate_anchor_state` uses `MODEL_ULTRALONG` and consumes tokens.
    # If they are already saved by `main`, we should skip.
    
    for anchor in relevant_anchors:
        # Check if anchor exists
        existing = await repo.get_latest_chapter(bid, anchor + 1) # get_latest_chapter(ep) returns ep-1. We want anchor at `anchor`.
        # Wait, get_latest_chapter(16) returns 15. Correct.
        # But we saved anchor as "ANCHOR_EP_15".
        # Let's check.
        # Actually, let's just generate if needed. The cost is low compared to writing.
        # But wait, we want to use the consistency from Phase 1.
        # If we regenerate, we might lose the Phase 1 consistency.
        # So we should ONLY generate if missing.
        
        # Check existence logic:
        chk = await db.fetch_one("SELECT id FROM chapters WHERE book_id=? AND ep_num=?", (bid, anchor))
        if not chk:
             await engine.generate_anchor_state(full_data, anchor)

    # 2. タスク分割 (Split Threads)
    # Create segment boundaries
    boundaries = sorted([start_ep - 1] + relevant_anchors + [end_ep])
    boundaries = sorted(list(set(boundaries)))
    
    # Build ranges: (boundaries[i]+1, boundaries[i+1])
    ranges = []
    for i in range(len(boundaries)-1):
        s = boundaries[i] + 1
        e = boundaries[i+1]
        if s <= e:
            ranges.append((s, e))
    
    print(f"Parallel Schedule: {ranges}")
    
    # 並列数を増やす (Parallel Execution)
    semaphore = asyncio.Semaphore(5) # Increase concurrency for parallel blocks

    for s, e in ranges:
        tasks.append(engine.write_episodes(
            full_data, 
            s, 
            e, 
            style_dna_str=saved_style, 
            target_model=MODEL_LITE, 
            semaphore=semaphore
        ))

    results = await asyncio.gather(*tasks)

    total_count = 0
    for res in results:
        if res and 'chapters' in res:
            total_count += len(res['chapters'])
            
    print(f"Batch Done (Ep {start_ep}-{end_ep}). Total Episodes Written: {total_count}")
    return total_count, full_data, saved_style

# ==========================================
# 3. Main Logic
# ==========================================

async def create_zip_package(book_id, title):
    print("Packing ZIP...")
    buffer = io.BytesIO()
    
    # Repository経由でデータ取得
    repo = NovelRepository(db)
    current_book = await repo.get_book(book_id)
    db_chars = await repo.get_characters(book_id)
    db_plots = await repo.get_plots(book_id)
    chapters = await repo.get_chapters(book_id)
    
    # マーケティングデータの取得
    marketing_data = {}
    if current_book.get('marketing_data'):
        try:
                marketing_data = json.loads(current_book['marketing_data'])
        except: pass

    def clean_filename_title(t):
        return re.sub(r'[\\/:*?"<>|]', '', re.sub(r'^第\d+話[\s　]*', '', t)).strip()

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
            plot_txt += f"・詳細設計図: {p.get('detailed_blueprint', '')}\n"
            plot_txt += f"・導入 (Setup): {p.get('setup', '')}\n"
            plot_txt += f"・展開 (Conflict): {p.get('conflict', '')}\n"
            plot_txt += f"・見せ場 (Climax): {p.get('climax', '')}\n"
            plot_txt += f"・引き (Next Hook): {p.get('resolution', '')}\n" # Note: mapped to Resolution col
            plot_txt += f"・テンション: {p.get('tension', '-')}/100\n\n"
        z.writestr("00_全話プロット構成案.txt", plot_txt)

        for ch in chapters:
            # Skip anchor chapters in final zip
            if ch['title'].startswith("ANCHOR_EP_"):
                continue
                
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

    print("Starting Factory Pipeline (Infinite Loop Mode)...")

    while True:
        print(f"\n=== Starting New Novel Generation Sequence at {datetime.datetime.now()} ===")
        try:
            # Step 0: Trend Analysis (Static List Selection)
            seed = await engine.trend_analyst.get_dynamic_seed()
            
            # Step 1: 1-50話プロット + マーケティングアセット生成 (2-Stage)
            print("Step 1: Generating Full Series Plot (Ep 1-50) & Marketing Assets...")
            data1 = await engine.generate_universe_blueprint_phase1(
                seed['genre'], seed['style'], seed['personality'], seed['tone'], seed['keywords']
            )
            
            if not data1: 
                print("Plot Gen failed. Skipping to next cycle.")
                await asyncio.sleep(10)
                continue

            bid, plots_p1 = await engine.save_blueprint_to_db(data1, seed['genre'], seed['style'])
            print(f"Plot Phase Saved. ID: {bid}")
            
            # --- Save Pre-generated Anchors ---
            if hasattr(data1, 'anchors') and data1.anchors:
                print("Saving Pre-generated Anchors...")
                for anchor in data1.anchors:
                    # Normalized data format for saving
                    ws_data = anchor.world_state.model_dump()
                    # ensure stringification
                    if 'dependency_graph' in ws_data and isinstance(ws_data['dependency_graph'], (dict, list)):
                        ws_data['dependency_graph'] = json.dumps(ws_data['dependency_graph'], ensure_ascii=False)
                    
                    await engine.repo.db.save_model(
                        """INSERT OR REPLACE INTO chapters (book_id, ep_num, title, content, summary, ai_insight, world_state, created_at)
                           VALUES (?,?,?,?,?,?,?,?)""",
                        (
                            bid, 
                            anchor.ep_num, # Use ep_num from anchor
                            f"ANCHOR_EP_{anchor.ep_num}", 
                            "(ANCHOR_STATE_ONLY)", 
                            anchor.summary, 
                            '', 
                            json.dumps(ws_data, ensure_ascii=False), 
                            datetime.datetime.now().isoformat()
                        )
                    )
            
            print("Step 2: Execution - Writing Episodes (Ep 1-50)...")
            
            # 全50話執筆
            count_p1, full_data_final, saved_style = await task_write_batch(engine, bid, start_ep=1, end_ep=50)
            
            # Finalize
            print("Running Final Packaging...")
            
            # Repository経由でタイトル取得
            book_info = await engine.repo.get_book(bid)
            title = book_info['title']
            
            zip_bytes = await create_zip_package(bid, title)
            send_email(zip_bytes, title)
            
            print(f"Mission Complete: {title}. Moving to next creation in 60 seconds...")
            await asyncio.sleep(60)
            
        except Exception as e:
            print(f"Pipeline Critical Error: {e}")
            import traceback
            traceback.print_exc()
            print("Recovering... sleeping for 300 seconds before retry.")
            await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())