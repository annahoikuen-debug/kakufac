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
from contextlib import contextmanager
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
# 環境変数から取得
API_KEY = os.environ.get("GEMINI_API_KEY")
GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_PASS = os.environ.get("GMAIL_PASS")
TARGET_EMAIL = os.environ.get("GMAIL_USER") 

# モデル設定 (2026年仕様: Gemma 3 Limits Optimized)
MODEL_ULTRALONG = "gemini-2.0-flash"       # Gemini 2.0 Flash (プロット・高品質・スキーマ対応)
MODEL_LITE = "gemini-2.0-flash-lite"        # Gemma 3相当の軽量モデル（スキーマ対応のためGemini系推奨）
MODEL_PRO = "gemini-2.0-pro-exp"            # 高品質推論用

DB_FILE = "factory_run.db" # 自動実行用に一時DBへ変更

# Global Config: Rate Limits
MIN_REQUEST_INTERVAL = 0.5

# ==========================================
# Pydantic Schemas (構造化出力用)
# ==========================================
class PlotScene(BaseModel):
    setup: str = Field(..., description="導入")
    conflict: str = Field(..., description="展開")
    climax: str = Field(..., description="結末")

class PlotEpisode(BaseModel):
    ep_num: int
    title: str
    setup: str
    conflict: str
    climax: str
    resolution: str
    tension: int
    scenes: List[str]

class MCProfile(BaseModel):
    name: str
    tone: str
    personality: str
    ability: str
    monologue_style: str
    pronouns: Dict[str, str]
    keyword_dictionary: Dict[str, str]

class NovelStructure(BaseModel):
    title: str
    concept: str
    synopsis: str
    mc_profile: MCProfile
    plots: List[PlotEpisode]

class Phase2Structure(BaseModel):
    plots: List[PlotEpisode]

class WorldState(BaseModel):
    immutable: Dict[str, Any] = Field(default_factory=dict, description="不変設定（性別、物理法則など）")
    mutable: Dict[str, Any] = Field(default_factory=dict, description="可変設定（場所、ステータス、生死）")
    revealed: List[str] = Field(default_factory=list, description="読者に開示済みの設定リスト")

class SceneBlueprint(BaseModel):
    blueprint: str = Field(..., description="執筆用詳細設計図")
    required_info: str = Field(..., description="今回開示すべき最小限の情報")

class ConsistencyResult(BaseModel):
    is_consistent: bool = Field(..., description="設定矛盾がないか")
    fatal_errors: List[str] = Field(default_factory=list, description="致命的な矛盾")
    minor_errors: List[str] = Field(default_factory=list, description="軽微な矛盾")
    rewrite_needed: bool = Field(..., description="リライトが必要か")

class AnalysisResult(BaseModel):
    score_structure: int
    score_character: int
    score_hook: int
    score_volume: int
    total_score: int
    improvement_point: str

class MarketingAssets(BaseModel):
    evaluations: List[Dict[str, Any]] # 簡易化
    marketing_assets: Dict[str, Any]

# ==========================================
# プロンプト集約 (PROMPT_TEMPLATES)
# ==========================================
PROMPT_TEMPLATES = {
    "system_rules": """# SYSTEM RULES: STRICT ADHERENCE REQUIRED
1. [PRONOUNS] 主人公の一人称・二人称は以下を厳守せよ: {pronouns}
   ※「俺」設定なのに「僕」と言う等のキャラ崩壊は禁止する。
2. [KEYWORD DICTIONARY] 以下の用語・ルビ・特殊呼称を必ず使用せよ: {keywords}
3. [MONOLOGUE STYLE] 独白・心理描写は以下の癖を反映せよ: {monologue_style}
   ※単なる状況説明ではなく、主人公のフィルターを通した『歪んだ世界観』として情景を記述せよ。
4. [NARRATIVE STYLE] 地の文の文体・雰囲気: 「{style}」
   ※この文体を厳守し、描写のトーンを統一せよ。
5. [ANTI-CLICHÉ] 「――その時だった」「ふと、気づくと」「運命の歯車が」等のテンプレート表現を厳禁とする。代わりに、物理現象（影の伸び、気温、心拍数）の変化で事態の急変を描写せよ。
--------------------------------------------------
""",
    "writing_rules": """
【超重要: 執筆密度を究極まで高める鉄則】
AI特有の「要約癖」を完全に捨て、以下のルールで描写密度を最大化せよ。

1. **1話3シーン制**:
   1話を必ず「3つの異なるシーン（場所・時間の転換）」に分割して構成せよ。各シーン800文字以上を費やし、シーン間には「移動や時間経過」の描写を挟むこと。

2. **アクション・アンカー（予備動作）**:
   攻撃や移動などの動作描写では、結果を書く前に必ず**「予備動作（視線の動き、筋肉の緊張、呼吸、服の擦れる音）」を2行以上描写**し、スローモーションのようなリアリティを出すこと。

3. **ナラティブ・ループ**:
   会話シーンは**「1.台詞」→「2.その瞬間の心理」→「3.情景（風、光、音）」**の3点セットを繰り返す構造にすること。会話文だけで物語を進行させることを厳禁とする。

4. **Dynamic Pacing（動的演出）**:
   各話のプロット内にある『tension』値を参照して文体を変えよ。
   - **Tension 70以上**: 「視覚情報・短文中心」でスピード感を重視せよ。
   - **Tension 40以下**: 「心理描写・聴覚情報中心」で情緒と余韻を重視せよ。
""",
    "cliffhanger_protocol": """
【究極の「引き」生成ロジック: Cliffhanger Protocol】
各エピソードの結末は、文脈に応じて最も効果的な「引き」を自律的に判断し、**「読者が次を読まずにいられない状態」**を強制的に作り出せ。

1. **逆算式・ゴール地点固定**:
   - あなたは「結末の衝撃」から逆算して伏線を張る構成作家である。
   - 本文執筆前に、その話の**「最悪、あるいは最高の結末（最後の一行）」**を確定せよ。
   - その一行が読者に最大の衝撃を与えるよう、そこに至るまでの伏線、期待、誤認をシーン1・2に配置せよ。
   - 結末をぼかさないこと。予定調和な終わり方をしないこと。

2. **テンション・カタストロフィ**:
   - あなたは解決の1秒前に筆を置く、冷酷なディレクターである。
   - 絶体絶命の瞬間、あるいは秘密が暴かれる**「直前」で物語を強制終了**せよ。
   - 読者が「救い」や「納得」を得る記述を一切排除せよ。安心させず、解決しきらないこと。
""",
    "formatting_rules": """
【演出指示】
- 「三点リーダー（……）の後は、あえて改行して空白を作れ。その空白で読者の心拍数を上げろ。」
- 「最後の一行は、15文字以内の短い一文で、重く、鋭く言い放て。」
- 「解決策（チート能力の使用など）を思いついた瞬間にエピソードを切れ。」
"""
}

# ==========================================
# Formatter Class
# ==========================================
class TextFormatter:
    @staticmethod
    def format(text, k_dict=None):
        if not text: return ""
        text = text.replace("\\n", "\n")
        
        # 1. 不要タグ削除
        text = re.sub(r'^[■【\[#]?(?:パート|Part|part|Chapter|section|導入|本筋|結末|構成|要素).*$', '', text, flags=re.MULTILINE)
        text = re.sub(r'^\s*[-*]{3,}\s*$', '', text, flags=re.MULTILINE)
        text = re.sub(r'【読者の反応】.*$', '', text, flags=re.DOTALL)
        text = re.sub(r'```json.*?```', '', text, flags=re.DOTALL) 

        # 2. キーワード置換
        if k_dict:
            for term, ruby in k_dict.items():
                pattern = re.compile(re.escape(term) + r'(?!《)')
                text = pattern.sub(f"|{term}《{ruby}》", text)

        # 3. 記号正規化と作法徹底
        text = text.replace("|", "｜")
        # 三点リーダーの正規化（偶数個かつ2個以上）
        text = re.sub(r'…+', '……', text)
        text = text.replace('……', '……') # 念のため
        # ダッシュの正規化
        text = text.replace("——", "――").replace("--", "――").replace("―", "――")
        text = text.replace("――――", "――")
        
        text = re.sub(r'^[ \t　]+(?=「)', '', text, flags=re.MULTILINE)
        text = text.replace("｜", "|") # DB保存時は一旦半角に戻す

        # 4. 強制改行ロジック削除 (段落維持のみ)

        # 5. 行再構築（空行強制・字下げ）
        lines = []
        text = text.replace('\r\n', '\n')
        
        for line in text.split('\n'):
            line = line.strip()
            if not line: continue
            
            # セリフと地の文の処理
            if line.startswith(('「', '『', '（', '【', '<', '〈')):
                lines.append("") # セリフ前空行
                lines.append(line)
                lines.append("") # セリフ後空行
            else:
                lines.append(f"　{line}")
                lines.append("") # 段落後空行

        text = "\n".join(lines)

        # 6. 余分な空白の削除
        text = re.sub(r'\n{3,}', '\n\n', text)

        return text.strip()

# ==========================================
# 1. データベース管理
# ==========================================
class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_tables()

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def _init_tables(self):
        with self._get_conn() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, genre TEXT, concept TEXT,
                    synopsis TEXT, catchcopy TEXT, target_eps INTEGER, style_dna TEXT,
                    target_audience TEXT, special_ability TEXT DEFAULT '',
                    status TEXT DEFAULT 'active', created_at TEXT, marketing_data TEXT, sub_plots TEXT
                );
                CREATE TABLE IF NOT EXISTS bible (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, book_id INTEGER, 
                    immutable TEXT, mutable TEXT, revealed TEXT,
                    last_updated TEXT
                );
                CREATE TABLE IF NOT EXISTS plot (
                    book_id INTEGER, ep_num INTEGER, title TEXT, summary TEXT,
                    main_event TEXT, sub_event TEXT, pacing_type TEXT,
                    tension INTEGER DEFAULT 50, cliffhanger_score INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'planned', 
                    setup TEXT, conflict TEXT, climax TEXT, resolution TEXT,
                    scenes TEXT,
                    PRIMARY KEY(book_id, ep_num)
                );
                CREATE TABLE IF NOT EXISTS chapters (
                    book_id INTEGER, ep_num INTEGER, title TEXT, content TEXT,
                    score_story INTEGER, killer_phrase TEXT, reader_retention_score INTEGER,
                    ending_emotion TEXT, discomfort_score INTEGER DEFAULT 0, tags TEXT,
                    ai_insight TEXT, retention_data TEXT, summary TEXT, world_state TEXT,
                    created_at TEXT, PRIMARY KEY(book_id, ep_num)
                );
                CREATE TABLE IF NOT EXISTS characters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, book_id INTEGER, name TEXT, role TEXT, dna_json TEXT, monologue_style TEXT
                );
            ''')

    def execute(self, query, params=()):
        with self._get_conn() as conn:
            cursor = conn.execute(query, params)
            return cursor.lastrowid

    def fetch_all(self, query, params=()):
        with self._get_conn() as conn:
            return [dict(row) for row in conn.execute(query, params).fetchall()]
            
    def fetch_one(self, query, params=()):
        with self._get_conn() as conn:
            row = conn.execute(query, params).fetchone()
            return dict(row) if row else None

db = DatabaseManager(DB_FILE)

# ==========================================
# 2. Dynamic Bible System
# ==========================================
class DynamicBibleManager:
    def __init__(self, book_id):
        self.book_id = book_id
    
    def get_current_state(self) -> WorldState:
        row = db.fetch_one("SELECT * FROM bible WHERE book_id=? ORDER BY id DESC LIMIT 1", (self.book_id,))
        if not row:
            return WorldState()
        try:
            return WorldState(
                immutable=json.loads(row['immutable']) if row['immutable'] else {},
                mutable=json.loads(row['mutable']) if row['mutable'] else {},
                revealed=json.loads(row['revealed']) if row['revealed'] else []
            )
        except:
            return WorldState()

    def update_state(self, new_state: WorldState):
        db.execute(
            "INSERT INTO bible (book_id, immutable, mutable, revealed, last_updated) VALUES (?,?,?,?,?)",
            (
                self.book_id,
                json.dumps(new_state.immutable, ensure_ascii=False),
                json.dumps(new_state.mutable, ensure_ascii=False),
                json.dumps(new_state.revealed, ensure_ascii=False),
                datetime.datetime.now().isoformat()
            )
        )

    def get_prompt_context(self) -> str:
        state = self.get_current_state()
        return f"""
【WORLD STATE (Current)】
[IMMUTABLE - Do Not Change]: {json.dumps(state.immutable, ensure_ascii=False)}
[MUTABLE - Can Change]: {json.dumps(state.mutable, ensure_ascii=False)}
[REVEALED - Known to Reader]: {json.dumps(state.revealed, ensure_ascii=False)}
"""

# ==========================================
# 3. Adaptive Rate Limiter (Circuit Breaker)
# ==========================================
class AdaptiveRateLimiter:
    def __init__(self, initial_limit=5, min_limit=1):
        self.limit = initial_limit
        self.min_limit = min_limit
        self.semaphore = asyncio.Semaphore(initial_limit)
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        await self.semaphore.acquire()

    def release(self):
        self.semaphore.release()

    async def report_success(self):
        async with self.lock:
            if self.limit < 10: # Max limit cap
                self.limit += 1
                # Increase semaphore capacity strictly
                # (Simple implementations often just recreate semaphore or release extra, 
                # here we just rely on future acquires being faster if we could dynamically resize.
                # Since asyncio semaphore doesn't support resize easily, we accept strict backoff
                # but lazy expansion or just keep semantic limit high and use sleep).
                pass

    async def report_failure(self):
        async with self.lock:
            old_limit = self.limit
            self.limit = max(self.min_limit, self.limit // 2)
            print(f"📉 Circuit Breaker Triggered: Limit reduced {old_limit} -> {self.limit}")
            await asyncio.sleep(5) # Cooldown
            
            # Drain semaphore to match new limit is complex, 
            # instead we simply sleep to simulate backpressure.

# ==========================================
# 4. ULTRA Engine (Autopilot & Mobile Opt)
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

    def _generate_system_rules(self, mc_profile, style="標準"):
        pronouns_json = json.dumps(mc_profile.get('pronouns', {}), ensure_ascii=False)
        keywords_json = json.dumps(mc_profile.get('keyword_dictionary', {}), ensure_ascii=False)
        monologue = mc_profile.get('monologue_style', '標準')
        return PROMPT_TEMPLATES["system_rules"].format(pronouns=pronouns_json, keywords=keywords_json, monologue_style=monologue, style=style)

    # ---------------------------------------------------------
    # Retry Wrappers for Stability & Circuit Breaker
    # ---------------------------------------------------------
    async def _generate_with_retry(self, model, contents, config, retries=10, initial_delay=2.0):
        """非同期版: サーキットブレーカー付きリトライ"""
        await self.rate_limiter.acquire()
        try:
            for attempt in range(retries):
                try:
                    # スキーマがある場合は構造化モード
                    response = await self.client.aio.models.generate_content(
                        model=model, 
                        contents=contents, 
                        config=config
                    )
                    await self.rate_limiter.report_success()
                    return response
                except Exception as e:
                    error_str = str(e)
                    is_429 = "429" in error_str or "ResourceExhausted" in error_str
                    
                    if is_429:
                        await self.rate_limiter.report_failure()
                        wait_time = initial_delay * (2 ** attempt) + random.uniform(1, 3)
                        print(f"⚠️ Quota Limit. Sleeping {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)
                    else:
                        print(f"⚠️ API Error: {e}. Retrying...")
                        await asyncio.sleep(2)
            raise Exception("Max retries exceeded")
        finally:
            self.rate_limiter.release()

    # ---------------------------------------------------------
    # Core Logic
    # ---------------------------------------------------------

    async def generate_universe_blueprint_phase1(self, genre, style, mc_personality, mc_tone, keywords):
        """第1段階: 構造化出力を用いたプロット生成"""
        print("Step 1: Hyper-Resolution Plot Generation Phase 1 (Ep 1-13)...")
        
        prompt = f"""
あなたはWeb小説の神級プロットアーキテクトです。
ジャンル「{genre}」で、読者を熱狂させる**全25話完結の物語構造**を作成してください。

【ユーザー指定の絶対条件】
1. 文体: 「{style}」
2. 主人公: 性格{mc_personality}, 口調「{mc_tone}」
3. テーマ: {keywords}

【Task: Phase 1 (Ep 1-13)】
作品設定と、第1話〜第13話の詳細プロットを作成せよ。
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
            # Pydanticモデルとしてパースされた結果を辞書化
            return json.loads(res.text)
        except Exception as e:
            print(f"Plot Phase 1 Error: {e}")
            return None

    async def generate_universe_blueprint_phase2(self, genre, style, mc_personality, mc_tone, keywords, data1):
        """第2段階: 14話〜25話の生成"""
        print("Step 1 (Parallel): Hyper-Resolution Plot Generation Phase 2 (Ep 14-25)...")
        
        context_summ = "\n".join([f"Ep{p['ep_num']}: {p['resolution'][:50]}..." for p in data1['plots']])
        prompt = f"""
あなたはWeb小説の神級プロットアーキテクトです。
全25話完結の物語構造の後半を作成します。

【これまでの流れ (Ep1-13)】
{context_summ}

【Task: Phase 2 (Ep 14-25)】
前回の続きとして、第14話〜第25話（最終話）を作成せよ。
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
            print(f"Plot Phase 2 Error: {e}")
            return None

    async def evaluate_consistency(self, ep_text, bible_manager) -> ConsistencyResult:
        """【構造改革】リライト要否の論理判定"""
        state = bible_manager.get_current_state()
        prompt = f"""
あなたは物語の整合性を監査するAIロジックです。
以下のエピソード本文と「Bible（世界設定）」を比較し、矛盾を検出してください。

【Bible】
Immutable: {json.dumps(state.immutable, ensure_ascii=False)}
Mutable: {json.dumps(state.mutable, ensure_ascii=False)}

【Episode Text】
{ep_text[:3000]}... (Excerpt)

判定基準:
1. 死んだはずのキャラが生きていないか？
2. 設定された物理法則や能力に違反していないか？
3. キャラの口調や一人称（Bible外だが文脈で判断）が崩壊していないか？

重大な矛盾がある場合は rewrite_needed: true とせよ。
"""
        try:
            res = await self._generate_with_retry(
                model=MODEL_LITE,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=ConsistencyResult,
                    safety_settings=self.safety_settings
                )
            )
            return ConsistencyResult.model_validate_json(res.text)
        except Exception as e:
            print(f"Consistency Check Error: {e}")
            return ConsistencyResult(is_consistent=True, fatal_errors=[], minor_errors=[], rewrite_needed=False)

    async def sync_with_chapter(self, bible_manager, chapter_text):
        """【知能統合】本文からBibleを自動更新"""
        current = bible_manager.get_current_state()
        prompt = f"""
あなたはデータベース管理者です。
以下のエピソード本文から「新たに確定した設定」「変化したステータス」「読者に開示された秘密」を抽出し、
WorldStateを更新してください。

【Current State】
{json.dumps(current.model_dump(), ensure_ascii=False)}

【Episode Text】
{chapter_text}

Task:
1. Immutable: 基本的に変更なし。新事実があれば追加。
2. Mutable: 位置移動、アイテム増減、生死変化を反映。
3. Revealed: 本文中で読者に説明された用語や設定を追加。
"""
        try:
            res = await self._generate_with_retry(
                model=MODEL_LITE,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=WorldState,
                    safety_settings=self.safety_settings
                )
            )
            new_state = WorldState.model_validate_json(res.text)
            bible_manager.update_state(new_state)
        except Exception as e:
            print(f"Bible Sync Error: {e}")

    async def write_episodes(self, book_data, start_ep, end_ep, style_dna_str="標準", target_model=MODEL_LITE, rewrite_instruction=None, semaphore=None):
        """【執筆洗練】ハイパー・ナラティブ・エンジン"""
        
        all_plots = sorted(book_data['plots'], key=lambda x: x.get('ep_num', 999))
        target_plots = [p for p in all_plots if start_ep <= p.get('ep_num', -1) <= end_ep]
        if not target_plots: return None

        full_chapters = []
        bible_manager = DynamicBibleManager(book_data['book_id'])
        
        # 前話の文脈取得 (Bridge Logic用)
        prev_ep_row = db.fetch_one("SELECT content, summary FROM chapters WHERE book_id=? AND ep_num=? ORDER BY ep_num DESC LIMIT 1", (book_data['book_id'], start_ep - 1))
        prev_context_text = prev_ep_row['content'][-500:] if prev_ep_row and prev_ep_row['content'] else "（物語開始）"

        system_rules = self._generate_system_rules(book_data['mc_profile'], style=style_dna_str)
        mc_name = book_data['mc_profile'].get('name', '主人公')
        
        # Vocal Persona Setup
        vocab_filter = f"""
【Vocal Persona: {mc_name}】
- 知識レベル: 一般人レベル（専門用語は知らないこと）
- 禁止語彙: {json.dumps(book_data['mc_profile'].get('keyword_dictionary', {}), ensure_ascii=False)} 以外の難解な言葉
- 制約: このキャラクターが知り得ない情報は、地の文でも絶対に描写しないこと。
"""

        for plot in target_plots:
            ep_num = plot['ep_num']
            print(f"Hyper-Narrative Engine Writing Ep {ep_num}...")
            
            full_content = ""
            current_text_tail = prev_context_text
            
            scenes = plot.get('scenes', [plot.get('setup',''), plot.get('conflict',''), plot.get('climax','') + plot.get('resolution','')])
            
            for part_idx, scene_plot in enumerate(scenes, 1):
                # A. 情報開示制限 (Show, Don't Tell)
                bible_state = bible_manager.get_current_state()
                revealed_list = bible_state.revealed
                
                # --- Step 2: Segment Design (Gemma 3 27B) ---
                design_prompt = f"""
{system_rules}
{vocab_filter}
【Role: Architect (Gemma 3 27B)】
以下のプロットに基づき、シーンの「執筆用詳細設計図」と「情報開示戦略」を策定せよ。

【Current Scene Plot】
{scene_plot}
【Bible Context】
{bible_manager.get_prompt_context()}

【Constraint: Show, Don't Tell】
1. 読者に伝えるべき「新しい設定」をBibleから**1つだけ**選べ。(required_info)
2. 既に開示済みリスト（{json.dumps(revealed_list, ensure_ascii=False)}）にある情報は、説明せず当然の前提として扱え。
"""
                blueprint_data = None
                async with semaphore:
                    try:
                        res = await self._generate_with_retry(
                            model=MODEL_PRO, 
                            contents=design_prompt,
                            config=types.GenerateContentConfig(
                                response_mime_type="application/json",
                                response_schema=SceneBlueprint,
                                safety_settings=self.safety_settings
                            )
                        )
                        blueprint_data = SceneBlueprint.model_validate_json(res.text)
                    except Exception as e:
                        print(f"Design Error Ep{ep_num}-{part_idx}: {e}")
                        blueprint_data = SceneBlueprint(blueprint=scene_plot, required_info="なし")

                # --- Step 3: Focused Writing (Gemma 3 12B) ---
                # C. 論理的接続 (Bridge Logic)
                bridge_instruction = f"""
【Bridge Logic】
前シーンの末尾: "...{current_text_tail}"
指示: 前シーンの「感情の余韻」を冒頭一行目で引き継ぎ、なぜ次の場所に移動するのか、その「動機」を必ず描写せよ。
"""
                write_prompt = f"""
{system_rules}
{vocab_filter}
{bridge_instruction}
【Role: Writer (Gemma 3 12B)】
Blueprintに従い、シーンを執筆せよ。

【Blueprint】
{blueprint_data.blueprint}

【Mandatory New Info (Insert naturally)】
{blueprint_data.required_info}

【Rewrite Instruction (Marketing Feedback)】
{rewrite_instruction if rewrite_instruction else "特になし"}
"""
                scene_text = ""
                async with semaphore:
                    try:
                        res = await self._generate_with_retry(
                            model=MODEL_LITE, 
                            contents=write_prompt,
                            config=types.GenerateContentConfig(safety_settings=self.safety_settings) # Text Output
                        )
                        scene_text = res.text
                    except Exception as e:
                        print(f"Writing Error Ep{ep_num}-{part_idx}: {e}")

                cleaned_part = scene_text.strip()
                full_content += cleaned_part + "\n\n"
                current_text_tail = cleaned_part[-200:]

            # --- Step 4: Auto-Sync Bible ---
            await self.sync_with_chapter(bible_manager, full_content)

            # エピソード完了処理
            full_chapters.append({
                "ep_num": ep_num,
                "title": plot['title'],
                "content": full_content,
                "summary": plot.get('resolution', '')[:100],
                "world_state": bible_manager.get_current_state().model_dump()
            })

        return {"chapters": full_chapters}

    async def _summarize_chunk(self, text_chunk, start_ep, end_ep, prev_summary="", next_summary=""):
        """【内部ヘルパー】エピソード群を圧縮要約する"""
        prompt = f"""
【Task: Context Compression】 以下の第{start_ep}話〜第{end_ep}話の本文を、物語の重要ポイント（伏線・感情・結末）を漏らさず、全体で1000文字程度に「濃縮要約」せよ。

【Text Chunk (Ep{start_ep}-{end_ep})】
{text_chunk} 
"""
        try:
            res = await self._generate_with_retry(
                model=MODEL_LITE,
                contents=prompt,
                config=types.GenerateContentConfig(safety_settings=self.safety_settings)
            )
            return res.text.strip()
        except Exception as e:
            return text_chunk[:1000]

    async def analyze_and_create_assets(self, book_id):
        """【安定化】フィードバックループ統合"""
        print("Starting Recursive Analysis (Sliding Window)...")
        
        chapters = db.fetch_all("SELECT ep_num, title, summary, content FROM chapters WHERE book_id=? ORDER BY ep_num", (book_id,))
        book_info = db.fetch_one("SELECT title FROM books WHERE id=?", (book_id,))
        if not chapters: return [], [], None

        # コンテキスト圧縮
        chunk_size = 5
        summary_tasks = []
        for i in range(0, len(chapters), chunk_size):
            chunk = chapters[i : i + chunk_size]
            full_text = "\n".join([f"Ep{c['ep_num']} {c['title']}:\n{c['content']}" for c in chunk])
            summary_tasks.append(self._summarize_chunk(full_text, chunk[0]['ep_num'], chunk[-1]['ep_num']))
        
        compressed_summaries = await asyncio.gather(*summary_tasks)
        master_context = "\n\n".join(compressed_summaries)
        
        prompt = f"""
あなたはWeb小説の敏腕編集者兼マーケターです。
以下のタスクを一括実行し、JSONで出力せよ。

Task 1: 各話スコアリング & 改善提案
Task 2: マーケティング素材生成 (キャッチコピー、タグ、近況ノート)

【作品タイトル】{book_info['title']}
【物語全体ダイジェスト】
{master_context}
"""
        try:
            res = await self._generate_with_retry(
                model=MODEL_LITE,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=MarketingAssets,
                    safety_settings=self.safety_settings
                )
            )
            data = MarketingAssets.model_validate_json(res.text)
            
            # --- 構造改革: 閾値廃止と論理判定への移行 ---
            # ここではスコアも見るが、後のプロセスで evaluate_consistency を呼ぶためのリストアップを行う
            rewrite_target_eps = []
            bible_manager = DynamicBibleManager(book_id)
            
            for evaluation in data.evaluations:
                # 低スコアまたは "improvement_point" に重大な指摘がある場合
                ep_num = evaluation.get('ep_num')
                # ここでConsistency Checkを非同期で走らせるのも手だが、今回はリライト候補として挙げ、
                # リライトループ内で evaluate_consistency を呼ぶ設計とする。
                if evaluation.get('total_score', 0) < 60: # 最低限の足切り
                     rewrite_target_eps.append(ep_num)
            
            # DB更新
            db.execute("UPDATE books SET marketing_data=? WHERE id=?", (json.dumps(data.marketing_assets, ensure_ascii=False), book_id))
            
            return data.evaluations, rewrite_target_eps, data.marketing_assets
            
        except Exception as e:
            print(f"Analysis Error: {e}")
            return [], [], None

    async def rewrite_target_episodes(self, book_data, target_ep_ids, evaluations, style_dna_str="標準"):
        """【安定化】マーケティング・フィードバックループ"""
        rewritten_count = 0
        semaphore = asyncio.Semaphore(2) 
        
        eval_map = {e['ep_num']: e for e in evaluations}
        tasks = []

        bible_manager = DynamicBibleManager(book_data['book_id'])

        for ep_id in target_ep_ids:
            # 1. 整合性チェック (Consistency Check)
            chapter_row = db.fetch_one("SELECT content FROM chapters WHERE book_id=? AND ep_num=?", (book_data['book_id'], ep_id))
            consistency = await self.evaluate_consistency(chapter_row['content'], bible_manager)
            
            if not consistency.rewrite_needed and ep_id not in target_ep_ids:
                continue

            # 2. マージ: マーケティング指摘 + 整合性エラー
            eval_data = eval_map.get(ep_id, {})
            marketing_instruction = eval_data.get('improvement_point', "")
            consistency_instruction = f"矛盾修正: {','.join(consistency.fatal_errors)}" if consistency.fatal_errors else ""
            
            instruction = f"【編集指示】\n{marketing_instruction}\n{consistency_instruction}"
            
            tasks.append(self.write_episodes(
                book_data, 
                ep_id, 
                ep_id, 
                style_dna_str=style_dna_str, 
                target_model=MODEL_PRO, 
                rewrite_instruction=instruction,
                semaphore=semaphore
            ))
            
        results = await asyncio.gather(*tasks)
        
        for res in results:
            if res and 'chapters' in res:
                self.save_chapters_to_db(book_data['book_id'], res['chapters'])
                rewritten_count += 1
        
        return rewritten_count

    def save_blueprint_to_db(self, data, genre, style_dna_str):
        # Pydanticモデルから辞書へ
        if isinstance(data, dict): data_dict = data
        else: data_dict = data.model_dump() # Should not happen based on return type of generate_universe_blueprint_phase1 logic which returns dict
        
        # Phase1が辞書で返ってくるように修正済みだが念のため
        
        dna = json.dumps({
            "tone": data_dict['mc_profile']['tone'], 
            "personality": data_dict['mc_profile'].get('personality', ''),
            "style_mode": style_dna_str,
            "pov_type": "一人称"
        }, ensure_ascii=False)
        
        ability_val = data_dict['mc_profile'].get('ability', '')
        
        bid = db.execute(
            "INSERT INTO books (title, genre, synopsis, concept, target_eps, style_dna, status, special_ability, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (data_dict['title'], genre, data_dict['synopsis'], data_dict['concept'], 25, dna, 'active', ability_val, datetime.datetime.now().isoformat())
        )
        c_dna = json.dumps(data_dict['mc_profile'], ensure_ascii=False)
        monologue_val = data_dict['mc_profile'].get('monologue_style', '')
        db.execute("INSERT INTO characters (book_id, name, role, dna_json, monologue_style) VALUES (?,?,?,?,?)", (bid, data_dict['mc_profile']['name'], '主人公', c_dna, monologue_val))
        
        # Initial Bible Creation
        db.execute("INSERT INTO bible (book_id, immutable, mutable, revealed, last_updated) VALUES (?,?,?,?,?)",
                   (bid, "{}", "{}", "[]", datetime.datetime.now().isoformat()))

        saved_plots = []
        for p in data_dict['plots']:
            full_title = f"第{p['ep_num']}話 {p['title']}"
            main_ev = f"{p.get('setup','')}->{p.get('climax','')}"
            scenes_json = json.dumps(p.get('scenes', []), ensure_ascii=False)
            db.execute(
                """INSERT INTO plot (book_id, ep_num, title, main_event, setup, conflict, climax, resolution, tension, status, scenes)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (bid, p['ep_num'], full_title, main_ev, 
                 p.get('setup'), p.get('conflict'), p.get('climax'), p.get('resolution'), 
                 p.get('tension', 50), 'planned', scenes_json)
            )
            saved_plots.append(p)
        return bid, saved_plots

    def save_additional_plots_to_db(self, book_id, data_p2):
        saved_plots = []
        # data_p2 is dict (json.loads result)
        for p in data_p2['plots']:
            full_title = f"第{p['ep_num']}話 {p['title']}"
            main_ev = f"{p.get('setup','')}->{p.get('climax','')}"
            scenes_json = json.dumps(p.get('scenes', []), ensure_ascii=False)
            db.execute(
                """INSERT INTO plot (book_id, ep_num, title, main_event, setup, conflict, climax, resolution, tension, status, scenes)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (book_id, p['ep_num'], full_title, main_ev, 
                 p.get('setup'), p.get('conflict'), p.get('climax'), p.get('resolution'), 
                 p.get('tension', 50), 'planned', scenes_json)
            )
            saved_plots.append(p)
        return saved_plots

    def save_chapters_to_db(self, book_id, chapters_list):
        count = 0
        if not chapters_list: return 0
            
        for ch in chapters_list:
            content = TextFormatter.format(ch['content'])
            w_state = json.dumps(ch.get('world_state', {}), ensure_ascii=False) if ch.get('world_state') else ""

            db.execute(
                """INSERT OR REPLACE INTO chapters (book_id, ep_num, title, content, summary, ai_insight, world_state, created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (book_id, ch['ep_num'], ch.get('title', f"第{ch['ep_num']}話"), content, ch.get('summary', ''), '', w_state, datetime.datetime.now().isoformat())
            )
            db.execute("UPDATE plot SET status='completed' WHERE book_id=? AND ep_num=?", (book_id, ch['ep_num']))
            count += 1
        return count

# ==========================================
# Task Functions
# ==========================================
def mc_profile_str(mc_profile): return f"{mc_profile.get('name')} (性格:{mc_profile.get('personality')}, 口調:{mc_profile.get('tone')})"

async def task_plot_gen_phase2(engine, bid, genre, style, mc_personality, mc_tone, keywords, data1):
    print(f"Parallel Task: Generating Phase 2 for Book ID {bid}...")
    data2 = await engine.generate_universe_blueprint_phase2(genre, style, mc_personality, mc_tone, keywords, data1)

    if data2 and 'plots' in data2:
        saved_plots_p2 = engine.save_additional_plots_to_db(bid, data2)
        print(f"Phase 2 Plots Saved ({len(saved_plots_p2)} eps).")
        return data2['plots']
    else:
        print("Phase 2 Generation Failed.")
        return []

async def task_write_batch(engine, bid, start_ep, end_ep):
    book_info = db.fetch_one("SELECT * FROM books WHERE id=?", (bid,))
    plots = db.fetch_all("SELECT * FROM plot WHERE book_id=? ORDER BY ep_num", (bid,))
    mc = db.fetch_one("SELECT * FROM characters WHERE book_id=? AND role='主人公'", (bid,))

    try:
        style_dna_json = json.loads(book_info['style_dna'])
        saved_style = style_dna_json.get('style_mode', '標準')
    except:
        saved_style = '標準'
    mc_profile = json.loads(mc['dna_json']) if mc and mc['dna_json'] else {"name":"主人公", "tone":"標準"}
    mc_profile['monologue_style'] = mc.get('monologue_style', '') 

    for p in plots:
        if p.get('scenes'):
            try: p['scenes'] = json.loads(p['scenes'])
            except: pass

    full_data = {"book_id": bid, "title": book_info['title'], "mc_profile": mc_profile, "plots": [dict(p) for p in plots]}
    semaphore = asyncio.Semaphore(10)

    tasks = []
    print(f"Starting Machine-Gun Parallel Writing (Ep {start_ep} - {end_ep})...")

    target_plots = [p for p in plots if start_ep <= p['ep_num'] <= end_ep]

    for p in target_plots:
        ep_num = p['ep_num']
        tension = p.get('tension', 50)
        
        target_model = MODEL_LITE
        if tension >= 80 or ep_num == 1 or ep_num == 25:
            target_model = MODEL_PRO 
        else:
            target_model = MODEL_LITE
        
        tasks.append(engine.write_episodes(
            full_data, 
            ep_num, 
            ep_num, 
            style_dna_str=saved_style, 
            target_model=target_model, 
            semaphore=semaphore
        ))

    results = await asyncio.gather(*tasks)

    total_count = 0
    for res_data in results:
        if res_data and 'chapters' in res_data:
            c = engine.save_chapters_to_db(bid, res_data['chapters'])
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
# 3. Main Logic (Headless)
# ==========================================
def load_seed():
    if not os.path.exists("story_seeds.json"):
        return {
            "genre": "現代ダンジョン",
            "keywords": "配信, 事故, 無双",
            "personality": "冷静沈着",
            "tone": "俺",
            "hook_text": "配信切り忘れで世界最強がバレる",
            "style": "標準"
        }

    with open("story_seeds.json", "r", encoding='utf-8') as f:
        data = json.load(f)
        seed = random.choice(data['seeds'])
        tmpl = random.choice(seed['templates'])
        twists = ["記憶喪失", "実は2周目", "相棒がラスボス", "寿命が残りわずか"]
        twist = random.choice(twists)
        
        print(f"★ Selected: {seed['genre']} - {tmpl['type']}")
        return {
            "genre": seed['genre'],
            "keywords": f"{tmpl['keywords']}, {twist}",
            "personality": tmpl['mc_profile'],
            "tone": "俺",
            "hook_text": tmpl['hook'],
            "style": "標準"
        }

def create_zip_package(book_id, title, marketing_data):
    print("Packing ZIP...")
    buffer = io.BytesIO()

    current_book = db.fetch_one("SELECT * FROM books WHERE id=?", (book_id,))
    db_chars = db.fetch_all("SELECT * FROM characters WHERE book_id=?", (book_id,))
    db_plots = db.fetch_all("SELECT * FROM plot WHERE book_id=? ORDER BY ep_num", (book_id,))
    chapters = db.fetch_all("SELECT * FROM chapters WHERE book_id=? ORDER BY ep_num", (book_id,))

    def clean_filename_title(t):
        return re.sub(r'[\\/:*?"<>|]', '', re.sub(r'^第\d+話[\s　]*', '', t)).strip()

    keyword_dict = {}
    mc_char = next((c for c in db_chars if c['role'] == '主人公'), None)
    if mc_char:
        try:
            dna = json.loads(mc_char['dna_json'])
            keyword_dict = dna.get('keyword_dictionary', {})
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
                dna = json.loads(char['dna_json'])
                for k, v in dna.items():
                    if k not in ['name', 'role', 'monologue_style']:
                        val_str = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)
                        setting_txt += f"  - {k}: {val_str}\n"
            except:
                setting_txt += f"  - 設定データ: {char['dna_json']}\n"
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
            plot_txt += f"・結末 (Resolution): {p.get('resolution', '')}\n"
            plot_txt += f"・テンション: {p.get('tension', '-')}/100\n\n"
        z.writestr("00_全話プロット構成案.txt", plot_txt)

        for ch in chapters:
            clean_title = clean_filename_title(ch['title'])
            fname = f"chapters/{ch['ep_num']:02d}_{clean_title}.txt"
            body = TextFormatter.format(ch['content'], k_dict=keyword_dict)
            z.writestr(fname, body)
        
        if marketing_data:
            kinkyo = marketing_data.get('kinkyo_note', '')
            if kinkyo:
                z.writestr("00_近況ノート.txt", kinkyo)
            
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

    engine = UltraEngine(API_KEY)

    print("Starting Factory Pipeline (Async / Structural Output)...")

    while True:
        try:
            seed = load_seed()
            
            print("Step 1a: Generating Plot Phase 1...")
            data1 = await engine.generate_universe_blueprint_phase1(
                seed['genre'], seed['style'], seed['personality'], seed['tone'], seed['keywords']
            )
            
            if not data1: 
                print("Plot Gen Phase 1 failed. Retrying in 10s...")
                await asyncio.sleep(1)
                continue

            bid, plots_p1 = engine.save_blueprint_to_db(data1, seed['genre'], seed['style'])
            print(f"Phase 1 Saved. ID: {bid}")
            
            print("Step 2: Starting Parallel Execution (Write P1 vs Gen P2)...")
            
            task_write_p1 = asyncio.create_task(
                task_write_batch(engine, bid, start_ep=1, end_ep=13)
            )
            
            task_gen_p2 = asyncio.create_task(
                task_plot_gen_phase2(
                    engine, bid, seed['genre'], seed['style'], seed['personality'], seed['tone'], seed['keywords'], data1
                )
            )
            
            count_p1, full_data_p1, saved_style = await task_write_p1
            await task_gen_p2
            
            print("Parallel Execution Completed. Proceeding to Write Phase 2...")

            count_p2, full_data_final, _ = await task_write_batch(engine, bid, start_ep=14, end_ep=25)
            
            full_data = full_data_final 

            evals, rewrite_targets, assets = await task_analyze_marketing(engine, bid)
            print(f"Rewriting Targets (Consistency & Low Score): {rewrite_targets}")

            if rewrite_targets:
                await task_rewrite(engine, full_data, rewrite_targets, evals, saved_style)

            book_info = db.fetch_one("SELECT title FROM books WHERE id=?", (bid,))
            title = book_info['title']
            
            zip_bytes = create_zip_package(bid, title, assets)
            send_email(zip_bytes, title)
            print(f"Mission Complete: {title}. Sleeping for next run...")
            
            await asyncio.sleep(10) 

        except Exception as e:
            print(f"Pipeline Critical Error: {e}")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())