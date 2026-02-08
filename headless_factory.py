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
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from google import genai
from google.genai import types

# ==========================================
# 0. 設定 & 2026年仕様 (Headless)
# ==========================================
# 環境変数から取得
API_KEY = os.environ.get("GEMINI_API_KEY")
GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_PASS = os.environ.get("GMAIL_PASS")
TARGET_EMAIL = os.environ.get("GMAIL_USER") 

# モデル設定 (2026年仕様: Gemma 3 Limits Optimized)
MODEL_ULTRALONG = "gemini-2.5-flash"      # Gemini 2.5 Flash (プロット・高品質用)
MODEL_LITE = "gemma-3-12b-it"             # Gemma 3 12B (量産の馬: 初稿・通常回用)
MODEL_PRO = "gemma-3-27b-it"              # Gemma 3 27B (エースの筆: 推敲・重要回用)
MODEL_EMBEDDING = "Gemma 3 2B"  # Gemma Embedding

DB_FILE = "factory_run.db" # 自動実行用に一時DBへ変更
REWRITE_THRESHOLD = 70  # リライト閾値

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
                    id INTEGER PRIMARY KEY AUTOINCREMENT, book_id INTEGER, content TEXT,
                    terminology_map TEXT, history_log TEXT, last_updated TEXT
                );
                CREATE TABLE IF NOT EXISTS plot (
                    book_id INTEGER, ep_num INTEGER, title TEXT, summary TEXT,
                    main_event TEXT, sub_event TEXT, pacing_type TEXT,
                    tension INTEGER DEFAULT 50, cliffhanger_score INTEGER DEFAULT 0,
                    stress_level INTEGER DEFAULT 0, cumulative_stress INTEGER DEFAULT 0,
                    love_meter INTEGER DEFAULT 0,
                    is_catharsis BOOLEAN DEFAULT 0, catharsis_type TEXT DEFAULT 'なし',
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
                CREATE TABLE IF NOT EXISTS plot_vectors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, book_id INTEGER, ep_num INTEGER, 
                    chunk_id INTEGER, content TEXT, vector_json TEXT
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
# 2. ULTRA Engine (Autopilot & Mobile Opt)
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

    def _clean_json(self, text):
        if not text: return None
        try:
            cleaned = re.sub(r'```json\n?|```', '', text).strip()
            match = re.search(r'\{.*\}', cleaned, re.DOTALL)
            if match:
                return json.loads(match.group(0))
            return json.loads(cleaned)
        except:
            try:
                if cleaned.count('{') > cleaned.count('}'):
                    cleaned += '}' * (cleaned.count('{') - cleaned.count('}'))
                if cleaned.count('[') > cleaned.count(']'):
                    cleaned += ']' * (cleaned.count('[') - cleaned.count(']'))
                return json.loads(cleaned)
            except:
                return None

    def _validate_world_state(self, current_state, new_state_update):
        """WorldState矛盾チェックバリデータ"""
        if not current_state or not new_state_update: return
        
        # 1. 生存フラグチェック
        if 'is_alive' in current_state and 'is_alive' in new_state_update:
            if not current_state['is_alive'] and new_state_update['is_alive']:
                print(f"⚠️ [CRITICAL WARNING] DB State Conflict: Dead character revived! {current_state} vs {new_state_update}")
        
        # 2. 所持品矛盾チェック (簡易版)
        if 'inventory' in current_state and 'inventory' in new_state_update:
            # ここにロジックを追加可能
            pass 

    def _generate_system_rules(self, mc_profile, style="標準"):
        pronouns_json = json.dumps(mc_profile.get('pronouns', {}), ensure_ascii=False)
        keywords_json = json.dumps(mc_profile.get('keyword_dictionary', {}), ensure_ascii=False)
        monologue = mc_profile.get('monologue_style', '標準')
        return PROMPT_TEMPLATES["system_rules"].format(pronouns=pronouns_json, keywords=keywords_json, monologue_style=monologue, style=style)

    def _get_embedding(self, text):
        """Get embedding vector using Gemma Embedding model"""
        try:
            result = self.client.models.embed_content(
                model=MODEL_EMBEDDING,
                contents=text,
                config=types.EmbedContentConfig(
                    task_type="RETRIEVAL_DOCUMENT"
                )
            )
            return result.embeddings[0].values
        except Exception as e:
            print(f"Embedding Error: {e}")
            return [0.0] * 768

    def _cosine_similarity(self, v1, v2):
        """Calculate cosine similarity between two vectors"""
        dot_product = sum(a * b for a, b in zip(v1, v2))
        magnitude_v1 = math.sqrt(sum(a * a for a in v1))
        magnitude_v2 = math.sqrt(sum(b * b for b in v2))
        if magnitude_v1 == 0 or magnitude_v2 == 0: return 0.0
        return dot_product / (magnitude_v1 * magnitude_v2)

    def _save_plot_embeddings(self, book_id, plots):
        """Chunk plots and save embeddings to SQLite (Gemma Embedding)"""
        print("Vectorizing plots...")
        for p in plots:
            # プロット情報を結合してチャンク化（300文字単位）
            full_text = f"第{p['ep_num']}話 {p['title']}\n{p['setup']}\n{p['conflict']}\n{p['climax']}\n{p['resolution']}"
            chunks = [full_text[i:i+300] for i in range(0, len(full_text), 300)]
            
            for idx, chunk in enumerate(chunks):
                vector = self._get_embedding(chunk)
                db.execute(
                    "INSERT INTO plot_vectors (book_id, ep_num, chunk_id, content, vector_json) VALUES (?,?,?,?,?)",
                    (book_id, p['ep_num'], idx, chunk, json.dumps(vector))
                )
        print(f"Vectorization complete for {len(plots)} episodes.")

    def get_next_context(self, book_id, current_text_tail, n_results=3):
        """Retrieve relevant plot fragments using Vector Search"""
        if not current_text_tail: return ""
        
        query_vector = self._get_embedding(current_text_tail)
        all_vectors = db.fetch_all("SELECT content, vector_json FROM plot_vectors WHERE book_id=?", (book_id,))
        
        scored = []
        for row in all_vectors:
            try:
                v = json.loads(row['vector_json'])
                score = self._cosine_similarity(query_vector, v)
                scored.append((score, row['content']))
            except: continue
            
        scored.sort(key=lambda x: x[0], reverse=True)
        top_k = [item[1] for item in scored[:n_results]]
        return "\n---\n".join(top_k)

    def generate_universe_blueprint_full(self, genre, style, mc_personality, mc_tone, keywords):
        """全25話の構成を2段階（1-13, 14-25）で生成し、Ki-Sho-Ten-Ketsu-Hiki構造を採用"""
        print("Step 1: Hyper-Resolution Plot Generation (Gemini 2.5 Flash)...")
        theme_instruction = f"【最重要テーマ・伏線指示】\nこの物語全体を貫くテーマ: {keywords}"
        
        core_instruction = f"""
あなたはWeb小説の神級プロットアーキテクトです。
ジャンル「{genre}」で、読者を熱狂させる**全25話完結の物語構造**を作成してください。
Gemini 2.5 Flashの能力を最大限活かし、各話2,000文字相当の情報量を持つプロットを生成せよ。

【ユーザー指定の絶対条件】
1. 文体: 「{style}」
2. 主人公: 性格{mc_personality}, 口調「{mc_tone}」
{theme_instruction}

【構成指針: 2段階生成ロジック】
- 第1段階: 1話〜13話（セットアップから中盤の転換点まで）
- 第2段階: 14話〜25話（クライマックスからエンディングまで）
- 各話構成: 「起(Intro)・承(Development)・転(Twist)・結(Conclusion)・引き(Cliffhanger)」の5要素を記述。
- インデックス: Embedding用に各話を「Scene 1(起承)」「Scene 2(転)」「Scene 3(結引き)」に分類可能にせよ。
"""

        # --- Phase 1: 1-13話 ---
        prompt1 = f"""
{core_instruction}

【Task: Phase 1 (Ep 1-13)】
作品設定と、第1話〜第13話の詳細プロットを作成せよ。
各話は以下のJSON構造を厳守すること。

出力フォーマット(JSON):
{{
  "title": "作品タイトル",
  "concept": "作品コンセプト",
  "synopsis": "全体あらすじ",
  "mc_profile": {{
    "name": "主人公名",
    "tone": "{mc_tone}", 
    "personality": "{mc_personality}",
    "ability": "スキル詳細",
    "monologue_style": "...",
    "pronouns": {{ "self": "俺/私", "others": "お前/君" }},
    "keyword_dictionary": {{ "用語": "ルビ" }}
  }},
  "plots": [
    {{
      "ep_num": 1,
      "title": "...",
      "setup": "【起:Intro】...", 
      "conflict": "【承:Development】...", 
      "climax": "【転:Twist】...", 
      "resolution": "【結:Conclusion & 引き:Cliffhanger】...",
      "tension": 90,
      "scenes": ["Scene1内容...", "Scene2内容...", "Scene3内容..."]
    }},
    ... (13話まで)
  ]
}}
"""
        data1 = None
        for attempt in range(3):
            try:
                res1 = self.client.models.generate_content(
                    model=MODEL_ULTRALONG,
                    contents=prompt1,
                    config=types.GenerateContentConfig(response_mime_type="application/json", safety_settings=self.safety_settings)
                )
                data1 = self._clean_json(res1.text)
                if data1: break
            except Exception as e:
                print(f"Plot Phase 1 Error: {e}")
                time.sleep(5)
        
        if not data1: return None

        # --- Phase 2: 14-25話 ---
        context_summ = "\n".join([f"Ep{p['ep_num']}: {p['resolution'][:50]}..." for p in data1['plots']])
        prompt2 = f"""
{core_instruction}

【Task: Phase 2 (Ep 14-25)】
前回の続きとして、第14話〜第25話（最終話）を作成せよ。
これまでの伏線を回収し、感動のフィナーレへ導くこと。

【これまでの流れ】
{context_summ}

出力フォーマット(JSON):
{{
  "plots": [
    {{
      "ep_num": 14,
      "title": "...",
      "setup": "...", "conflict": "...", "climax": "...", "resolution": "...",
      "tension": 85,
      "scenes": ["...", "...", "..."]
    }},
    ... (25話まで)
  ]
}}
"""
        data2 = None
        for attempt in range(3):
            try:
                res2 = self.client.models.generate_content(
                    model=MODEL_ULTRALONG,
                    contents=prompt2,
                    config=types.GenerateContentConfig(response_mime_type="application/json", safety_settings=self.safety_settings)
                )
                data2 = self._clean_json(res2.text)
                if data2: break
            except Exception as e:
                print(f"Plot Phase 2 Error: {e}")
                time.sleep(5)

        if data2 and 'plots' in data2:
            data1['plots'].extend(data2['plots'])
            
        return data1

    async def write_episodes(self, book_data, start_ep, end_ep, style_dna_str="標準", target_model=MODEL_LITE, rewrite_instruction=None, semaphore=None):
        """マイクロ執筆エンジン (Asyncio Parallel: Tension-Selector & 2-Stage Draft/Polish)"""
        
        start_idx = start_ep - 1
        all_plots = sorted(book_data['plots'], key=lambda x: x.get('ep_num', 999))
        target_plots = [p for p in all_plots if start_ep <= p.get('ep_num', -1) <= end_ep]
        
        if not target_plots: return None

        full_chapters = []
        
        # 4. ステートフル・オーケストレーター: 初期状態ロード
        current_world_state = {}
        prev_ep_row = db.fetch_one("SELECT world_state FROM chapters WHERE book_id=? AND ep_num=? ORDER BY ep_num DESC LIMIT 1", (book_data['book_id'], start_ep - 1))
        if prev_ep_row and prev_ep_row['world_state']:
            try: current_world_state = json.loads(prev_ep_row['world_state'])
            except: pass

        system_rules = self._generate_system_rules(book_data['mc_profile'], style=style_dna_str)

        for plot in target_plots:
            ep_num = plot['ep_num']
            print(f"Async Writing Ep {ep_num} (Model: {target_model})...")
            
            full_content = ""
            current_text_tail = "" # 直近100文字
            
            scenes = plot.get('scenes', [plot.get('setup',''), plot.get('conflict',''), plot.get('climax','') + plot.get('resolution','')])
            
            for part_idx, scene_plot in enumerate(scenes, 1):
                # 2. Vector DB連携: 関連プロット検索
                retrieved_context = self.get_next_context(book_data['book_id'], current_text_tail) if current_text_tail else "（初回生成のためコンテキストなし）"
                
                # 4. State管理: JSONをプロンプトに埋め込み
                state_str = json.dumps(current_world_state, ensure_ascii=False)
                
                # --- Stage 1: Draft (Always 12B) ---
                draft_prompt = f"""
{system_rules}
【Task: Draft Generation】
あなたはGemma 3 12B、高速ドラフトエンジンです。
第{ep_num}話のパート{part_idx}/3の下書きを作成してください（800文字）。
【Current Scene Plot】
{scene_plot}
【Reference Context】
{retrieved_context}
【Previous Text】
...{current_text_tail}
"""
                draft_text = ""
                async with semaphore:
                    for attempt in range(3):
                        try:
                            res = await self.client.aio.models.generate_content(
                                model=MODEL_LITE,
                                contents=draft_prompt,
                                config=types.GenerateContentConfig(safety_settings=self.safety_settings)
                            )
                            draft_text = res.text
                            # TPM Control: Sleep to adhere to 15k TPM limit
                            await asyncio.sleep(10) 
                            break
                        except Exception as e:
                            print(f"Draft Error Ep{ep_num}-{part_idx}: {e}")
                            await asyncio.sleep(5)

                # --- Stage 2: Polish (Target Model: 12B or 27B) ---
                polish_prompt = f"""
{system_rules}
【Task: Polish & Refine】
以下のドラフト原稿を、指定の文体で清書・推敲してください。
・感情描写と五感情報を増強せよ。
・矛盾がないか確認し、Stateを更新せよ。
・WorldState: {state_str}

【Draft Text】
{draft_text}

【Output Format】
本文......
```json
{{ "updated_state": {{ ... }} }}
"""
                res_text = ""
                async with semaphore:
                    for attempt in range(3):
                        try:
                            res = await self.client.aio.models.generate_content(
                                model=target_model, # Tension-selected model
                                contents=polish_prompt,
                                config=types.GenerateContentConfig(safety_settings=self.safety_settings)
                            )
                            res_text = res.text
                            # TPM Control: Sleep to adhere to 15k TPM limit
                            await asyncio.sleep(10)
                            break
                        except Exception as e:
                            print(f"Polish Error Ep{ep_num}-{part_idx}: {e}")
                            await asyncio.sleep(5)

            # State更新と本文抽出
            state_match = re.search(r'```json\s*(\{.*?\})\s*```', res_text, re.DOTALL)
            if state_match:
                try:
                    new_state_wrapper = json.loads(state_match.group(1))
                    updated_state_fragment = new_state_wrapper.get('updated_state', {})
                    
                    # バリデーション実行 (WorldState更新時)
                    self._validate_world_state(current_world_state, updated_state_fragment)
                    
                    current_world_state.update(updated_state_fragment)
                    res_text = res_text.replace(state_match.group(0), "") # JSON除去
                except: pass
            
            cleaned_part = res_text.strip()
            full_content += cleaned_part + "\n\n"
            current_text_tail = cleaned_part[-100:]
            
            # Short yield
            await asyncio.sleep(1)

        # エピソード完了処理
        full_chapters.append({
            "ep_num": ep_num,
            "title": plot['title'],
            "content": full_content,
            "summary": plot.get('resolution', '')[:100],
            "world_state": current_world_state
        })

        return {"chapters": full_chapters}

    async def _summarize_chunk(self, text_chunk, start_ep, end_ep):
        """【内部ヘルパー】エピソード群を圧縮要約する"""
        prompt = f"""
【Task: Context Compression】
以下の第{start_ep}話〜第{end_ep}話の本文を、物語の重要ポイント（伏線・感情・結末）を漏らさず、全体で1000文字程度に「濃縮要約」せよ。
あらすじではなく、マーケティング分析（キャラの魅力、構成の評価）に使える「詳細なダイジェスト」を作成すること。

【Text Chunk】
{text_chunk}
"""
        try:
            res = await self.client.aio.models.generate_content(
                model=MODEL_LITE,
                contents=prompt,
                config=types.GenerateContentConfig(safety_settings=self.safety_settings)
            )
            return res.text.strip()
        except Exception as e:
            print(f"Summary Error Ep{start_ep}-{end_ep}: {e}")
            return text_chunk[:1000] # Fallback

    async def analyze_and_create_assets(self, book_id):
        """【STEP 4 & 6統合: 改】スライディングウィンドウ分析 & マーケティング素材生成"""
        print("Starting Recursive Analysis (Sliding Window)...")
        
        # 1. 全話取得
        chapters = db.fetch_all("SELECT ep_num, title, summary, content FROM chapters WHERE book_id=? ORDER BY ep_num", (book_id,))
        book_info = db.fetch_one("SELECT title FROM books WHERE id=?", (book_id,))
        if not chapters: return [], [], None

        # 2. コンテキスト圧縮 (5話ごとにチャンク化して並列要約)
        chunk_size = 5
        summary_tasks = []
        
        for i in range(0, len(chapters), chunk_size):
            chunk = chapters[i : i + chunk_size]
            start_ep = chunk[0]['ep_num']
            end_ep = chunk[-1]['ep_num']
            
            # 本文結合
            full_text = "\n".join([f"Ep{c['ep_num']} {c['title']}:\n{c['content']}" for c in chunk])
            summary_tasks.append(self._summarize_chunk(full_text, start_ep, end_ep))
        
        # 並列実行待機
        compressed_summaries = await asyncio.gather(*summary_tasks)
        master_context = "\n\n".join(compressed_summaries)
        
        print(f"Context Compressed: {len(master_context)} chars (from approx {len(chapters)*2000} chars)")

        # 3. 圧縮コンテキストを用いた最終分析
        # Safety Settings
        safety_settings = [
            types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="BLOCK_NONE"),
            types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="BLOCK_NONE"),
        ]

        prompt = f"""
あなたはWeb小説の敏腕編集者兼マーケターです。 全25話の原稿が出揃いました。
以下は物語全体の「濃縮ダイジェスト」です。これに基づき、以下のタスクを一括実行してください。

Task 1: 各話スコアリング & 改善提案
以下の4項目（各25点満点、合計100点）で採点し、改善点を指摘せよ。
- 構成 (Structure)
- キャラ (Character)
- 引き (Hook)
- 文章量 (Volume)

Task 2: マーケティング素材生成
- cover_prompt: 表紙イラスト用プロンプト（英語）。
- illustrations: 指定話数（1, 10, 25話）の挿絵プロンプト。
- tags: 検索タグ（10個）。
- catchcopies: 読者を惹きつけるキャッチコピー3案。
- kinkyo_note: 「★評価・フォロー」を熱心にお願いする近況ノート本文（400文字程度）。

【出力フォーマット(JSON)】
{{
  "evaluations": [
    {{ "ep_num": 1, "scores": {{ "structure": 20, "character": 15, "hook": 25, "volume": 20 }}, "total_score": 80, "improvement_point": "..." }},
    ... (25話まで)
  ],
  "marketing_assets": {{
    "cover_prompt": "...",
    "illustrations": [ {{ "ep_num": 1, "prompt": "..." }}, ... ],
    "tags": ["...", ...],
    "catchcopies": ["...", ...],
    "kinkyo_note": "..."
  }}
}}

【作品タイトル】{book_info['title']}
【物語全体ダイジェスト】
{master_context}
""" 
        data = None
        for attempt in range(3):
            try:
                # 分析は高品質なProモデル推奨だが、速度優先ならLite。ここではLiteを使用。
                res = await self.client.aio.models.generate_content(
                    model=MODEL_LITE,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        safety_settings=self.safety_settings
                    )
                )
                data = self._clean_json(res.text)
                if data: break
            except Exception as e:
                if attempt == 2:
                    print(f"Analysis & Marketing Error: {e}")
                    return [], [], None
                await asyncio.sleep(2 ** attempt)

        if not data: return [], [], None

        evals = data.get('evaluations', [])
        rewrite_target_eps = [e['ep_num'] for e in evals if e.get('total_score', 0) < REWRITE_THRESHOLD]
        assets = data.get('marketing_assets', {})
        
        # DB更新
        existing = db.fetch_one("SELECT marketing_data FROM books WHERE id=?", (book_id,))
        m_data = {}
        if existing and existing['marketing_data']:
            try: m_data = json.loads(existing['marketing_data'])
            except: pass
        
        m_data["episode_evaluations"] = evals
        m_data.update(assets)
        
        db.execute("UPDATE books SET marketing_data=? WHERE id=?", (json.dumps(m_data, ensure_ascii=False), book_id))
        
        return evals, rewrite_target_eps, assets

    async def rewrite_target_episodes(self, book_data, target_ep_ids, evaluations, style_dna_str="標準"):
        """【STEP 5】指定エピソードの自動リライト（スコア不足項目への特化指示）"""
        rewritten_count = 0
        semaphore = asyncio.Semaphore(1) # TPM制限下でのリライト実行のため1に設定
        
        # 評価データのマップ化
        eval_map = {e['ep_num']: e for e in evaluations}
        
        tasks = []

        for ep_id in target_ep_ids:
            eval_data = eval_map.get(ep_id)
            if not eval_data: continue

            # スコアが低い項目を特定して指示を作成
            scores = eval_data.get('scores', {})
            low_areas = [k for k, v in scores.items() if v < 15] # 25点満点で15点未満を弱点とする
            
            specific_instruction = ""
            if "structure" in low_areas: specific_instruction += "起承転結を明確にし、伏線を強調してください。"
            if "character" in low_areas: specific_instruction += "主人公の感情描写を倍増させ、動機を深く掘り下げてください。"
            if "hook" in low_areas: specific_instruction += "結末の引きを劇的に強め、謎や危機で終わらせてください。"
            if "volume" in low_areas: specific_instruction += "描写の密度を高め、情景や五感情報を大幅に加筆してください。"
            
            base_point = eval_data.get('improvement_point', "全体的に改善")
            instruction = f"【編集者からの指摘: {base_point}】\n重点改善項目: {','.join(low_areas)}\n具体的な指示: {specific_instruction} この指摘を解消し、スコア{REWRITE_THRESHOLD}点以上になるように書き直してください。"
            
            # Async write_episodes呼び出し (Rewrite時は一律27B推奨だが、ここでは構成上Liteでドラフト、TensionロジックでPolish)
            # リライトは重要度が高いとみなし、EP_IDに関わらずMODEL_PROを使うよう強制も可能だが、
            # 汎用性を保つため既存ロジックを流用しつつtarget_modelをPROにする
            tasks.append(self.write_episodes(
                book_data, 
                ep_id, 
                ep_id, 
                style_dna_str=style_dna_str, 
                target_model=MODEL_PRO, # リライトはエースの筆で
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
        dna = json.dumps({
            "tone": data['mc_profile']['tone'], 
            "personality": data['mc_profile'].get('personality', ''),
            "style_mode": style_dna_str,
            "pov_type": "一人称"
        })
        
        ability_val = data['mc_profile'].get('ability', '')
        if isinstance(ability_val, dict):
            ability_val = json.dumps(ability_val, ensure_ascii=False)
        else:
            ability_val = str(ability_val)

        bid = db.execute(
            "INSERT INTO books (title, genre, synopsis, concept, target_eps, style_dna, status, special_ability, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (data['title'], genre, data['synopsis'], data['concept'], 25, dna, 'active', ability_val, datetime.datetime.now().isoformat())
        )
        c_dna = json.dumps(data['mc_profile'])
        monologue_val = data['mc_profile'].get('monologue_style', '')
        db.execute("INSERT INTO characters (book_id, name, role, dna_json, monologue_style) VALUES (?,?,?,?,?)", (bid, data['mc_profile']['name'], '主人公', c_dna, monologue_val))
        
        # 2. Vector DB連携: Embedding保存のためにリストを返す
        saved_plots = []
        for p in data['plots']:
            full_title = f"第{p['ep_num']}話 {p['title']}"
            main_ev = f"{p.get('setup','')}->{p.get('climax','')}"
            scenes_json = json.dumps(p.get('scenes', []), ensure_ascii=False)
            db.execute(
                """INSERT INTO plot (book_id, ep_num, title, main_event, setup, conflict, climax, resolution, tension, stress_level, status, scenes)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (bid, p['ep_num'], full_title, main_ev, 
                 p.get('setup'), p.get('conflict'), p.get('climax'), p.get('resolution'), 
                 p.get('tension', 50), p.get('stress_level', 0), 'planned', scenes_json)
            )
            saved_plots.append(p)
        return bid, saved_plots

    def save_chapters_to_db(self, book_id, chapters_list):
        count = 0
        if not chapters_list: return 0
            
        for ch in chapters_list:
            # Formatterクラスを使用
            content = TextFormatter.format(ch['content'])

            # World StateをJSON文字列化
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
def task_plot_gen(engine, genre, style, personality, tone, keywords):
    """Step 1: プロット生成 (2分割 & Embedding)"""
    print("Step 1: Hyper-Resolution Plot Generation (Gemini 2.5 Flash)...")

    blueprint = engine.generate_universe_blueprint_full(
        genre, style, personality, tone, keywords
    )

    if blueprint:
        bid, plots = engine.save_blueprint_to_db(blueprint, genre, style)
        print(f"Full Plot Generated: ID {bid}")
        # 2. Vector DB連携: 保存と同時にEmbedding
        engine._save_plot_embeddings(bid, plots)
        return bid
    else:
        print("Plot Generation Failed")
        return None

async def task_write_batch(engine, bid):
    """Step 2: バッチ執筆 (Machine-Gun Parallel Async + Dynamic Routing)"""
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

    # plotのscenesを展開
    for p in plots:
        if p.get('scenes'):
            try: p['scenes'] = json.loads(p['scenes'])
            except: pass

    full_data = {"book_id": bid, "title": book_info['title'], "mc_profile": mc_profile, "plots": [dict(p) for p in plots]}

    # 同時実行数制御用セマフォ (1: Low TPM)
    semaphore = asyncio.Semaphore(1)

    tasks = []
    print("Starting Machine-Gun Parallel Writing (25 Episodes)...")

    for p in plots:
        ep_num = p['ep_num']
        tension = p.get('tension', 50)
        
        # Tension連動型モデルセレクター
        target_model = MODEL_LITE
        if tension >= 80 or ep_num == 1 or ep_num == 25:
            target_model = MODEL_PRO # エースの筆
        else:
            target_model = MODEL_LITE # 量産の馬
        
        # Async Taskの作成 (全話一斉発射)
        tasks.append(engine.write_episodes(
            full_data, 
            ep_num, 
            ep_num, 
            style_dna_str=saved_style, 
            target_model=target_model, 
            semaphore=semaphore
        ))

    # 全タスク並列実行待機
    results = await asyncio.gather(*tasks)

    total_count = 0
    for res_data in results:
        if res_data and 'chapters' in res_data:
            c = engine.save_chapters_to_db(bid, res_data['chapters'])
            total_count += c
            
    print(f"Batch Done. Total Episodes: {total_count}")
        
    return total_count, full_data, saved_style

async def task_analyze_marketing(engine, bid):
    """Step 3 & 4: 分析・マーケティング統合"""
    print("Analyzing & Creating Marketing Assets...")
    evals, rewrite_targets, assets = await engine.analyze_and_create_assets(bid)
    return evals, rewrite_targets, assets

async def task_rewrite(engine, full_data, rewrite_targets, evals, saved_style):
    """Step 5: リライト"""
    print(f"Rewriting {len(rewrite_targets)} Episodes (Threshold < {REWRITE_THRESHOLD})...")
    c = await engine.rewrite_target_episodes(full_data, rewrite_targets, evals, style_dna_str=saved_style)
    return c

# ==========================================
# 3. Main Logic (Headless)
# ==========================================
db = DatabaseManager(DB_FILE)

def load_seed():
    """ネタ帳読み込み"""
    if not os.path.exists("story_seeds.json"):
        # Fallback
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

    # DBから必要データを再取得
    current_book = db.fetch_one("SELECT * FROM books WHERE id=?", (book_id,))
    db_chars = db.fetch_all("SELECT * FROM characters WHERE book_id=?", (book_id,))
    db_plots = db.fetch_all("SELECT * FROM plot WHERE book_id=? ORDER BY ep_num", (book_id,))
    chapters = db.fetch_all("SELECT * FROM chapters WHERE book_id=? ORDER BY ep_num", (book_id,))

    # ファイル名クリーニング
    def clean_filename_title(t):
        return re.sub(r'[\\/:*?"<>|]', '', re.sub(r'^第\d+話[\s　]*', '', t)).strip()

    # キーワード辞書準備
    keyword_dict = {}
    mc_char = next((c for c in db_chars if c['role'] == '主人公'), None)
    if mc_char:
        try:
            dna = json.loads(mc_char['dna_json'])
            keyword_dict = dna.get('keyword_dictionary', {})
        except: pass

    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        # 1. 作品登録用データ
        reg_info = f"【タイトル】\n{title}\n\n【あらすじ】\n{current_book.get('synopsis', '')}\n"
        z.writestr("00_作品登録用データ.txt", reg_info)

        # 2. 設定資料
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

        # 3. 全話プロット
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

        # 4. チャプター
        for ch in chapters:
            clean_title = clean_filename_title(ch['title'])
            fname = f"chapters/{ch['ep_num']:02d}_{clean_title}.txt"
            body = TextFormatter.format(ch['content'], k_dict=keyword_dict)
            z.writestr(fname, body)
        
        # 5. マーケティング
        if marketing_data:
            # 近況ノート
            kinkyo = marketing_data.get('kinkyo_note', '')
            if kinkyo:
                z.writestr("00_近況ノート.txt", kinkyo)
            
            # マーケティング資産
            meta = f"【タイトル】\n{title}\n\n"
            meta += f"【キャッチコピー】\n" + "\n".join(marketing_data.get('catchcopies', [])) + "\n\n"
            meta += f"【検索タグ】\n{' '.join(marketing_data.get('tags', []))}\n\n"
            meta += f"【表紙プロンプト】\n{marketing_data.get('cover_prompt', '')}\n\n"
            meta += "【挿絵プロンプト集】\n"
            for ill in marketing_data.get('illustrations', []):
                meta += f"第{ill['ep_num']}話: {ill['prompt']}\n"
            z.writestr("marketing_assets.txt", meta)

            # marketing_raw.json も保存（Streamlit版に準拠）
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

    # 5. 全自動パイプライン: 常時稼働ループ (RPD監視・エラーリトライ)
    print("Starting Factory Pipeline (Async)...")

    while True:
        try:
            # 1. ネタ選定
            seed = load_seed()
            
            # Step 1: Plot (Hyper-Resolution Gemini 2.5) - Synchronous DB/API logic retained
            bid = task_plot_gen(engine, seed['genre'], seed['style'], seed['personality'], seed['tone'], seed['keywords'])
            if not bid: 
                print("Plot Gen failed. Retrying in 10s...")
                await asyncio.sleep(10)
                continue

            # Step 2: Write (Micro-Batching Gemma 3 Async + Dynamic Routing + 2-Stage Draft/Polish)
            total_count, full_data, saved_style = await task_write_batch(engine, bid)

            # Step 3 & 4: Analyze & Market - Lite model sync call
            evals, rewrite_targets, assets = await task_analyze_marketing(engine, bid)
            print(f"Rewriting Targets (Below Threshold): {rewrite_targets}")

            # Step 5: Rewrite - Async
            if rewrite_targets:
                await task_rewrite(engine, full_data, rewrite_targets, evals, saved_style)

            # Step 6: Package & Send
            book_info = db.fetch_one("SELECT title FROM books WHERE id=?", (bid,))
            title = book_info['title']
            
            zip_bytes = create_zip_package(bid, title, assets)
            send_email(zip_bytes, title)
            print(f"Mission Complete: {title}. Sleeping for next run...")
            
            # 1日の制限を考慮して長時間待機 (シミュレーション)
            await asyncio.sleep(60) 

        except Exception as e:
            print(f"Pipeline Critical Error: {e}")
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())