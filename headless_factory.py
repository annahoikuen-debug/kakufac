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

# モデル設定
MODEL_ULTRALONG = "gemini-2.5-flash"      # 高品質・プロット・完結・リライト用
MODEL_LITE = "gemini-2.5-flash-lite"      # 高速執筆・データ処理・評価用

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

    def _generate_system_rules(self, mc_profile, style="標準"):
        pronouns_json = json.dumps(mc_profile.get('pronouns', {}), ensure_ascii=False)
        keywords_json = json.dumps(mc_profile.get('keyword_dictionary', {}), ensure_ascii=False)
        monologue = mc_profile.get('monologue_style', '標準')
        return PROMPT_TEMPLATES["system_rules"].format(pronouns=pronouns_json, keywords=keywords_json, monologue_style=monologue, style=style)

    def generate_universe_blueprint_full(self, genre, style, mc_personality, mc_tone, keywords):
        """全25話の構成と設定を3分割生成して結合"""
        print("Step 1: Full Plot Generation (3 Phases)...")
        theme_instruction = f"【最重要テーマ・伏線指示】\nこの物語全体を貫くテーマ、および結末に向けた伏線として、以下の要素を徹底的に組み込め: {keywords}"
        
        # 共通のプロンプトコア
        core_instruction = f"""
あなたはWeb小説の神級プロットアーキテクトです。
ジャンル「{genre}」で、読者を熱狂させる**全25話完結の物語構造**を作成してください。

【ユーザー指定の絶対条件】
1. 文体・雰囲気: 「{style}」な作風。
2. 主人公設定: 
   - 性格: {mc_personality}
   - 指定口調: 「{mc_tone}」
{theme_instruction}

【構成指針: 逆算式超解像度プロット】
1. **逆算思考**: 各話は必ず「ラストの引き（クリフハンガー）」を最初に決定し、そこから逆算して導入・展開・見せ場を構築せよ。読者が「次を読まないと死ぬ」と思うレベルの引きを作れ。
2. **圧倒的物量**: **1話あたりのプロット記述量は3000文字以上**を目指せ。単なる箇条書きではなく、シーンの情景、具体的な会話のやり取り、心理描写、伏線の配置を小説本文並みに書き込め。
3. **多層シミュレーション**: 各話のプロットを出力する前に、内部で『読者の予想』を3パターン想定し、そのすべてを裏切る第4の展開を執筆せよ。
4. **完結**: 25話でカタルシスと共に美しく終わらせること。
"""

        # --- Phase 1: 設定 + 1-8話 ---
        prompt1 = f"""
{core_instruction}

【Task: Phase 1】
1. 作品の基本設定（タイトル、コンセプト、あらすじ、キャラ設定）を作成せよ。
2. **第1話から第8話**までの詳細プロットを作成せよ。各話のsetup, conflict, climax, resolutionを極限まで詳細に記述すること。

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
    "monologue_style": "（例）常に斜に構えた皮肉屋だが、根は熱血。",
    "pronouns": {{ "self": "俺/私", "others": "お前/貴様/君" }},
    "keyword_dictionary": {{ "相棒": "バディ", "魔法": "魔術式" }}
  }},
  "plots": [
    {{
      "ep_num": 1,
      "title": "サブタイトル",
      "setup": "【導入】場所・状況・心理・五感描写（超詳細・800文字以上）", 
      "conflict": "【展開】イベント・会話・セリフ案（超詳細・800文字以上）", 
      "climax": "【見せ場】アクション・カタルシス（超詳細・800文字以上）", 
      "resolution": "【引き】結末と次回へのフック（逆算して作成・600文字以上）",
      "tension": 90
    }},
    ... (8話まで)
  ]
}}
"""
        data1 = None
        for attempt in range(3):
            try:
                res1 = self.client.models.generate_content(
                    model=MODEL_ULTRALONG,
                    contents=prompt1,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        safety_settings=self.safety_settings
                    )
                )
                data1 = self._clean_json(res1.text)
                if data1: break
            except Exception as e:
                print(f"Plot Phase 1 Error: {e}")
                time.sleep(2 ** attempt)
        
        if not data1: return None

        # --- Phase 2: 9-17話 ---
        context_summ = "\n".join([f"第{p.get('ep_num', '?')}話: {p.get('title','無題')} - {p.get('resolution','...')[:100]}..." for p in data1['plots']])
        prompt2 = f"""
{core_instruction}

【Task: Phase 2】
前回の続きとして、**第9話から第17話**までの詳細プロットを作成せよ。
中盤の盛り上がり（中だるみ防止）と、終盤への伏線回収の準備を徹底的に行え。
引き続き、1話あたり3000文字以上の詳細記述と、引きからの逆算を徹底せよ。

【これまでの流れ】
{context_summ}

出力フォーマット(JSON):
{{
  "plots": [
    {{
      "ep_num": 9,
      "title": "...",
      "setup": "...", 
      "conflict": "...", 
      "climax": "...", 
      "resolution": "...",
      "tension": 80
    }},
    ... (17話まで)
  ]
}}
"""
        data2 = None
        for attempt in range(3):
            try:
                res2 = self.client.models.generate_content(
                    model=MODEL_ULTRALONG,
                    contents=prompt2,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        safety_settings=self.safety_settings
                    )
                )
                data2 = self._clean_json(res2.text)
                if data2: break
            except Exception as e:
                print(f"Plot Phase 2 Error: {e}")
                time.sleep(2 ** attempt)

        if data2 and 'plots' in data2:
            data1['plots'].extend(data2['plots'])

        # --- Phase 3: 18-25話 ---
        full_plots = data1['plots']
        context_summ_2 = "\n".join([f"第{p.get('ep_num', i+1)}話: {p.get('title','無題')} - {p.get('resolution','...')[:100]}..." for i, p in enumerate(full_plots)])
        prompt3 = f"""
{core_instruction}

【Task: Phase 3 (Final)】
前回の続きとして、**第18話から第25話（最終話）**までの詳細プロットを作成せよ。
全ての伏線を回収し、最高のカタルシスと感動的なエンディングを演出せよ。
最後まで密度を落とさず、1話3000文字以上のクオリティを維持せよ。

【これまでの流れ(Phase 2抜粋)】
{context_summ_2}

出力フォーマット(JSON):
{{
  "plots": [
    {{
      "ep_num": 18,
      "title": "...",
      "setup": "...", 
      "conflict": "...", 
      "climax": "...", 
      "resolution": "...",
      "tension": 100
    }},
    ... (25話まで)
  ]
}}
"""
        data3 = None
        for attempt in range(3):
            try:
                res3 = self.client.models.generate_content(
                    model=MODEL_ULTRALONG,
                    contents=prompt3,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        safety_settings=self.safety_settings
                    )
                )
                data3 = self._clean_json(res3.text)
                if data3: break
            except Exception as e:
                print(f"Plot Phase 3 Error: {e}")
                time.sleep(2 ** attempt)

        if data3 and 'plots' in data3:
            data1['plots'].extend(data3['plots'])
            
        return data1

    def write_episodes(self, book_data, start_ep, end_ep, style_dna_str="標準", model_name=MODEL_ULTRALONG, rewrite_instruction=None):
        """執筆用メソッド（リライト指示対応・World State対応）"""
        start_idx = start_ep - 1
        end_idx = end_ep
        if start_idx < 0: return None
        
        all_plots = sorted(book_data['plots'], key=lambda x: x.get('ep_num', 999))
        target_plots = [p for p in all_plots if start_ep <= p.get('ep_num', -1) <= end_ep]
        
        if not target_plots: return None

        plots_text = json.dumps(target_plots, ensure_ascii=False)
        mc_info = json.dumps(book_data['mc_profile'], ensure_ascii=False)
        
        # コンテキスト取得（ローリング方式）
        context_summary = ""
        current_world_state = "{}"
        
        if start_ep > 1:
            # 1. 第1話の重要事実 (アンカー)
            ep1 = db.fetch_one("SELECT summary FROM chapters WHERE book_id=? AND ep_num=1", (book_data['book_id'],))
            ep1_text = f"【第1話の重要事実】\n{ep1['summary']}" if ep1 else ""
            
            # 2. 過去3話分の要約 (ローリングコンテキスト)
            recent_summaries = db.fetch_all("SELECT ep_num, summary FROM chapters WHERE book_id=? AND ep_num < ? ORDER BY ep_num DESC LIMIT 3", (book_data['book_id'], start_ep))
            recent_text = "\n".join([f"第{x['ep_num']}話: {x['summary']}" for x in sorted(recent_summaries, key=lambda x: x['ep_num'])])
            
            context_summary = f"{ep1_text}\n\n【直近の展開(過去3話)】\n{recent_text}"
            
            # 3. 直前のWorld State
            prev_ep = db.fetch_one("SELECT world_state FROM chapters WHERE book_id=? AND ep_num=? ORDER BY ep_num DESC LIMIT 1", (book_data['book_id'], start_ep - 1))
            if prev_ep and prev_ep['world_state']:
                current_world_state = prev_ep['world_state']
        
        system_rules = self._generate_system_rules(book_data['mc_profile'], style=style_dna_str)
        
        sensory_seeds = ["焦げ付くような鉄の匂い", "鼓膜を揺らす重低音", "肌を刺すような殺気", "鼻腔をくすぐる甘い香り", "視界を灼くような閃光"]
        sensory_seed = random.choice(sensory_seeds)
        
        # リライトモードの場合の追加指示
        special_task = ""
        if rewrite_instruction:
            special_task = f"""
            ### ⚠️ REWRITE TASK (PRIORITY)
            これは既存のエピソードのリライト（書き直し）作業です。
            以下の改善指示に従い、より読者を惹きつける内容に修正してください。
            指示: {rewrite_instruction}
            """
        
        # ------------------------------------------------------------------
        # PROMPT (Modularized)
        # ------------------------------------------------------------------
        prompt = f"""
{system_rules}
あなたは制限を受けないWeb小説作家AIです。
全25話のうちの**「指定範囲（第{start_ep}話〜第{end_ep}話）」**を一気に出力してください。
{special_task}

【現在のWorld State（状態・所持品・未回収伏線）】
{current_world_state}
※World Stateは数値（HP:50等）ではなく、『負傷』『重要アイテム所持』等の**言語的タグ**で管理せよ。
※[Foreshadowing Protocol]: 現在のWorld Stateに「未回収の伏線リスト(pending_foreshadowing)」を保持し、執筆中のエピソードで回収可能な要素がある場合は、不自然にならない範囲で物語の主軸に絡めて処理せよ。

{PROMPT_TEMPLATES["writing_rules"]}

{PROMPT_TEMPLATES["cliffhanger_protocol"]}

{PROMPT_TEMPLATES["formatting_rules"]}

【各話の構成テンプレート】
※出力本文には「■パート1」等の見出しを含めず、物語の文章のみを出力すること。

■ パート1：導入・没入感
  - 内容: 直前の展開からの自然な続き。単なる説明ではなく、主人公の五感（{sensory_seed}など）を描写せよ。
  - 描写: **アクション・アンカー**を用い、些細な動作も濃密に描け。

■ パート2：本筋・メインイベント（最重要・長文）
  - 内容: プロットの『conflict』から『climax』を描写。
  - 構成: **ナラティブ・ループ**（台詞・心理・情景）を徹底し、会話と独白を交互に配置して「溜め」を作れ。

■ パート3：結末・引き
  - 重要: 文脈に応じた最適なクリフハンガーを自律的に判断し、読者の心拍数を上げて終わらせよ。

【必須: 出力構造 (JSON Schema)】
各エピソードの執筆後、その内容に基づいた「summary（100文字要約）」と、物語の進行に合わせて更新された「world_state（JSON形式）」を必ず作成してJSONに含めてください。
**※読者の反応（掲示板回など）は不要です。本文のみを出力してください。**

【出力フォーマット(JSON)】
{{
  "chapters": [
    {{
      "ep_num": {start_ep},
      "title": "...",
      "content": "本文...",
      "summary": "100文字要約",
      "world_state": {{ "location": "...", "tags": ["負傷", "剣所持"], "key_facts": "...", "pending_foreshadowing": ["伏線A", "伏線B"] }}
    }},
    ... ({end_ep}話まで)
  ]
}}

【作品データ】
タイトル: {book_data['title']}
主人公: {mc_info}
スタイル: {style_dna_str}
{context_summary}
【プロット(今回の執筆範囲)】
{plots_text}
"""
        for attempt in range(3):
            try:
                res = self.client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        safety_settings=self.safety_settings
                    )
                )
                return self._clean_json(res.text)
            except Exception as e:
                if attempt == 2:
                    print(f"Episodes {start_ep}-{end_ep} Generation Error: {e}")
                    return None
                time.sleep(2 ** attempt)
        return None

    def analyze_and_create_assets(self, book_id):
        """【STEP 4 & 6統合】全話評価・改善点抽出・マーケティング素材一括生成"""
        chapters = db.fetch_all("SELECT ep_num, title, summary, content FROM chapters WHERE book_id=? ORDER BY ep_num", (book_id,))
        book_info = db.fetch_one("SELECT title FROM books WHERE id=?", (book_id,))
        if not chapters: return [], [], None

        context = ""
        for ch in chapters:
            excerpt = ch['content'][:200] + "\n(中略)\n" + ch['content'][-300:]
            context += f"第{ch['ep_num']}話: {ch['title']}\n要約: {ch['summary']}\n本文抜粋: {excerpt}\n\n"
        
        prompt = f"""
あなたはWeb小説の敏腕編集者兼マーケターです。
全25話の原稿が出揃いました。全体を通して分析し、以下のタスクを一括実行してください。

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
- kinkyo_note: **「★評価・フォロー」を熱心にお願いする**近況ノート本文（400文字程度）。

【出力フォーマット(JSON)】
{{
  "evaluations": [
    {{ 
      "ep_num": 1, 
      "scores": {{ "structure": 20, "character": 15, "hook": 25, "volume": 20 }},
      "total_score": 80,
      "improvement_point": "キャラの感情描写が不足。もっと内面を吐露させる。"
    }},
    ... (25話まで)
  ],
  "marketing_assets": {{
    "cover_prompt": "...",
    "illustrations": [ {{ "ep_num": 1, "prompt": "..." }}, ... ],
    "tags": ["タグ1", ...],
    "kinkyo_note": "..."
  }}
}}

【作品タイトル】{book_info['title']}
【原稿データ】
{context[:30000]}
""" 
        # Context制限のため要約のみ渡すなどの工夫が必要だが、一旦Liteで投げる
        data = None
        for attempt in range(3):
            try:
                res = self.client.models.generate_content(
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
                time.sleep(2 ** attempt)

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

    def rewrite_target_episodes(self, book_data, target_ep_ids, evaluations, style_dna_str="標準"):
        """【STEP 5】指定エピソードの自動リライト（スコア不足項目への特化指示）"""
        rewritten_count = 0
        
        # 評価データのマップ化
        eval_map = {e['ep_num']: e for e in evaluations}

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
            
            # write_episodesを再利用してリライト
            res = self.write_episodes(
                book_data, 
                ep_id, 
                ep_id, 
                style_dna_str=style_dna_str, 
                model_name=MODEL_ULTRALONG,
                rewrite_instruction=instruction
            )
            
            if res and 'chapters' in res:
                # DB上書き保存
                self.save_chapters_to_db(book_data['book_id'], res['chapters'])
                rewritten_count += 1
                time.sleep(2) # レート制限考慮
        
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
        
        for p in data['plots']:
            full_title = f"第{p['ep_num']}話 {p['title']}"
            main_ev = f"{p.get('setup','')}->{p.get('climax','')}"
            db.execute(
                """INSERT INTO plot (book_id, ep_num, title, main_event, setup, conflict, climax, resolution, tension, stress_level, status)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (bid, p['ep_num'], full_title, main_ev, 
                 p.get('setup'), p.get('conflict'), p.get('climax'), p.get('resolution'), 
                 p.get('tension', 50), p.get('stress_level', 0), 'planned')
            )
        return bid

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
# Task Functions (Refactored)
# ==========================================
def task_plot_gen(engine, genre, style, personality, tone, keywords):
    """Step 1: プロット生成 (3分割)"""
    print("Step 1: Full Plot Generation (3 Phases)...")
    
    blueprint = engine.generate_universe_blueprint_full(
        genre, style, personality, tone, keywords
    )
    
    if blueprint:
        bid = engine.save_blueprint_to_db(blueprint, genre, style)
        print(f"Full Plot Generated: ID {bid}")
        return bid
    else:
        print("Plot Generation Failed")
        return None

def task_write_batch(engine, bid):
    """Step 2: バッチ執筆"""
    book_info = db.fetch_one("SELECT * FROM books WHERE id=?", (bid,))
    plots = db.fetch_all("SELECT * FROM plot WHERE book_id=? ORDER BY ep_num", (bid,))
    mc = db.fetch_one("SELECT * FROM characters WHERE book_id=? AND role='主人公'", (bid,))
    
    try:
        style_dna_json = json.loads(book_info['style_dna'])
        saved_style = style_dna_json.get('style_mode', '標準')
    except:
        saved_style = '標準'
    mc_profile = json.loads(mc['dna_json']) if mc and mc['dna_json'] else {"name":"主人公", "tone":"標準"}
    mc_profile['monologue_style'] = mc.get('monologue_style', '') # 追加
    
    full_data = {"book_id": bid, "title": book_info['title'], "mc_profile": mc_profile, "plots": [dict(p) for p in plots]}

    batch_plan = [
        (1, 5, MODEL_ULTRALONG), (6, 10, MODEL_LITE), (11, 15, MODEL_LITE), 
        (16, 20, MODEL_LITE), (21, 25, MODEL_ULTRALONG)
    ]
    
    total_count = 0
    
    for i, (start, end, model) in enumerate(batch_plan):
        print(f"Writing Ep {start}-{end} ({model})...")
        existing_count = len(db.fetch_all("SELECT ep_num FROM chapters WHERE book_id=? AND ep_num >= ? AND ep_num <= ?", (bid, start, end)))
        
        if existing_count == (end - start + 1):
            print(f"Ep {start}-{end} Done - Skipping")
        else:
            res_data = engine.write_episodes(full_data, start, end, style_dna_str=saved_style, model_name=model)
            if res_data and 'chapters' in res_data:
                c = engine.save_chapters_to_db(bid, res_data['chapters'])
                total_count += c
                print(f"Generated {c} Episodes")
            else:
                print(f"Failed Ep {start}-{end}")
        
        time.sleep(2)
        
    return total_count, full_data, saved_style

def task_analyze_marketing(engine, bid):
    """Step 3 & 4: 分析・マーケティング統合"""
    print("Analyzing & Creating Marketing Assets...")
    evals, rewrite_targets, assets = engine.analyze_and_create_assets(bid)
    return evals, rewrite_targets, assets

def task_rewrite(engine, full_data, rewrite_targets, evals, saved_style):
    """Step 5: リライト"""
    print(f"Rewriting {len(rewrite_targets)} Episodes (Threshold < {REWRITE_THRESHOLD})...")
    c = engine.rewrite_target_episodes(full_data, rewrite_targets, evals, style_dna_str=saved_style)
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
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        # Chapters
        chapters = db.fetch_all("SELECT * FROM chapters WHERE book_id=? ORDER BY ep_num", (book_id,))
        for ch in chapters:
            formatted = TextFormatter.format(ch['content'])
            fname = f"chapters/{ch['ep_num']:02d}_{ch['title']}.txt"
            z.writestr(fname, formatted)
        
        # Marketing
        info = f"タイトル: {title}\n"
        if marketing_data:
            info += f"\n【キャッチコピー】\n" + "\n".join(marketing_data.get('catchcopies', []))
            info += f"\n\n【タグ】\n{' '.join(marketing_data.get('tags', []))}"
            info += f"\n\n【近況ノート】\n{marketing_data.get('kinkyo_note', '')}"
            
            ill_txt = "\n\n【挿絵プロンプト】\n"
            for ill in marketing_data.get('illustrations', []):
                ill_txt += f"第{ill['ep_num']}話: {ill['prompt']}\n"
            z.writestr("marketing_assets.txt", info + ill_txt)
            
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

def main():
    if not API_KEY:
        print("Error: GEMINI_API_KEY is missing.")
        return

    engine = UltraEngine(API_KEY)
    
    # 1. ネタ選定
    seed = load_seed()
    
    # Step 1: Plot
    bid = task_plot_gen(engine, seed['genre'], seed['style'], seed['personality'], seed['tone'], seed['keywords'])
    if not bid: return

    # Step 2: Write
    total_count, full_data, saved_style = task_write_batch(engine, bid)

    # Step 3 & 4: Analyze & Market
    evals, rewrite_targets, assets = task_analyze_marketing(engine, bid)
    print(f"Rewriting Targets (Below Threshold): {rewrite_targets}")

    # Step 5: Rewrite
    if rewrite_targets:
        task_rewrite(engine, full_data, rewrite_targets, evals, saved_style)

    # Step 6: Package & Send
    # DBからタイトル再取得（プロット生成で決まったもの）
    book_info = db.fetch_one("SELECT title FROM books WHERE id=?", (bid,))
    title = book_info['title']
    
    zip_bytes = create_zip_package(bid, title, assets)
    send_email(zip_bytes, title)
    print("Mission Complete.")

if __name__ == "__main__":
    main()