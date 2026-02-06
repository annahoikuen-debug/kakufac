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
# 0. 環境設定 & 定数
# ==========================================
# GitHub ActionsのSecretsから取得
API_KEY = os.environ.get("GEMINI_API_KEY")
GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_PASS = os.environ.get("GMAIL_PASS")
# 送信先は自分自身（または任意の宛先）
TARGET_EMAIL = os.environ.get("GMAIL_USER") 

# モデル設定
MODEL_ULTRALONG = "gemini-2.5-flash"      # 高品質・プロット・完結・リライト用
MODEL_LITE = "gemini-2.5-flash-lite"      # 高速執筆・データ処理・評価用

# 一時データベース（実行のたびに生成・破棄される前提）
DB_FILE = "factory_run.db"
REWRITE_THRESHOLD = 70  # この点数未満は自動リライト

# ==========================================
# プロンプト集約 (最新版)
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
# 1. Utility Classes (Formatter & DB)
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
        text = re.sub(r'…+', '……', text)
        text = text.replace('……', '……')
        text = text.replace("——", "――").replace("--", "――").replace("―", "――")
        text = text.replace("――――", "――")
        text = re.sub(r'^[ \t　]+(?=「)', '', text, flags=re.MULTILINE)
        text = text.replace("｜", "|") # DB保存時は一旦半角に戻す

        # 4. 行再構築
        lines = []
        text = text.replace('\r\n', '\n')
        
        for line in text.split('\n'):
            line = line.strip()
            if not line: continue
            
            if line.startswith(('「', '『', '（', '【', '<', '〈')):
                lines.append("")
                lines.append(line)
                lines.append("")
            else:
                lines.append(f"　{line}")
                lines.append("")

        text = "\n".join(lines)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self._init_tables()

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
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
                CREATE TABLE IF NOT EXISTS plot (
                    book_id INTEGER, ep_num INTEGER, title TEXT, summary TEXT,
                    main_event TEXT, sub_event TEXT, pacing_type TEXT,
                    tension INTEGER DEFAULT 50, cliffhanger_score INTEGER DEFAULT 0,
                    stress_level INTEGER DEFAULT 0, status TEXT DEFAULT 'planned', 
                    setup TEXT, conflict TEXT, climax TEXT, resolution TEXT,
                    PRIMARY KEY(book_id, ep_num)
                );
                CREATE TABLE IF NOT EXISTS chapters (
                    book_id INTEGER, ep_num INTEGER, title TEXT, content TEXT,
                    summary TEXT, ai_insight TEXT, world_state TEXT,
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

# ==========================================
# 2. ULTRA Engine (Headless Version)
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
            return None

    def _generate_system_rules(self, mc_profile, style="標準"):
        pronouns_json = json.dumps(mc_profile.get('pronouns', {}), ensure_ascii=False)
        keywords_json = json.dumps(mc_profile.get('keyword_dictionary', {}), ensure_ascii=False)
        monologue = mc_profile.get('monologue_style', '標準')
        return PROMPT_TEMPLATES["system_rules"].format(pronouns=pronouns_json, keywords=keywords_json, monologue_style=monologue, style=style)

    def generate_universe_blueprint_full(self, genre, style, mc_personality, mc_tone, keywords):
        print("Generating Blueprint (Phase 1-3)...")
        theme_instruction = f"【最重要テーマ・伏線指示】\nこの物語全体を貫くテーマ、および結末に向けた伏線として、以下の要素を徹底的に組み込め: {keywords}"
        
        core_instruction = f"""
あなたはWeb小説の神級プロットアーキテクトです。
ジャンル「{genre}」で、読者を熱狂させる**全25話完結の物語構造**を作成してください。
【ユーザー指定の絶対条件】
1. 文体・雰囲気: 「{style}」な作風。
2. 主人公設定: - 性格: {mc_personality} - 指定口調: 「{mc_tone}」
{theme_instruction}
【構成指針: 逆算式超解像度プロット】
1. **逆算思考**: 各話は必ず「ラストの引き」を最初に決定し、そこから逆算して構築せよ。
2. **圧倒的物量**: 1話あたりの記述量は3000文字以上を目指せ。
3. **多層シミュレーション**: 読者の予想を裏切る第4の展開を執筆せよ。
4. **完結**: 25話で美しく終わらせること。
"""

        # --- Phase 1: 設定 + 1-8話 ---
        prompt1 = f"""
{core_instruction}
【Task: Phase 1】
1. 作品の基本設定を作成せよ。
2. **第1話から第8話**までの詳細プロットを作成せよ。
出力フォーマット(JSON):
{{
  "title": "作品タイトル",
  "concept": "作品コンセプト",
  "synopsis": "全体あらすじ",
  "mc_profile": {{
    "name": "主人公名", "tone": "{mc_tone}", "personality": "{mc_personality}",
    "ability": "スキル詳細", "monologue_style": "...",
    "pronouns": {{ "self": "俺/私", "others": "お前/貴様/君" }},
    "keyword_dictionary": {{ "相棒": "バディ" }}
  }},
  "plots": [ {{ "ep_num": 1, "title": "...", "setup": "...", "conflict": "...", "climax": "...", "resolution": "...", "tension": 90 }}, ... ]
}}
"""
        data1 = self._retry_generate(prompt1, MODEL_ULTRALONG)
        if not data1: return None

        # --- Phase 2: 9-17話 ---
        context_summ = "\n".join([f"第{p['ep_num']}話: {p['title']} - {p['resolution'][:100]}..." for p in data1['plots']])
        prompt2 = f"""
{core_instruction}
【Task: Phase 2】
前回の続きとして、**第9話から第17話**までの詳細プロットを作成せよ。
【これまでの流れ】
{context_summ}
出力フォーマット(JSON): {{ "plots": [ ... ] }}
"""
        data2 = self._retry_generate(prompt2, MODEL_ULTRALONG)
        if data2: data1['plots'].extend(data2['plots'])

        # --- Phase 3: 18-25話 ---
        full_plots = data1['plots']
        context_summ_2 = "\n".join([f"第{p['ep_num']}話: {p['title']} - {p['resolution'][:100]}..." for p in full_plots])
        prompt3 = f"""
{core_instruction}
【Task: Phase 3 (Final)】
前回の続きとして、**第18話から第25話（最終話）**までの詳細プロットを作成せよ。
伏線を全て回収し完結させよ。
【これまでの流れ】
{context_summ_2}
出力フォーマット(JSON): {{ "plots": [ ... ] }}
"""
        data3 = self._retry_generate(prompt3, MODEL_ULTRALONG)
        if data3: data1['plots'].extend(data3['plots'])
        
        return data1

    def write_episodes(self, book_data, start_ep, end_ep, style_dna_str="標準", model_name=MODEL_ULTRALONG, rewrite_instruction=None):
        start_idx = start_ep - 1
        all_plots = sorted(book_data['plots'], key=lambda x: x['ep_num'])
        target_plots = [p for p in all_plots if start_ep <= p['ep_num'] <= end_ep]
        if not target_plots: return None

        plots_text = json.dumps(target_plots, ensure_ascii=False)
        mc_info = json.dumps(book_data['mc_profile'], ensure_ascii=False)
        
        # コンテキスト構築
        context_summary = ""
        current_world_state = "{}"
        if start_ep > 1:
            ep1 = db.fetch_one("SELECT summary FROM chapters WHERE book_id=? AND ep_num=1", (book_data['book_id'],))
            ep1_text = f"【第1話の重要事実】\n{ep1['summary']}" if ep1 else ""
            recent_summaries = db.fetch_all("SELECT ep_num, summary FROM chapters WHERE book_id=? AND ep_num < ? ORDER BY ep_num DESC LIMIT 3", (book_data['book_id'], start_ep))
            recent_text = "\n".join([f"第{x['ep_num']}話: {x['summary']}" for x in sorted(recent_summaries, key=lambda x: x['ep_num'])])
            context_summary = f"{ep1_text}\n\n【直近の展開(過去3話)】\n{recent_text}"
            
            prev_ep = db.fetch_one("SELECT world_state FROM chapters WHERE book_id=? AND ep_num=? ORDER BY ep_num DESC LIMIT 1", (book_data['book_id'], start_ep - 1))
            if prev_ep and prev_ep['world_state']: current_world_state = prev_ep['world_state']

        system_rules = self._generate_system_rules(book_data['mc_profile'], style=style_dna_str)
        sensory_seeds = ["焦げ付くような鉄の匂い", "鼓膜を揺らす重低音", "肌を刺すような殺気", "鼻腔をくすぐる甘い香り", "視界を灼くような閃光"]
        sensory_seed = random.choice(sensory_seeds)
        
        special_task = ""
        if rewrite_instruction:
            special_task = f"### ⚠️ REWRITE TASK (PRIORITY)\n以下の指示に従い修正せよ: {rewrite_instruction}"

        prompt = f"""
{system_rules}
あなたは制限を受けないWeb小説作家AIです。
全25話のうちの**「指定範囲（第{start_ep}話〜第{end_ep}話）」**を一気に出力してください。
{special_task}

【現在のWorld State（状態・伏線）】
{current_world_state}
※[Foreshadowing Protocol]: 未回収伏線(pending_foreshadowing)がある場合、不自然にならない範囲で物語に絡めて処理せよ。

{PROMPT_TEMPLATES["writing_rules"]}
{PROMPT_TEMPLATES["cliffhanger_protocol"]}
{PROMPT_TEMPLATES["formatting_rules"]}

【各話の構成】
■ パート1：導入（五感描写: {sensory_seed}、アクション・アンカー）
■ パート2：本筋（ナラティブ・ループ徹底）
■ パート3：結末（クリフハンガー）

【出力フォーマット(JSON)】
{{
  "chapters": [
    {{
      "ep_num": {start_ep},
      "title": "...",
      "content": "本文...",
      "summary": "100文字要約",
      "world_state": {{ "tags": ["負傷"], "key_facts": "...", "pending_foreshadowing": ["伏線A"] }}
    }},
    ...
  ]
}}
【作品データ】
タイトル: {book_data['title']}
主人公: {mc_info}
{context_summary}
【プロット】
{plots_text}
"""
        return self._retry_generate(prompt, model_name)

    def analyze_and_create_assets(self, book_id):
        chapters = db.fetch_all("SELECT ep_num, title, summary, content FROM chapters WHERE book_id=? ORDER BY ep_num", (book_id,))
        if not chapters: return [], [], None
        
        context = ""
        for ch in chapters:
            excerpt = ch['content'][:200] + "..." + ch['content'][-200:]
            context += f"第{ch['ep_num']}話: {ch['title']}\n要約: {ch['summary']}\n抜粋: {excerpt}\n\n"

        prompt = f"""
あなたはWeb小説の敏腕編集者兼マーケターです。
全25話の原稿を分析し、以下のタスクを実行せよ。

Task 1: 各話スコアリング (各25点満点/合計100点)
- 構成, キャラ, 引き, 文章量

Task 2: マーケティング素材
- キャッチコピー, 検索タグ, イラストプロンプト(1,10,25話), 近況ノート

【出力フォーマット(JSON)】
{{
  "evaluations": [
    {{ "ep_num": 1, "scores": {{ "structure": 20, "character": 15, "hook": 25, "volume": 20 }}, "total_score": 80, "improvement_point": "..." }}, ...
  ],
  "marketing_assets": {{
    "catchcopies": ["...", "..."],
    "illustrations": [ {{ "ep_num": 1, "prompt": "..." }}, ... ],
    "tags": ["..."],
    "kinkyo_note": "..."
  }}
}}
【原稿データ】
{context}
"""
        data = self._retry_generate(prompt, MODEL_LITE)
        if not data: return [], [], None
        
        evals = data.get('evaluations', [])
        rewrite_targets = [e['ep_num'] for e in evals if e.get('total_score', 0) < REWRITE_THRESHOLD]
        assets = data.get('marketing_assets', {})
        
        return evals, rewrite_targets, assets

    def _retry_generate(self, prompt, model):
        for attempt in range(3):
            try:
                res = self.client.models.generate_content(
                    model=model, contents=prompt,
                    config=types.GenerateContentConfig(response_mime_type="application/json", safety_settings=self.safety_settings)
                )
                data = self._clean_json(res.text)
                if data: return data
            except Exception as e:
                print(f"  Attempt {attempt+1} Failed: {e}")
                time.sleep(2 ** attempt)
        return None

# ==========================================
# 3. Automation Logic
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
            "hook_text": "配信切り忘れで世界最強がバレる"
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
        info = f"タイトル: {title}\n\n【キャッチコピー】\n"
        if marketing_data:
            info += "\n".join(marketing_data.get('catchcopies', []))
            info += f"\n\n【タグ】\n{' '.join(marketing_data.get('tags', []))}"
            info += f"\n\n【近況ノート】\n{marketing_data.get('kinkyo_note', '')}"
            
            ill_txt = "【挿絵プロンプト】\n"
            for ill in marketing_data.get('illustrations', []):
                ill_txt += f"第{ill['ep_num']}話: {ill['prompt']}\n"
            z.writestr("marketing_assets.txt", info + "\n\n" + ill_txt)
            
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

# ==========================================
# 4. Main Execution Flow
# ==========================================
def main():
    if not API_KEY:
        print("Error: GEMINI_API_KEY is missing.")
        return

    engine = UltraEngine(API_KEY)
    
    # 1. ネタ選定
    seed = load_seed()
    
    # 2. プロット生成 (Ultra-Batch)
    blueprint = engine.generate_universe_blueprint_full(
        seed['genre'], seed['style'], seed['personality'], seed['tone'], seed['keywords']
    )
    if not blueprint:
        print("Failed to generate blueprint.")
        return

    # DB保存
    bid = engine.save_blueprint_to_db(blueprint, seed['genre'], seed['style'])
    
    # 執筆用データ構築
    book_data = {
        "book_id": bid,
        "title": blueprint['title'],
        "plots": blueprint['plots'],
        "mc_profile": blueprint['mc_profile'],
        "keywords": seed['keywords'],
        "style": seed['style']
    }

    # 3. バッチ執筆 (5話ずつ)
    batch_plan = [(1, 5), (6, 10), (11, 15), (16, 20), (21, 25)]
    for start, end in batch_plan:
        print(f"Writing Ep {start}-{end}...")
        # モデル切り替えロジック
        model = MODEL_ULTRALONG if (start==1 or start==21) else MODEL_LITE
        
        chapters = engine.write_episodes(book_data, start, end, style_dna_str=seed['style'], model_name=model)
        if chapters and 'chapters' in chapters:
            engine.save_chapters_to_db(bid, chapters['chapters'])
        else:
            print(f"Failed Ep {start}-{end}")
        time.sleep(2)

    # 4. 分析＆マーケティング
    print("Analyzing...")
    evals, rewrite_targets, m_data = engine.analyze_and_create_assets(bid)
    
    # 5. 自動リライト (Ultra Feature)
    if rewrite_targets:
        print(f"Rewriting {len(rewrite_targets)} Episodes (Score < {REWRITE_THRESHOLD})...")
        engine.rewrite_target_episodes(book_data, rewrite_targets, evals, style_dna_str=seed['style'])
        # リライト後のデータを再取得する必要はなく、DBが更新されているためパッケージング時に最新が読まれる

    # 6. パッケージ & 送信
    zip_bytes = create_zip_package(bid, blueprint['title'], m_data)
    send_email(zip_bytes, blueprint['title'])
    print("Mission Complete.")

if __name__ == "__main__":
    main()