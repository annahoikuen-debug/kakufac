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
from pydantic import BaseModel, Field, ConfigDict
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
MODEL_ULTRALONG = "gemini-3-flash-preview"       # Gemini 3.0 Flash (プロット用・JSON対応)
MODEL_LITE = "gemma-3-12b-it"        # Gemma 3 12B (通常執筆・JSON非対応)
MODEL_PRO = "gemma-3-27b-it"             # Gemma 3 27B (重要回執筆・JSON非対応)
MODEL_MARKETING = "gemini-2.5-flash-lite" # マーケティング分析用 (JSON対応)

DB_FILE = "factory_run.db" # 自動実行用に一時DBへ変更

# Global Config: Rate Limits
MIN_REQUEST_INTERVAL = 0.5

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

STYLE_SAMPLES = {
    "style_serious_fantasy": "　俺は杖を握りしめ、泥にまみれた自分の手を見つめた。\n　前世では、こんな風に必死になることなんてなかった。ただ部屋に引きこもり、親の脛をかじり、ネットの海で誰かを叩くだけの人生。\n　だが、今は違う。\n　ルーデウスとしての俺は、守るべきもののために魔法を放つことができる。\n「……よし、やるか」\n　俺は深く息を吐き出すと、魔力を練り上げた。下腹部に熱が集まる感覚。かつては性的な衝動にしか使わなかったこの熱を、今は生存本能として制御している。\n　泥臭くてもいい。格好悪くてもいい。俺は、この世界で本気で生きると決めたのだから。",
    "style_psychological_loop": "　――熱い。熱い熱い熱い熱い熱い。\n　脳髄が沸騰するような、魂が焦げ付くような、理不尽な暴力がナツキ・スバルを襲う。\n「あ、が……ぁ！？」\n　声にならない絶叫。視界が真っ赤に染まり、愛しい人の名前を呼ぼうとした喉は、ごぼりと溢れ出した鮮血によって塞がれた。\n　死ぬ。また死ぬのか。何も成し遂げられず、誰も救えず、ただ無様に？\n　嫌だ。それだけは、絶対に。\n（エミリア、たん――）\n　思考が断絶する。世界が暗転する。だが、その刹那、スバルの心臓を握り潰すような『影』の気配だけが、死の淵にあっても彼を離さなかった。",
    "style_military_rational": "　結論から言えば、それは極めて非効率的な資源の浪費であった。\n　帝国軍参謀本部は、前線の兵站維持にかかるコストと、敵要塞攻略による政治的プロパガンダ効果を天秤にかけ、あろうことか後者を選択したのだ。\n「狂気の沙汰だな。あるいは、存在Ｘとやらの悪意か」\n　ターニャ・デグレチャフは、眼下の泥沼と化した戦場を見下ろし、小さく舌打ちした。\n　人的資本の摩耗を度外視したドクトリンなど、シカゴ学派の信奉者としては到底容認できるものではない。だが、軍人としての義務（デュ・ヴォア）が、彼女にライン戦線への突撃を命じていた。",
    "style_magic_engineering": "　達也はCADのトリガーを引くのと同時に、脳内の魔法演算領域で起動式を展開した。\n　事象改変のプロセスは0.05秒。対象となる空間の座標情報を上書きし、運動エネルギーのベクトルを百八十度反転させる『分解』の魔法だ。\n　物理法則を無視するのではない。物理法則そのものを一時的に書き換える、現代魔法の精髄。\n「……消えろ」\n　放たれたサイオンの波は、敵が展開していた対魔法障壁の構成定義を瞬時に読み解き、その結合を霧散させた。\n　深雪が兄を見つめる視線には熱がこもっていたが、達也はあくまで事務的に、残心の動作へと移行した。",
    "style_comedy_speed": "「おい、カズマ！　ちょっとこれを見なさいよ！　私の華麗なる宴会芸スキルがレベルアップしたわ！」\n「……お前、クエストに行く前に何を習得してんだよ」\n　ギルドの酒場で、駄女神アクアが得意げに水の入ったジョッキを頭に乗せている。\n「ふっふっふ、これで信者からの賽銭も倍増間違いなしね！　さあ、崇めなさい！」\n「崇めるかボケ。大体な、今のパーティーに必要なのは回復魔法の射程延長だろ。なんで『花鳥風月』の持続時間を伸ばしてんだ」\n「ああっ！　私の芸術を否定するなんて、これだから引きこもりは！」\n　俺は頭を抱えた。このパーティー、前途多難すぎる。",
    "style_overlord": "　アインズ・ウール・ゴウンは、玉座にて静かに思考を巡らせていた。\n（……え、何それ。デミウルゴス、また何か恐ろしい計画立ててない？　世界征服とか本気で言ってるの？）\n　内なる鈴木悟の叫びをよそに、骸骨の支配者は威厳たっぷりに頷いてみせる。\n「うむ。デミウルゴスよ、予の考えをよくぞ理解した」\n「ハッ！　至高の御方におかれましては、既にその先の千年王国まで見据えておられるのですね！」\n　守護者たちが一斉に平伏する。その光景に、アインズは存在しない胃が痛むのを覚えた。\n　絶対支配者としてのロールプレイは、今日も綱渡りである。",
    "style_slime_nation": "《解。個体名リムル＝テンペストの魔素量が規定値に達しました。これより『魔王への進化（ハーベストフェスティバル）』を開始します》\n　世界の言葉が頭に響く。どうやら俺、また進化しちゃうらしい。\n「リムル様！　お体の具合が！？」\n　ベニマルたちが慌てているが、俺の意識は急速に眠りへと落ちていく。\n　まあ、ラファエル先生……じゃなくて『智慧之王（ラファエル）』さんがなんとかしてくれるだろう。\n　目が覚めたら、きっとまた強くなっているはずだ。そんな楽観的な思考と共に、俺は深い闇へと沈んでいった。\n　――あ、ついでにスキル『暴食之王（ベルゼビュート）』で周囲の残骸も片付けておいてね。",
    "style_spider_chaos": "　はいはい、鑑定鑑定。\n【ステータス閲覧権限がありません】\n　って、オイイイイイ！　なんでだよ！　私、頑張ったよね！？\n　ここ迷宮の最下層だぞ？　周り全部バケモノだぞ？\n　目の前には巨大な地龍。絶対勝てない。無理ゲー。詰んだ。\n　……いや、待てよ。私の『韋駄天』スキルと『蜘蛛の糸』を組み合わせれば、ワンチャン逃げ切れるんじゃね？\n　思考加速、オン！　並列意思、作戦会議開始！\n『逃げるが勝ち！』『いや、食べてレベルアップっしょ』『毒合成準備完了！』\n　よし、方針決定。嫌がらせして逃げる！",
    "style_vrmmo_introspection": "　視界の端で、HPバーがレッドゾーンへと突入する。\n　だが、俺の意識はかつてないほど澄み渡っていた。システムが描画するポリゴンの砕ける光。剣が風を切る鋭い音。その全てが、この仮想世界（VR）における俺の命の鼓動だ。\n「――スターバースト・ストリーム！」\n　叫びと共に、システム・アシストに身を委ねるのではなく、自らの意志で剣速を加速させる。\n　十六連撃。その全てを叩き込むコンマ数秒の間、俺は確かにここに生きていた。\n　仮想と現実の境界線など、剣を振るうこの瞬間には何の意味も持たないのだ。",
    "style_action_heroic": "　鐘の音が鳴り響く。\n　それは始まりの合図であり、英雄への階梯を登る者への祝福だ。\n「う、おおおおおおおおっ！」\n　少年は吼えた。憧れに届くために。あの人の隣に立つために。\n　全身から血を流し、意識はとっくに限界を超えている。それでも、彼の足は止まらない。\n　ドゴォォォォン！！\n　ミノタウロスの剛腕を、極小のナイフ一本で受け止める。\n　――冒険者とは。英雄とは。\n　その答えを刻み込むように、白き閃光がダンジョンの闇を切り裂いた。",
    "style_otome_misunderstand": "　大変です！　破滅フラグです！\n　私の脳内会議（議長：私）が緊急招集された。\n『どうする？　このままだとジオルド王子の好感度が下がって断罪ルートよ！』\n『とりあえず土下座？　それともお菓子で買収？』\n『よし、畑を耕そう！　土と触れ合えば心も落ち着くはず！』\n　そう決意して鍬を握りしめた私を、なぜか王子は熱っぽい瞳で見つめている。\n「……カタリナ、君は本当に予想がつかないね。そういうところが、愛おしい」\n　えっ、何が？　土汚れがですか？\n　相変わらず王子の美的センスは謎だと思いつつ、私は今日も元気に土を耕すのだった。",
    "style_dark_hero": "「――邪魔だ。消えろ」\n　ハジメは無造作に引き金を引いた。大型リボルバー『ドンナー』が火を噴き、魔物の頭部がトマトのように弾け飛ぶ。\n　慈悲はない。容赦もない。敵対する者は全て殺す。\n　それが、奈落の底で彼が得た生存哲学だった。\n「ハジメくん、素敵……っ！」\n　隣でユエが頬を染めて見上げている。\n「……ユエ、終わったら休憩だ。血の匂いがきつい」\n「ん。ハジメくんの匂い、好き」\n　最強で、最凶。世界を敵に回しても、俺たちは止まらない。",
    "style_average_gag": "「いやいや、普通の女の子ですから！　平均的ですから！」\n　マイルは必死に否定したが、放たれた魔法は山を一つ消滅させていた。\n　……あれれー？　おっかしいぞー？（某名探偵ボイス）\n　神様、平均値って言いましたよね？　古龍種と最弱スライムを足して２で割った数値とかじゃないですよね？\n「あーもう、バレなきゃいいんです！　これはえっと、秘伝の古武術です！」\n「「「絶対違う！！」」」\n　パーティーメンバー（赤き誓い）のツッコミが綺麗にハモった。\n　今日もマイルの『普通』への道は遠い。",
    "style_romcom_cynical": "　青春とは嘘であり、悪だ。そう定義した過去の自分を、俺はまだ否定しきれていない。\n　教室の空気は澱んでいる。スクールカーストという名の見えない序列が、呼吸の仕方さえ規定しているようだ。\n「……比企谷くん、また腐った魚のような目をしているわね」\n「失敬な。これは深海魚のように高圧環境に適応進化した目だ」\n　雪ノ下雪乃の毒舌は、今日も鋭利な刃物のように的確だ。\n　だが、その言葉の裏にある微かな躊躇いを、俺は見逃さない。\n　俺たちは『本物』を探している。傷つけ合うことでしか触れ合えない、不器用な共犯者として。",
    "style_trpg_hardboiled": "　小鬼を殺した。\n　鉄兜の冒険者は、血濡れた剣を無造作に振るい、脂を落とす。\n「一匹」\n　淡々とした計数。そこに感情はない。あるのは作業としての殺戮のみ。\n　女神官が震える手で奇跡を行使し、傷ついた戦士を癒やす。\n「あ、あの……ゴブリンスレイヤーさん、大丈夫ですか？」\n「問題ない。まだ巣の奥に気配がある」\n　彼は歩き出す。世界を救うつもりなどない。ただ、村を焼く小鬼を許さない。\n　これは、神々がダイスを振って遊ぶ盤上の、ごくありふれた駒の物語。",
    "style_chat_log": "【悲報】底辺探索者さん、S級ダンジョンに迷い込む\n\n1 : 名無しの探索者\n>>1 乙\nまた自殺志願者かよ\n\n2 : 名無しの探索者\n配信見たけど装備が初期装備で草\nこれ死ぬわ\n\n3 : 主人公（配信中）\n「あ、えっと、コメントありがとうございます。ここ、なんか強いモンスター多くないですか？」\n\n4 : 名無しの探索者\n>>3\n気づけｗｗｗそこはラストダンジョンだｗｗｗ\n後ろ！　ドラゴンいるって！\n\n5 : 名無しの探索者\n（ドガァァァン！　という音と共にドラゴンが消滅）\n\n6 : 名無しの探索者\nは？\n\n7 : 名無しの探索者\nえっ、ワンパン？\n合成映像乙……じゃないだと！？\n\n8 : 名無しの探索者\n【速報】新たな神、降臨",
    "style_villainess_elegant": "「あら、ご機嫌よう。泥棒猫さん」\n　わたくしは扇子を口元にあて、優雅に微笑んで差し上げました。\n　目の前には、わたくしの婚約者に擦り寄る男爵令嬢。\n　安っぽい生地のドレス。手入れのされていない髪。そして何より、その浅ましい性根。\n「ひ、酷いですわ！　私はただ、殿下と！」\n「お黙りなさい。わたくしの視界に入るだけで、空気が汚れますのよ」\n　冷徹に、しかし高貴に。悪役令嬢としての矜持を持って、わたくしはこの場を支配いたします。\n　たとえ断罪イベントが待っていようとも、わたくしは最期まで美しくありたいのですわ。",
    "style_slow_life": "　朝の日差しと共に目を覚ますと、隣にはフェンリルのポチが丸くなっていた。\n「ん……おはよう、ポチ」\n「ワン！（おはよう、ご主人！）」\n　もふもふの毛並みを撫で回してから、俺は畑へと向かう。\n　『超育成』スキルのおかげで、昨日植えたトマトがもう真っ赤な実をつけている。\n　もぎ取って一口かじれば、口いっぱいに広がる濃厚な甘みと酸味。\n「うん、美味い！　やっぱり異世界で食べる野菜は最高だな」\n　魔王討伐？　英雄？　興味ないね。\n　俺はこの辺境で、美味しいものを食べて、のんびり暮らすんだ。",
    "style_web_standard": "　キィィィィン！\n　激しい金属音が鳴り響く。\n　俺は聖剣を振るい、魔王の攻撃を受け止めた。\n「バカな！？　人間ごときが私の剣を受け止めるだと！？」\n「へっ、悪いな。俺のステータスはとっくに限界突破してるんだよ！」\n　ステータスオープン。\n【攻撃力：∞（測定不能）】\n　これを見た魔王が顔を青ざめる。\n「ありえない……貴様、何者だ！？」\n「ただの通りすがりの高校生さ！」\n　ズバァァァッ！\n　俺の一撃が、魔王を真っ二つにした。\n　やれやれ、また俺何かやっちゃいました？",
    "style_bookworm_daily": "　マインは本を探して、今日も今日とて下町を歩き回る。\n　羊皮紙は高くて買えない。木簡を作るには道具が足りない。\n　それでも、活字への渇望が止まらない。\n「あー、本が読みたい！　インクの匂いを嗅ぎたい！」\n　ルッツが呆れた顔で見ているけれど、気にしない。\n　麗乃だった頃の記憶が、私を突き動かす。\n　なければ作ればいい。紙も、インクも、本も。\n　ここからが、私の本作りへの第一歩なのだから。"
}

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
    pronouns: str = Field(..., description="JSON string mapping keys (e.g., '一人称', '二人称') to values")
    keyword_dictionary: str = Field(..., description="JSON string mapping unique terms to their reading or definition")

class NovelStructure(BaseModel):
    title: str
    concept: str
    synopsis: str
    mc_profile: MCProfile
    plots: List[PlotEpisode]

class Phase2Structure(BaseModel):
    plots: List[PlotEpisode]

class WorldState(BaseModel):
    immutable: str = Field(..., description="JSON string representing immutable settings")
    mutable: str = Field(..., description="JSON string representing mutable settings")
    revealed: List[str] = Field(default_factory=list, description="読者に開示済みの設定リスト")

class ConsistencyResult(BaseModel):
    is_consistent: bool = Field(..., description="設定矛盾がないか")
    fatal_errors: List[str] = Field(default_factory=list, description="致命的な矛盾")
    minor_errors: List[str] = Field(default_factory=list, description="軽微な矛盾")
    rewrite_needed: bool = Field(..., description="リライトが必要か")

class EvaluationItem(BaseModel):
    ep_num: int
    total_score: int
    improvement_point: str

class MarketingAssets(BaseModel):
    evaluations: List[EvaluationItem]
    marketing_assets: str = Field(..., description="JSON string containing marketing assets like catchcopies and tags")

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

【文体指定: {style_name}】
{style_instruction}

【文体サンプル (Few-Shot)】
以下の雰囲気を極限まで模倣せよ:
"{few_shot_sample}"

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
   - 尻切れトンボにならず、次の話への興味を惹く「引き（クリフハンガー）」で終わること。

3. **密度**:
   - 「〜ということがあった」のようなあらすじ要約を厳禁とする。
   - 情景描写、五感、セリフ、内面描写を交え、読者が没入できる小説形式で記述せよ。
   - 会話文だけで進行させず、必ず地の文での状況描写を挟むこと。
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
        text = re.sub(r'…+', '……', text)
        text = text.replace('……', '……') 
        text = text.replace("——", "――").replace("--", "――").replace("―", "――")
        text = text.replace("――――", "――")
        text = re.sub(r'^[ \t　]+(?=「)', '', text, flags=re.MULTILINE)
        text = text.replace("｜", "|") 

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
            return WorldState(immutable="{}", mutable="{}", revealed=[])
        try:
            return WorldState(
                immutable=row['immutable'] if row['immutable'] else "{}",
                mutable=row['mutable'] if row['mutable'] else "{}",
                revealed=json.loads(row['revealed']) if row['revealed'] else []
            )
        except:
            return WorldState(immutable="{}", mutable="{}", revealed=[])

    def update_state(self, new_state: WorldState):
        db.execute(
            "INSERT INTO bible (book_id, immutable, mutable, revealed, last_updated) VALUES (?,?,?,?,?)",
            (
                self.book_id,
                new_state.immutable, 
                new_state.mutable,   
                json.dumps(new_state.revealed, ensure_ascii=False),
                datetime.datetime.now().isoformat()
            )
        )

    def get_prompt_context(self) -> str:
        state = self.get_current_state()
        return f"""
【WORLD STATE (Current)】
[IMMUTABLE]: {state.immutable}
[MUTABLE]: {state.mutable}
[REVEALED]: {json.dumps(state.revealed, ensure_ascii=False)}
"""

# ==========================================
# 3. Adaptive Rate Limiter
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
            if self.limit < 10: 
                self.limit += 1

    async def report_failure(self):
        async with self.lock:
            old_limit = self.limit
            self.limit = max(self.min_limit, self.limit // 2)
            print(f"📉 Circuit Breaker Triggered: Limit reduced {old_limit} -> {self.limit}")
            await asyncio.sleep(5) 

# ==========================================
# 4. ULTRA Engine (Autopilot)
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

    def _generate_system_rules(self, mc_profile, style="style_web_standard"):
        p_data = mc_profile.get('pronouns', {})
        k_data = mc_profile.get('keyword_dictionary', {})
        if isinstance(p_data, str):
            try: p_data = json.loads(p_data)
            except: pass
        if isinstance(k_data, str):
            try: k_data = json.loads(k_data)
            except: pass
            
        pronouns_json = json.dumps(p_data, ensure_ascii=False)
        keywords_json = json.dumps(k_data, ensure_ascii=False)
        monologue = mc_profile.get('monologue_style', '標準')

        style_def = STYLE_DEFINITIONS.get(style, STYLE_DEFINITIONS["style_web_standard"])
        style_sample = STYLE_SAMPLES.get(style, STYLE_SAMPLES["style_web_standard"])

        return PROMPT_TEMPLATES["system_rules"].format(
            pronouns=pronouns_json, 
            keywords=keywords_json, 
            monologue_style=monologue,
            style_name=style_def["name"],
            style_instruction=style_def["instruction"],
            few_shot_sample=style_sample
        )

    async def _generate_with_retry(self, model, contents, config, retries=10, initial_delay=2.0):
        await self.rate_limiter.acquire()
        try:
            for attempt in range(retries):
                try:
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
                        wait_time = (initial_delay * (2 ** attempt)) + random.uniform(0.5, 1.5)
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
        """第1段階: 1-25話のプロット生成 (50話構成へ変更)"""
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
作品設定と、前半パートである**第1話〜第25話**の詳細プロットを作成せよ。プロットは１話１０００字程度
前半のクライマックス（第25話）に向けて、テンションを高めていくこと。
注: mc_profile内の pronouns と keyword_dictionary は有効なJSON文字列として出力すること。
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
            
            if 'mc_profile' in data:
                if isinstance(data['mc_profile'].get('pronouns'), str):
                    try: data['mc_profile']['pronouns'] = json.loads(data['mc_profile']['pronouns'])
                    except: data['mc_profile']['pronouns'] = {}
                if isinstance(data['mc_profile'].get('keyword_dictionary'), str):
                    try: data['mc_profile']['keyword_dictionary'] = json.loads(data['mc_profile']['keyword_dictionary'])
                    except: data['mc_profile']['keyword_dictionary'] = {}

            return data
        except Exception as e:
            print(f"Plot Phase 1 Error: {e}")
            return None

    async def generate_universe_blueprint_phase2(self, genre, style, mc_personality, mc_tone, keywords, data1):
        """第2段階: 26-50話のプロット生成"""
        print("Step 1 (Parallel): Hyper-Resolution Plot Generation Phase 2 (Ep 26-50)...")
        
        context_summ = "\n".join([f"Ep{p['ep_num']}: {p['resolution'][:50]}..." for p in data1['plots']])
        prompt = f"""
あなたはWeb小説の神級プロットアーキテクトです。
全50話完結の物語構造の後半を作成します。

【これまでの流れ (Ep1-25)】
{context_summ}

【Task: Phase 2 (Ep 26-50)】
前半の続きとして、**第26話〜第50話（最終話）**を作成せよ。プロットは１話１０００字程度
物語の伏線を回収し、感動的なフィナーレへ導くこと。
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
        """テキストベースの整合性チェック"""
        state = bible_manager.get_current_state()
        prompt = f"""
あなたは物語の整合性を監査するAIロジックです。
以下のエピソード本文と「Bible（世界設定）」を比較し、矛盾を検出してください。

【Bible】
Immutable: {state.immutable}
Mutable: {state.mutable}

【Episode Text】
{ep_text[:3000]}... (Excerpt)

判定基準:
1. 死んだはずのキャラが生きていないか？
2. 設定された物理法則や能力に違反していないか？
3. キャラの口調や一人称が崩壊していないか？

重大な矛盾がある場合は、以下の形式で出力せよ。矛盾がない場合は「SAFE」と出力せよ。

[DECISION] REWRITE_NEEDED
[REASONS]
- 矛盾点1
- 矛盾点2
"""
        try:
            res = await self._generate_with_retry(
                model=MODEL_LITE,
                contents=prompt,
                config=types.GenerateContentConfig(safety_settings=self.safety_settings)
            )
            text = res.text
            is_rewrite = "REWRITE_NEEDED" in text
            reasons = []
            if is_rewrite:
                reasons = re.findall(r"-\s*(.*)", text)
            
            return ConsistencyResult(
                is_consistent=not is_rewrite,
                fatal_errors=reasons,
                minor_errors=[],
                rewrite_needed=is_rewrite
            )
        except Exception as e:
            return ConsistencyResult(is_consistent=True, fatal_errors=[], minor_errors=[], rewrite_needed=False)

    async def sync_with_chapter(self, bible_manager, chapter_text):
        """テキストベースのBible自動更新"""
        current = bible_manager.get_current_state()
        prompt = f"""
あなたはデータベース管理者です。
以下のエピソード本文から「新たに確定した設定」「変化したステータス」「読者に開示された秘密」を抽出し、
WorldStateを更新してください。

【Current State】
Immutable: {current.immutable}
Mutable: {current.mutable}

【Episode Text】
{chapter_text[:5000]}

Task:
以下のタグ形式で出力せよ。JSONは使わず、キーバリュー形式のテキストで記述せよ。
[IMMUTABLE]
(変更なし、または新事実の追記)
[MUTABLE]
(位置、生死、所持品の変化)
[REVEALED]
(読者に開示された設定用語、カンマ区切り)
"""
        try:
            res = await self._generate_with_retry(
                model=MODEL_LITE,
                contents=prompt,
                config=types.GenerateContentConfig(safety_settings=self.safety_settings)
            )
            text = res.text
            
            imm_match = re.search(r"\[IMMUTABLE\]\s*(.*?)\s*(?=\[MUTABLE\]|$)", text, re.DOTALL)
            mut_match = re.search(r"\[MUTABLE\]\s*(.*?)\s*(?=\[REVEALED\]|$)", text, re.DOTALL)
            rev_match = re.search(r"\[REVEALED\]\s*(.*?)\s*$", text, re.DOTALL)
            
            imm_str = imm_match.group(1).strip() if imm_match else "{}"
            mut_str = mut_match.group(1).strip() if mut_match else "{}"
            rev_str = rev_match.group(1).strip() if rev_match else ""
            
            revealed_list = [x.strip() for x in rev_str.split(',') if x.strip()]
            
            new_state = WorldState(
                immutable=imm_str,
                mutable=mut_str,
                revealed=list(set(current.revealed + revealed_list))
            )
            bible_manager.update_state(new_state)
        except Exception as e:
            print(f"Bible Sync Error: {e}")

    async def write_episodes(self, book_data, start_ep, end_ep, style_dna_str="style_web_standard", target_model=MODEL_LITE, rewrite_instruction=None, semaphore=None):
        """
        【執筆エンジン大規模改修】ワンショット一括生成ロジック
        - 1話分のプロット全体を入力し、1500〜2000字を一気に出力する。
        - 3シーン分割ループを廃止。
        """
        
        all_plots = sorted(book_data['plots'], key=lambda x: x.get('ep_num', 999))
        target_plots = [p for p in all_plots if start_ep <= p.get('ep_num', -1) <= end_ep]
        if not target_plots: return None

        full_chapters = []
        bible_manager = DynamicBibleManager(book_data['book_id'])
        
        # 前話の文脈取得
        prev_ep_row = db.fetch_one("SELECT content, summary FROM chapters WHERE book_id=? AND ep_num=? ORDER BY ep_num DESC LIMIT 1", (book_data['book_id'], start_ep - 1))
        prev_context_text = prev_ep_row['content'][-500:] if prev_ep_row and prev_ep_row['content'] else "（物語開始）"

        system_rules = self._generate_system_rules(book_data['mc_profile'], style=style_dna_str)
        mc_name = book_data['mc_profile'].get('name', '主人公')
        
        vocab_filter = f"""
【Vocal Persona: {mc_name}】
- 知識レベル: 一般人レベル（専門用語は知らないこと）
- 禁止語彙: {json.dumps(book_data['mc_profile'].get('keyword_dictionary', {}), ensure_ascii=False)} 以外の難解な言葉
"""

        for plot in target_plots:
            ep_num = plot['ep_num']
            print(f"Hyper-Narrative Engine Writing Ep {ep_num} (One-Shot Mode)...")
            
            # モデル最適化ロジック: 1話、50話、高テンション回はPROモデルを使用
            tension = plot.get('tension', 50)
            current_model = MODEL_LITE
            if ep_num == 1 or ep_num == 50 or tension >= 80:
                current_model = MODEL_PRO
            
            # プロット情報の結合
            episode_plot_text = f"""
【Episode Title】{plot['title']}
【Setup (導入)】 {plot.get('setup', '')}
【Conflict (展開)】 {plot.get('conflict', '')}
【Climax (見せ場)】 {plot.get('climax', '')}
【Resolution (結末・引き)】 {plot.get('resolution', '')}
"""
            
            # 執筆プロンプト構築
            write_prompt = f"""
{system_rules}
{vocab_filter}
{PROMPT_TEMPLATES["writing_rules"]}
{PROMPT_TEMPLATES["cliffhanger_protocol"]}

【Role: Novelist ({current_model})】
以下のプロットに基づき、**第{ep_num}話**の本文を一括執筆せよ。

【前話からの文脈】
...{prev_context_text}

【今回のプロット】
{episode_plot_text}

【World Context (Bible)】
{bible_manager.get_prompt_context()}

【Rewrite Instruction】
{rewrite_instruction if rewrite_instruction else "なし"}
"""
            
            scene_text = ""
            async with semaphore:
                try:
                    res = await self._generate_with_retry(
                        model=current_model, 
                        contents=write_prompt,
                        config=types.GenerateContentConfig(safety_settings=self.safety_settings) # Text Output
                    )
                    scene_text = res.text
                except Exception as e:
                    print(f"Writing Error Ep{ep_num}: {e}")
                    scene_text = "（生成エラーが発生しました）"

            full_content = scene_text.strip()
            prev_context_text = full_content[-500:] # 次の話のために更新

            # --- Auto-Sync Bible (1話完了後に実施) ---
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

    async def _summarize_chunk(self, text_chunk, start_ep, end_ep):
        prompt = f"""
【Task: Context Compression】 以下の第{start_ep}話〜第{end_ep}話の本文を、物語の重要ポイントを漏らさず1000文字程度に「濃縮要約」せよ。
{text_chunk[:10000]}...
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
        """MODEL_MARKETING (Gemini 2.0 Flash Lite) を使用して分析"""
        print("Starting Recursive Analysis (Sliding Window)...")
        
        chapters = db.fetch_all("SELECT ep_num, title, summary, content FROM chapters WHERE book_id=? ORDER BY ep_num", (book_id,))
        book_info = db.fetch_one("SELECT title FROM books WHERE id=?", (book_id,))
        if not chapters: return [], [], None

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
注: marketing_assets はJSON形式の文字列として出力すること。

Task 1: 各話スコアリング & 改善提案
Task 2: マーケティング素材生成 (キャッチコピー、タグ、近況ノート)

【作品タイトル】{book_info['title']}
【物語全体ダイジェスト】
{master_context}
"""
        try:
            # JSONモード対応モデルを使用
            res = await self._generate_with_retry(
                model=MODEL_MARKETING,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=MarketingAssets,
                    safety_settings=self.safety_settings
                )
            )
            data = MarketingAssets.model_validate_json(res.text)
            
            marketing_assets_dict = {}
            if data.marketing_assets:
                try: marketing_assets_dict = json.loads(data.marketing_assets)
                except: pass
            
            rewrite_target_eps = []
            evaluations_list = [e.model_dump() for e in data.evaluations]
            
            for evaluation in evaluations_list:
                if evaluation.get('total_score', 0) < 60: 
                      rewrite_target_eps.append(evaluation.get('ep_num'))
            
            db.execute("UPDATE books SET marketing_data=? WHERE id=?", (json.dumps(marketing_assets_dict, ensure_ascii=False), book_id))
            
            return evaluations_list, rewrite_target_eps, marketing_assets_dict
            
        except Exception as e:
            print(f"Analysis Error: {e}")
            return [], [], None

    async def rewrite_target_episodes(self, book_data, target_ep_ids, evaluations, style_dna_str="style_web_standard"):
        """リライト処理"""
        rewritten_count = 0
        semaphore = asyncio.Semaphore(1) 
        eval_map = {e['ep_num']: e for e in evaluations}
        tasks = []
        bible_manager = DynamicBibleManager(book_data['book_id'])

        for ep_id in target_ep_ids:
            chapter_row = db.fetch_one("SELECT content FROM chapters WHERE book_id=? AND ep_num=?", (book_data['book_id'], ep_id))
            consistency = await self.evaluate_consistency(chapter_row['content'], bible_manager)
            
            if not consistency.rewrite_needed and ep_id not in target_ep_ids:
                continue

            eval_data = eval_map.get(ep_id, {})
            instruction = f"【編集指示】\n{eval_data.get('improvement_point', '')}\n矛盾修正: {','.join(consistency.fatal_errors)}"
            
            # リライトはPROモデルで行う
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
                self.save_chapters_to_db(book_data['book_id'], res['chapters'])
                rewritten_count += 1
        return rewritten_count

    def save_blueprint_to_db(self, data, genre, style_dna_str):
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
        bid = db.execute(
            "INSERT INTO books (title, genre, synopsis, concept, target_eps, style_dna, status, special_ability, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (data_dict['title'], genre, data_dict['synopsis'], data_dict['concept'], 50, dna, 'active', ability_val, datetime.datetime.now().isoformat())
        )
        c_dna = json.dumps(data_dict['mc_profile'], ensure_ascii=False)
        monologue_val = data_dict['mc_profile'].get('monologue_style', '')
        db.execute("INSERT INTO characters (book_id, name, role, dna_json, monologue_style) VALUES (?,?,?,?,?)", (bid, data_dict['mc_profile']['name'], '主人公', c_dna, monologue_val))
        
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
        saved_style = style_dna_json.get('style_mode', 'style_web_standard')
    except:
        saved_style = 'style_web_standard'
    mc_profile = json.loads(mc['dna_json']) if mc and mc['dna_json'] else {"name":"主人公", "tone":"標準"}
    mc_profile['monologue_style'] = mc.get('monologue_style', '') 

    for p in plots:
        if p.get('scenes'):
            try: p['scenes'] = json.loads(p['scenes'])
            except: pass

    full_data = {"book_id": bid, "title": book_info['title'], "mc_profile": mc_profile, "plots": [dict(p) for p in plots]}
    
    # 一括生成なのでAPI負荷が低いため、並列数を少し上げる
    semaphore = asyncio.Semaphore(3) 

    tasks = []
    print(f"Starting Machine-Gun Parallel Writing (Ep {start_ep} - {end_ep})...")

    target_plots = [p for p in plots if start_ep <= p['ep_num'] <= end_ep]

    for p in target_plots:
        ep_num = p['ep_num']
        
        # モデル選択は write_episodes 内で行われるが、念のためデフォルトを渡す
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
# 3. Main Logic
# ==========================================
def load_seed():
    style_keys = list(STYLE_DEFINITIONS.keys())
    selected_style = random.choice(style_keys)
    
    if not os.path.exists("story_seeds.json"):
        return {
            "genre": "現代ダンジョン",
            "keywords": "配信, 事故, 無双",
            "personality": "冷静沈着",
            "tone": "俺",
            "hook_text": "配信切り忘れで世界最強がバレる",
            "style": selected_style
        }

    with open("story_seeds.json", "r", encoding='utf-8') as f:
        data = json.load(f)
        seed = random.choice(data['seeds'])
        tmpl = random.choice(seed['templates'])
        twists = ["記憶喪失", "実は2周目", "相棒がラスボス", "寿命が残りわずか"]
        twist = random.choice(twists)
        
        print(f"★ Selected: {seed['genre']} - {tmpl['type']} (Style: {STYLE_DEFINITIONS[selected_style]['name']})")
        return {
            "genre": seed['genre'],
            "keywords": f"{tmpl['keywords']}, {twist}",
            "personality": tmpl['mc_profile'],
            "tone": "俺",
            "hook_text": tmpl['hook'],
            "style": selected_style
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
            if isinstance(keyword_dict, str):
                keyword_dict = json.loads(keyword_dict)
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

    print("Starting Factory Pipeline (Async / One-Shot Mode)...")

    #while True:
    try:
        seed = load_seed()
            
            # Step 1: 1-25話プロット
        print("Step 1a: Generating Plot Phase 1 (Ep 1-25)...")
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
            
            # Phase 1 執筆 (1-25話)
            task_write_p1 = asyncio.create_task(
                task_write_batch(engine, bid, start_ep=1, end_ep=25)
            )
            
            # Phase 2 プロット生成 (26-50話)
            task_gen_p2 = asyncio.create_task(
                task_plot_gen_phase2(
                    engine, bid, seed['genre'], seed['style'], seed['personality'], seed['tone'], seed['keywords'], data1
                )
            )
            
            count_p1, full_data_p1, saved_style = await task_write_p1
            await task_gen_p2
            
            print("Parallel Execution Completed. Proceeding to Write Phase 2 (Ep 26-50)...")

            # Phase 2 執筆 (26-50話)
            count_p2, full_data_final, _ = await task_write_batch(engine, bid, start_ep=26, end_ep=50)
            
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