import asyncio
import os
import random
import re
import json
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import aiohttp
from aiohttp import FormData

TOP_URL = "https://utaten.com/"
API_URL = "https://utaten.com/lyric/hopeVote"
CONFIG_FILE = "cli_config.json"

class SharedState:
    def __init__(self):
        self._count = 0
        self._last_point = None
        self._lock = None

    def init_lock(self):
        self._lock = asyncio.Lock()

    async def increment(self):
        async with self._lock:
            self._count += 1
            return self._count

    async def update_point(self, point):
        async with self._lock:
            if point is not None:
                self._last_point = point

    @property
    def value(self):
        return self._count

    @property
    def last_point(self):
        return self._last_point

def load_user_agents(file_path="11000UA.txt"):
    default_ua = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) Gecko/20100101 Firefox/152.0"]
    if not os.path.exists(file_path):
        return default_ua
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]
    except Exception:
        return default_ua

def load_proxies(file_path="proxies.txt"):
    if not os.path.exists(file_path):
        return []
    try:
        proxies = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if not line.startswith("http://") and not line.startswith("https://"):
                    line = f"http://{line}"
                proxies.append(line)
        return proxies
    except Exception:
        return []

UA_LIST = load_user_agents("11000UA.txt")
PROXY_LIST = load_proxies("proxies.txt")


def save_config(config_data):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config_data, f, ensure_ascii=False, indent=4)
        print(f"[System] 設定を {CONFIG_FILE} に保存しました。")
    except Exception as e:
        print(f"[System Error] 設定の保存に失敗しました: {e}")

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


async def fetch_session_parameters(session, target_url, custom_ua=None, proxy=None):
    ua = custom_ua if custom_ua else UA_LIST[0]
    session.headers.update({
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    })
    try:
        async with session.get(TOP_URL, timeout=10, proxy=proxy) as resp:
            await resp.text()

        session.headers.update({"Referer": TOP_URL})
        async with session.get(target_url, timeout=10, proxy=proxy) as resp:
            resp.raise_for_status()
            html_content = await resp.text()

        soup = BeautifulSoup(html_content, "html.parser")
        token = None
        element = soup.find(attrs={"data-token": True})
        if element and element.has_attr("data-token"):
            token = element["data-token"]

        lyric_id = None
        match = re.search(r"var\s+LYRIC_ID\s*=\s*(\d+)", html_content)
        if match:
            lyric_id = match.group(1)

        if token and lyric_id:
            return token, lyric_id, f"成功 (ID: {lyric_id}, Token: {token[:15]}...)"
        return None, None, "エラー: 要素のパースに失敗しました。"
    except asyncio.CancelledError:
        raise
    except Exception as e:
        return None, None, f"アクセス失敗: {str(e)}"


async def worker_task(task_id, target_url, get_end_time_func, config, state, log_on):
    local_loop_count = 0
    current_proxy = None
    
    # セッションをループの外で1度だけ生成し、ライフサイクルを保護する
    session = aiohttp.ClientSession()

    try:
        while True:
            if config["time_control_enabled"] and datetime.now() >= get_end_time_func():
                break
            if config["total_requests"] > 0 and state.value >= config["total_requests"]:
                break
            if config["target_point"] > 0 and state.last_point is not None:
                if state.last_point >= config["target_point"]:
                    break

            if local_loop_count % 10 == 0:
                # セッションの再生成を廃止し、Cookie（状態）のみをクリアする
                session.cookie_jar.clear()
                
                if config["use_proxy"] and PROXY_LIST:
                    current_proxy = random.choice(PROXY_LIST)
                    if log_on:
                        print(f"[{task_id}][プロキシ] 今回のサイクルで使用するIP: {current_proxy}")
                else:
                    current_proxy = None

                current_ua = random.choice(UA_LIST) if config["use_random_ua"] else None
                token, lyric_id, msg = await fetch_session_parameters(session, target_url, current_ua, current_proxy)
                
                if not (token and lyric_id):
                    # 取得失敗時もセッションは破棄せず、Cookieをクリアしてリトライ
                    session.cookie_jar.clear()
                    await asyncio.sleep(2.0)
                    continue

            api_headers = {
                "X-Requested-With": "XMLHttpRequest",
                "Origin": "https://utaten.com",
                "Referer": target_url,
                "Accept": "application/json, text/javascript, */*; q=0.01",
            }

            form_data = FormData(charset="utf-8")
            form_data.add_field('id', str(lyric_id))
            form_data.add_field('token', str(token))

            try:
                if (config["time_control_enabled"] and datetime.now() >= get_end_time_func()) or (config["total_requests"] > 0 and state.value >= config["total_requests"]):
                    break
                if config["target_point"] > 0 and state.last_point is not None and state.last_point >= config["target_point"]:
                    break

                if log_on:
                    print(f"[{task_id}][Request] API送信処理中... (現在の総送信数: {state.value:,})")

                async with session.post(API_URL, data=form_data, headers=api_headers, timeout=10, proxy=current_proxy) as resp:
                    status = resp.status
                    response_text = await resp.text()
                    
                    current_total = await state.increment()
                    
                    try:
                        resp_json = json.loads(response_text)
                        if "point" in resp_json and resp_json["point"] is not None:
                            await state.update_point(int(resp_json["point"]))
                    except Exception:
                        pass

                    if log_on:
                        p_raw = json.loads(response_text).get("point") if "point" in response_text else None
                        p_display = f"{p_raw * 10:,} pts" if p_raw is not None else "null"
                        print(f"[{task_id}][Response] ステータス: {status} (累計リクエスト数: {current_total:,})")
                        print(f"[{task_id}][Debugレスポンス（補正表示）]: status:{status}, point:{p_display}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                if log_on:
                    print(f"[{task_id}][Error] 送信エラー: {str(e)}")
                await asyncio.sleep(1.0)

            local_loop_count += 1

            mode = config["interval_mode"]
            if mode == 1:
                sleep_time = config["base_interval"]
            elif mode == 2:
                sleep_time = random.uniform(config["base_interval"], config["base_interval"] + 2.0)
            else:
                sleep_time = random.uniform(config["min_interval"], config["max_interval"])
            
            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        pass
    finally:
        # ループやタスクが終了した際に、ここで確実に1度だけセッションを解放する
        if session and not session.closed:
            await session.close()

async def monitor_task(state, config, get_end_time_func, log_on):
    if not log_on:
        print("\n[System] ログ出力がOFF（ミュート）に設定されています。進捗のみを5秒おきに表示します。")
    
    try:
        while True:
            now = datetime.now()
            end_time = get_end_time_func()
            
            if config["time_control_enabled"] and now >= end_time:
                print("\n[Time Over] 指定された稼働時間に到達したため、プロセスを自動停止します。")
                break
            if config["total_requests"] > 0 and state.value >= config["total_requests"]:
                break
            if config["target_point"] > 0 and state.last_point is not None and state.last_point >= config["target_point"]:
                print(f"\n[Target Reached] 目標ポイント（{(config['target_point'] * 10):,} pts）を達成したため、自動停止します。")
                break

            total_str = "無限" if config["total_requests"] == 0 else f"{config['total_requests']:,}"
            point_str = f"{(state.last_point * 10):,} pts" if state.last_point is not None else "未取得"
            target_p_str = "制限なし" if config["target_point"] == 0 else f"{(config['target_point'] * 10):,} pts"

            if config["time_control_enabled"]:
                time_left = end_time - now
                time_str = str(time_left).split(".")[0]
                suffix_str = f" | 残り時間: {time_str}"
            else:
                suffix_str = ""

            if not log_on:
                print(f" >> [進捗] 送信数: {state.value:,}/{total_str} | 現在Pt: {point_str} (目標: {target_p_str}){suffix_str}")
            
            await asyncio.sleep(5.0)
    except asyncio.CancelledError:
        pass

def parse_absolute_time(time_str):
    parts = list(map(int, time_str.split(":")))
    now = datetime.now()
    if len(parts) == 2:
        target_time = now.replace(hour=parts[0], minute=parts[1], second=0, microsecond=0)
    elif len(parts) == 3:
        target_time = now.replace(hour=parts[0], minute=parts[1], second=parts[2], microsecond=0)
    else:
        raise ValueError
    
    if target_time < now:
        target_time += timedelta(days=1)
    return target_time

async def engine_main(state, config, get_end_time_func, log_on):
    state.init_lock()
    tasks = []
    
    for t_idx in range(config["concurrency"]):
        task_id = f"Task-{t_idx+1:02d}"
        tasks.append(asyncio.create_task(worker_task(task_id, config["target_url"], get_end_time_func, config, state, log_on)))
    
    tasks.append(asyncio.create_task(monitor_task(state, config, get_end_time_func, log_on)))

    await asyncio.gather(*tasks)


def run_async_engine(state, config, current_end_time, log_on):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    get_end_time = lambda: current_end_time
    
    try:
        loop.run_until_complete(engine_main(state, config, get_end_time, log_on))
        return "DONE"
    except KeyboardInterrupt:
        tasks = asyncio.all_tasks(loop)
        for t in tasks:
            t.cancel()
        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        return "PAUSED"
    finally:
        loop.close()


def main():
    print("=== utaten ランキング支援ツール ===")
    
    saved_config = load_config()
    use_saved = False
    log_on = True

    if saved_config:
        print("\n--- 保存された過去の設定が見つかりました ---")
        print(f" URL: {saved_config.get('target_url')}")
        print(f" 総リクエスト数: {saved_config.get('total_requests', 0):,}")
        print(f" スレッド（タスク）数: {saved_config.get('concurrency')}")
        print(f" プロキシ利用モード: {'ON' if saved_config.get('use_proxy') else 'OFF'}")
        saved_target = saved_config.get('target_point', 0)
        print(f" 目標ポイント: {f'{(saved_target * 10):,}' if saved_target > 0 else '0'} pts")
        print(f" 時間制御モード使用: {'YES' if saved_config.get('time_control_enabled') else 'NO'}")
        print(f" 詳細ログ画面出力: {'ON (表示する)' if saved_config.get('log_on', True) else 'OFF (進捗行のみ)'}")
        
        ans = input("この設定をそのまま読み込み、プロセスを即時開始しますか？ (y/n): ").strip().lower()
        if ans == 'y':
            config = saved_config
            log_on = config.get("log_on", True)
            use_saved = True

    if not use_saved:
        config = {}
        config["target_url"] = input("\nターゲットURLを入力してください: ").strip()
        
        while True:
            try:
                config["total_requests"] = int(input("リクエスト総数を入力してください (0で無限ループ): "))
                break
            except ValueError:
                print("有効な数字を入力してください。")

        while True:
            try:
                config["concurrency"] = int(input("同時実行スレッド（タスク）数を入力してください (例 1〜10): "))
                if config["concurrency"] > 0:
                    break
            except ValueError:
                print("1以上の整数を入力してください。")

        config["use_random_ua"] = input("User-Agentのランダム化を行いますか？ (y/n): ").strip().lower() == 'y'
        config["use_proxy"] = input("プロキシプール（proxies.txt）を使用しますか？ (y/n): ").strip().lower() == 'y'
        if config["use_proxy"]:
            if not PROXY_LIST:
                print("[Warning] proxies.txt が存在しないか、有効なIPがありません。プロキシをOFFのまま進行します。")
                config["use_proxy"] = False
            else:
                print(f"[Info] プロキシプールから {len(PROXY_LIST)} 件のIPアドレスをロードしました。")

        print("\n--- インターバルモードの選択 ---")
        print(" [1] 固定インターバル（ユーザー指定の等間隔）")
        print(" [2] 自動変動（基準値ベースで最大+2秒変動）")
        print(" [3] カスタム範囲（スライダー相当のMin/Max指定）")
        while True:
            try:
                mode = int(input("モードを番号で選んでください (1/2/3): "))
                if mode in [1, 2, 3]:
                    config["interval_mode"] = mode
                    break
            except ValueError:
                pass
            print("1, 2, 3 のいずれかを入力してください。")

        if mode in [1, 2]:
            config["base_interval"] = float(input("基準インターバル（秒）を指定してください: "))
            config["min_interval"], config["max_interval"] = 0.0, 0.0
        else:
            config["min_interval"] = float(input("最小インターバル（Min秒）: "))
            config["max_interval"] = float(input("最大インターバル（Max秒）: "))
            config["base_interval"] = 0.0

        while True:
            try:
                raw_point = int(input("\n目標ポイント数を指定してください (Web画面上の数字のまま入力、0で制限なし): "))
                config["target_point"] = raw_point // 10
                break
            except ValueError:
                print("整数で入力してください。")

        config["time_control_enabled"] = input("\n時刻指定や稼働時間制限などの「時間制御」を使用しますか？ (y/n): ").strip().lower() == 'y'
        
        config["time_mode"] = 0
        config["duration_minutes"] = 0
        config["start_time_str"] = ""
        config["end_time_str"] = ""

        if config["time_control_enabled"]:
            print("\n--- 時間制御モードの選択 ---")
            print(" [1] 従来通り「分数」で指定する（例: 今から60分間動かす）")
            print(" [2] 本体時刻ベースの「時間指定」で稼働する（例: 02:00から05:30まで自動稼働）")
            while True:
                try:
                    config["time_mode"] = int(input("モードを選んでください (1/2): "))
                    if config["time_mode"] in [1, 2]:
                        break
                except ValueError:
                    pass
                print("1 または 2 を入力してください。")

            if config["time_mode"] == 1:
                while True:
                    try:
                        config["duration_minutes"] = int(input("稼働制限時間を分単位で指定してください: "))
                        break
                    except ValueError:
                        print("整数で入力してください。")
            else:
                config["start_time_str"] = input("稼働を開始する本体時刻を入力してください (即時開始なら 0、指定なら 例 02:00): ").strip()
                config["end_time_str"] = input("稼働を終了する本体時刻を入力してください (例 05:30 または 05:30:00): ").strip()

        log_on = input("\n画面に詳細な実行ログを出力しますか？ (y/n): ").strip().lower() == 'y'
        config["log_on"] = log_on

        save_ans = input("\n今回入力した設定を保存して次回自動ロードできるようにしますか？ (y/n): ").strip().lower()
        if save_ans == 'y':
            save_config(config)

    start_time = datetime.now()
    end_time = datetime.now() + timedelta(days=3650)

    if config["time_control_enabled"]:
        if config["time_mode"] == 1:
            end_time = datetime.now() + timedelta(minutes=config["duration_minutes"])
        else:
            try:
                start_time = parse_absolute_time(config["start_time_str"])
            except ValueError:
                print("[Error] 開始時刻の書式が不正です。即時開始します。")
                start_time = datetime.now()
            
            try:
                end_time = parse_absolute_time(config["end_time_str"])
            except ValueError:
                print("[Critical Error] 終了時刻の書式が不正です。時間制御をOFFにして進行します。")
                config["time_control_enabled"] = False

            if config["time_control_enabled"] and start_time >= end_time:
                end_time += timedelta(days=1)

    if config["time_control_enabled"] and config["time_mode"] == 2 and start_time > datetime.now():
        wait_seconds = (start_time - datetime.now()).total_seconds()
        print(f"\n[Scheduler] 現在時刻: {datetime.now().strftime('%H:%M:%S')}")
        print(f"[Scheduler] 設定された開始時刻（{start_time.strftime('%H:%M:%S')}）まで自律待機します。")
        print(f"[Scheduler] 起動まであと {wait_seconds/60:.1f} 分です。このままお待ちください...")
        
        try:
            while datetime.now() < start_time:
                time.sleep(1.0)
        except KeyboardInterrupt:
            print("\n 開始待機中にユーザーにより中断されました。")
            return

    state = SharedState()
    
    print(f"\n プロセスを起動しました。現在の時刻: {datetime.now().strftime('%H:%M:%S')}")
    print("中断もしくは停止する場合は Ctrl+C を押してください。")
    
    start_time_stamp = time.time()

    while True:
        status = run_async_engine(state, config, end_time, log_on)
        
        if status == "DONE":
            break
        elif status == "PAUSED":
            p_paused = f"{(state.last_point * 10):,} pts" if state.last_point is not None else '未取得'
            print("\n\n[一時停止] ユーザーによる中断（Ctrl+C）を検知しました。")
            print(f"現在の総送信成功回数: {state.value:,} 回 | 最終確認ポイント: {p_paused}")

            time_remaining = end_time - datetime.now()

            print("\n--- 次の操作を選択してください ---")
            print(" [1] 処理を再開する")
            print(" [2] 完全停止して終了する")
            
            choice = ""
            while choice not in ["1", "2"]:
                try:
                    choice = input("番号を入力してください (1/2): ").strip()
                except KeyboardInterrupt:
                    choice = "2"
            
            if choice == "1":
                print("\n処理を再開します...")
                if config["time_control_enabled"]:
                    end_time = datetime.now() + time_remaining
                continue
            else:
                print("\n完全停止を選択しました。結果確認のため、3秒後に終了処理へ移行します...")
                time.sleep(3)
                break

    elapsed = time.time() - start_time_stamp
    print("\n================ 最終稼働レポート ================")
    print(f" 稼働時間          : {elapsed / 60:.2f} 分")
    print(f" 総送信成功回数     : {state.value:,} 回")
    p_final = state.last_point
    print(f" 最終確認ポイント   : {f'{(p_final * 10):,}' if p_final is not None else '未取得'} pts")
    print("==================================================")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
