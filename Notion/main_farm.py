# main_farm.py
# ============================================================
# 역할: 한 농장당 프로세스 1개로, 해당 농장의 모든 컨트롤러(축사×방) STATE를
#       랜덤 값으로 생성해 MQTT로 발행. (100개 프로세스 대신 1개 프로세스)
# 사용: run_farm.py --single-process 시 이 스크립트를 농장당 1회 실행.
# ============================================================

import sys
import os
import json
import random
import time
import logging
import threading
from collections import deque

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_THIS_DIR)
_INTEGRATOR = os.path.join(_THIS_DIR, "ctr", "integrator")
sys.path.insert(0, _ROOT)
sys.path.insert(0, _INTEGRATOR)

from dotenv import load_dotenv
load_dotenv(os.path.join(_THIS_DIR, ".env"))

from payload_builder import build_payload_from_room_state, build_key12
from mqtt_client import IntegratorMQTTClient
from config import get_vent_mode

# 환경변수
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "127.0.0.1")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", None)
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", None)

REGIST_NO = os.getenv("REGIST_NO", "FARM00")
SPECIES_CHAR = os.getenv("SPECIES_CHAR", "P")
STALL_TY_CODE = int(os.getenv("STALL_TY_CODE", "1"))
NUM_BARNS = int(os.getenv("NUM_BARNS", "5"))
ROOMS_PER_BARN = int(os.getenv("ROOMS_PER_BARN", "20"))

UPLINK_INTERVAL = float(os.getenv("UPLINK_INTERVAL", "300"))
# 컨트롤러 데이터 간 전송 간격(초). 100건 발송 시 1건 보낸 뒤 이 시간만큼 대기 후 다음 발송.
CONTROLLER_SEND_INTERVAL_SEC = float(os.getenv("CONTROLLER_SEND_INTERVAL_SEC", "3.0"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
STARTUP_DELAY = float(os.getenv("STARTUP_DELAY", "1.0"))
STARTUP_JITTER_MAX = float(os.getenv("STARTUP_JITTER_MAX", "0"))
ACK_STATS_INTERVAL_SEC = float(os.getenv("ACK_STATS_INTERVAL_SEC", "30"))
ACK_RESEND_ENABLED = os.getenv("ACK_RESEND_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
ACK_RESEND_TIMEOUT_SEC = float(os.getenv("ACK_RESEND_TIMEOUT_SEC", "3.0"))
ACK_RESEND_MAX_RETRIES = int(os.getenv("ACK_RESEND_MAX_RETRIES", "-1"))  # -1: 무제한 재시도
RETRY_TICK_SEC = float(os.getenv("ACK_RETRY_TICK_SEC", "0.2"))
ACK_BLOCKING_MODE = os.getenv("ACK_BLOCKING_MODE", "0").strip().lower() in {"1", "true", "yes", "on"}

# 랜덤 범위 (온습도, 모터 RPM)
TEMP_MIN = float(os.getenv("SIM_TEMP_MIN", "20.0"))
TEMP_MAX = float(os.getenv("SIM_TEMP_MAX", "30.0"))
HUMID_MIN = float(os.getenv("SIM_HUMID_MIN", "40.0"))
HUMID_MAX = float(os.getenv("SIM_HUMID_MAX", "80.0"))
RPM_MAX = int(os.getenv("SIM_RPM_MAX", "1500"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# 유실 분석용: run_farm --metrics-file 설정 시 자식에 METRICS_FILE 전달됨
METRICS_FILE = (os.getenv("METRICS_FILE") or "").strip()

mqtt_client = None
_ack_lock = threading.Lock()
_ack_total = 0
_last_ack_ts_ms = None
_sent_total = 0
_resent_total = 0
_drop_unacked_total = 0
_last_ack_stats_log_mono = 0.0
_pending_by_seq = {}
_pending_seq_order = deque()
_ref_seq_counter = int(time.time() * 1000) & 0xFFFFFFFF
_awaiting_ack_ref_seq = None
_awaiting_ack_event = threading.Event()


def _utc_ms() -> int:
    return int(time.time() * 1000)


def _metric_append(record: dict) -> None:
    """METRICS_FILE에 JSONL 한 줄 append (다중 프로세스에서 같은 파일에 쓸 수 있음)."""
    if not METRICS_FILE:
        return
    line = json.dumps(record, ensure_ascii=False) + "\n"
    try:
        with open(METRICS_FILE, "a", encoding="utf-8") as fp:
            fp.write(line)
    except Exception as e:
        logging.debug(f"[Farm] metrics append failed: {e}")


def _on_uplink_ack(ack: dict) -> None:
    """receiver UPLINK_ACK 수신 콜백."""
    global _ack_total, _last_ack_ts_ms
    with _ack_lock:
        _ack_total += 1
        ack_ts = ack.get("ack_ts_ms")
        if isinstance(ack_ts, int):
            _last_ack_ts_ms = ack_ts
        ref_seq = ack.get("ref_seq")
        if isinstance(ref_seq, int):
            if _awaiting_ack_ref_seq == ref_seq:
                _awaiting_ack_event.set()
            _pending_by_seq.pop(ref_seq, None)


def _ack_snapshot():
    with _ack_lock:
        pending_total = len(_pending_by_seq)
        return _ack_total, _last_ack_ts_ms, pending_total, _resent_total, _drop_unacked_total


def _next_ref_seq() -> int:
    global _ref_seq_counter
    with _ack_lock:
        _ref_seq_counter = (_ref_seq_counter + 1) & 0xFFFFFFFF
        return _ref_seq_counter


def _track_pending_message(key12: str, payload: bytes, ref_seq: int) -> None:
    """ACK 대기 큐에 전송 성공 메시지를 적재."""
    if not ACK_RESEND_ENABLED:
        return
    with _ack_lock:
        seq_u32 = int(ref_seq) & 0xFFFFFFFF
        if seq_u32 not in _pending_by_seq:
            _pending_seq_order.append(seq_u32)
        _pending_by_seq[seq_u32] = {
            "key12": key12,
            "payload": payload,
            "sent_ts": time.time(),
            "retry_count": 0,
        }


def _retry_one_unacked_if_due() -> bool:
    """ACK 타임아웃 지난 메시지 1건 재전송. 성공 시 True."""
    global _resent_total, _drop_unacked_total
    if not ACK_RESEND_ENABLED or mqtt_client is None:
        return False
    timeout_sec = max(0.1, ACK_RESEND_TIMEOUT_SEC)
    now = time.time()
    with _ack_lock:
        while _pending_seq_order:
            ref_seq = _pending_seq_order[0]
            pending = _pending_by_seq.get(ref_seq)
            if pending is None:
                _pending_seq_order.popleft()
                continue
            sent_ts = float(pending.get("sent_ts", 0.0))
            if (now - sent_ts) < timeout_sec:
                return False
            key12 = pending["key12"]
            retry_count = int(pending.get("retry_count", 0))
            if ACK_RESEND_MAX_RETRIES >= 0 and retry_count >= ACK_RESEND_MAX_RETRIES:
                _pending_seq_order.popleft()
                _pending_by_seq.pop(ref_seq, None)
                _drop_unacked_total += 1
                logging.warning(
                    f"[Farm] {REGIST_NO}: drop unacked key12={key12} ref_seq={ref_seq} retry_count={retry_count}"
                )
                return False
            payload = pending["payload"]
            ok = mqtt_client.publish_state_to(key12, payload, ref_seq=ref_seq)
            if ok:
                pending["retry_count"] = retry_count + 1
                pending["sent_ts"] = now
                _resent_total += 1
                logging.debug(
                    f"[Farm] {REGIST_NO}: resent unacked key12={key12} ref_seq={ref_seq} "
                    f"retry_count={pending['retry_count']}"
                )
                return True
            return False
    return False


def _send_with_ack_blocking(key12: str, payload: bytes) -> bool:
    """
    Stop-and-wait 전송:
    - 1건 전송 후 ACK가 올 때까지 같은 payload를 timeout 간격으로 재전송.
    - ACK 수신 전에는 다음 메시지 전송 금지.
    """
    global _sent_total, _resent_total, _awaiting_ack_ref_seq
    if mqtt_client is None:
        return False
    timeout_sec = max(0.1, ACK_RESEND_TIMEOUT_SEC)
    retry_tick = max(0.05, RETRY_TICK_SEC)
    ref_seq = _next_ref_seq()
    with _ack_lock:
        _awaiting_ack_ref_seq = ref_seq
        _awaiting_ack_event.clear()
    _track_pending_message(key12, payload, ref_seq)
    first_ok = mqtt_client.publish_state_to(key12, payload, ref_seq=ref_seq)
    if not first_ok:
        with _ack_lock:
            _pending_by_seq.pop(ref_seq, None)
            _awaiting_ack_ref_seq = None
            _awaiting_ack_event.clear()
        logging.warning(f"[Farm] {REGIST_NO}: send failed key12={key12} ref_seq={ref_seq}")
        return False
    start_ts = time.time()
    _sent_total += 1
    logging.info(
        f"[Farm] {REGIST_NO}: sent key12={key12} ref_seq={ref_seq} awaiting_ack timeout={timeout_sec:.1f}s"
    )
    last_send_ts = time.time()
    retry_count = 0
    while True:
        if _awaiting_ack_event.wait(timeout=retry_tick):
            ack_rtt_ms = int((time.time() - start_ts) * 1000)
            with _ack_lock:
                _awaiting_ack_ref_seq = None
                _awaiting_ack_event.clear()
            logging.info(
                f"[Farm] {REGIST_NO}: ack received key12={key12} ref_seq={ref_seq} "
                f"ack_rtt_ms={ack_rtt_ms} retry_count={retry_count}"
            )
            return True
        now = time.time()
        if (now - last_send_ts) >= timeout_sec:
            # ACK 없으면 동일 payload를 계속 재전송
            if mqtt_client.publish_state_to(key12, payload, ref_seq=ref_seq):
                _resent_total += 1
                retry_count += 1
                last_send_ts = now
                logging.warning(
                    f"[Farm] {REGIST_NO}: ack timeout, resend key12={key12} ref_seq={ref_seq} "
                    f"retry_count={retry_count}"
                )
        _maybe_log_ack_stats(force=False)


def _sleep_with_retry(total_sec: float) -> None:
    """
    대기 중에도 ACK 타임아웃 재전송을 체크한다.
    - 전송 간격이 2초이고 timeout=1초면, 대기 중간(약 1초)에 재전송 가능.
    """
    if total_sec <= 0:
        return
    tick = max(0.05, RETRY_TICK_SEC)
    deadline = time.monotonic() + total_sec
    while True:
        remain = deadline - time.monotonic()
        if remain <= 0:
            break
        time.sleep(min(tick, remain))
        _retry_one_unacked_if_due()
        _maybe_log_ack_stats(force=False)


def _maybe_log_ack_stats(force: bool = False) -> None:
    """sent 대비 ack 관찰 로그 (기본 30초 간격)."""
    global _last_ack_stats_log_mono
    interval = max(1.0, ACK_STATS_INTERVAL_SEC)
    now_mono = time.monotonic()
    if not force and (now_mono - _last_ack_stats_log_mono) < interval:
        return
    ack_total, last_ack_ts_ms, pending_total, resent_total, drop_unacked_total = _ack_snapshot()
    now_ms = _utc_ms()
    last_ack_age_ms = (now_ms - last_ack_ts_ms) if isinstance(last_ack_ts_ms, int) else -1
    ack_gap = max(0, _sent_total - ack_total)
    logging.info(
        f"[Farm] {REGIST_NO}: ACK stats sent_total={_sent_total} ack_total={ack_total} "
        f"ack_gap={ack_gap} pending={pending_total} resent_total={resent_total} "
        f"drop_unacked_total={drop_unacked_total} last_ack_age_ms={last_ack_age_ms}"
    )
    _last_ack_stats_log_mono = now_mono


def _random_sensor_data():
    return {
        "humidity": round(random.uniform(HUMID_MIN, HUMID_MAX), 1),
        "temperature": round(random.uniform(TEMP_MIN, TEMP_MAX), 1),
    }


def _random_motor_state():
    """motor_flag(10), motor_rpm_map(10) 랜덤 생성. EC01(1), EC02(4), EC03(7) 항상 활성."""
    flag = [0] * 10
    rpm_map = [0] * 10
    for mid in (1, 4, 7):  # EC01, EC02, EC03
        flag[mid] = 1
        rpm_map[mid] = random.randint(1, RPM_MAX)
    return flag, rpm_map


def _send_all_controllers(jitter_sec: float = 0.0):
    """농장 내 모든 (stall_no, room_no)에 대해 랜덤 STATE 1회 발행. 컨트롤러 간 전송 간격 CONTROLLER_SEND_INTERVAL_SEC 적용."""
    global mqtt_client, _sent_total
    sent = 0
    interval_sec = max(0.0, CONTROLLER_SEND_INTERVAL_SEC)
    total = NUM_BARNS * ROOMS_PER_BARN
    idx = 0
    for stall_no in range(1, NUM_BARNS + 1):
        for room_no in range(1, ROOMS_PER_BARN + 1):
            idx += 1
            if jitter_sec > 0:
                time.sleep(random.uniform(0, jitter_sec))
            key12 = build_key12(REGIST_NO, STALL_TY_CODE, stall_no, room_no)
            sensor_data = _random_sensor_data()
            motor_flag, motor_rpm_map = _random_motor_state()
            vent_mode = get_vent_mode(key12)
            payload = build_payload_from_room_state(
                sensor_data=sensor_data,
                motor_flag=motor_flag,
                motor_rpm_map=motor_rpm_map,
                regist_no=REGIST_NO,
                species_char=SPECIES_CHAR,
                stall_ty_code=STALL_TY_CODE,
                stall_no=stall_no,
                room_no=room_no,
                vent_mode=vent_mode,
            )
            if ACK_BLOCKING_MODE:
                if _send_with_ack_blocking(key12, payload):
                    sent += 1
            else:
                ref_seq = _next_ref_seq()
                if mqtt_client and mqtt_client.publish_state_to(key12, payload, ref_seq=ref_seq):
                    sent += 1
                    _sent_total += 1
                    _track_pending_message(key12, payload, ref_seq)
                _retry_one_unacked_if_due()
            _maybe_log_ack_stats(force=False)
            # 컨트롤러 간 전송 간격 (마지막 건 제외)
            if interval_sec > 0 and idx < total:
                _sleep_with_retry(interval_sec)
    return sent


def main():
    global mqtt_client
    total = NUM_BARNS * ROOMS_PER_BARN
    logging.info(f"[Farm] {REGIST_NO}: {NUM_BARNS} barns × {ROOMS_PER_BARN} rooms = {total} controllers (single process)")

    first_key12 = build_key12(REGIST_NO, STALL_TY_CODE, 1, 1)
    mqtt_client = IntegratorMQTTClient(
        broker_host=MQTT_BROKER_HOST,
        broker_port=MQTT_BROKER_PORT,
        key12=first_key12,
        client_id=f"farm_{REGIST_NO}_{int(time.time())}",
        username=MQTT_USERNAME,
        password=MQTT_PASSWORD,
        on_downlink_callback=None,
        on_uplink_ack_callback=_on_uplink_ack,
    )
    if not mqtt_client.connect():
        logging.error("[Farm] MQTT connection failed, exiting")
        sys.exit(1)

    delay = max(0.0, STARTUP_DELAY)
    if delay > 0:
        time.sleep(delay)
    jitter = max(0.0, STARTUP_JITTER_MAX)

    total_rounds = 0
    total_sent = 0
    round_no = 0

    if METRICS_FILE:
        _metric_append({"ts_ms": _utc_ms(), "event": "first_round_start", "regist_no": REGIST_NO, "round": 1})

    # 첫 라운드: 전체 발송
    sent = _send_all_controllers(jitter_sec=jitter)
    round_no = 1
    total_rounds = 1
    total_sent = sent
    if METRICS_FILE:
        _metric_append({"ts_ms": _utc_ms(), "event": "round_sent", "regist_no": REGIST_NO, "sent": sent, "total": total, "round": round_no})
    logging.info(f"[Farm] {REGIST_NO}: first round sent {sent}/{total} STATE messages")
    _maybe_log_ack_stats(force=True)

    interval_sec = max(1, int(UPLINK_INTERVAL))
    next_send_ts = (int(time.time()) // interval_sec + 1) * interval_sec
    try:
        while True:
            wait = next_send_ts - time.time()
            if wait > 0:
                _sleep_with_retry(wait)
            sent = _send_all_controllers(jitter_sec=0)
            round_no += 1
            total_rounds += 1
            total_sent += sent
            if METRICS_FILE:
                _metric_append({"ts_ms": _utc_ms(), "event": "round_sent", "regist_no": REGIST_NO, "sent": sent, "total": total, "round": round_no})
            logging.info(f"[Farm] {REGIST_NO}: round sent {sent}/{total} STATE messages")
            _maybe_log_ack_stats(force=True)
            next_send_ts += interval_sec
    except KeyboardInterrupt:
        logging.info("[Farm] Stopped by user")
    finally:
        if METRICS_FILE:
            _metric_append({"ts_ms": _utc_ms(), "event": "farm_stop", "regist_no": REGIST_NO, "total_rounds": total_rounds, "total_sent": total_sent})
        if mqtt_client:
            mqtt_client.disconnect()
    sys.exit(0)


if __name__ == "__main__":
    main()
