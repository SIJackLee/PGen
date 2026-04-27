# receiver.py
# ============================================================
# 역할: MQTT Receiver (STATE 수신 -> Redis raw 적재 전용). 명령(DOWNLINK)·STATE_ACK는 Commander 담당.
# 입력: MQTT STATE (/KEY12/{key12})
# 출력: Redis raw 큐 (rx:v2:raw)
# 환경변수: MQTT_*, REDIS_*
# 프로토콜: 바이너리 (B64 인코딩)
#
# [중요] 구독: STATE(/KEY12/+) 만. DOWNLINK 형식(01 30) 페이로드 수신 시 rx:v2:raw 적재 스킵.
# ============================================================

from __future__ import annotations

import argparse
from dataclasses import dataclass
import base64
import logging
import os
import queue
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

# protocol 모듈 경로 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from protocol.binary_protocol import (
    VER,
    TYPE_DOWNLINK,
    UPLINK_ACK_OK,
    encode_b64_payload,
    pack_uplink_ack,
)

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

try:
    import redis
except Exception:
    redis = None


# ============================================================
# 환경변수 기본값
# ============================================================

DEFAULT_MQTT_HOST = "127.0.0.1"
DEFAULT_MQTT_PORT = 1883
DEFAULT_REDIS_HOST = "127.0.0.1"
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DB = 0
DEFAULT_QUEUE_RAW = "rx:v2:raw"
DEFAULT_QUEUE_MAX_SIZE = 100_000
DEFAULT_STATS_INTERVAL = 30.0
DEFAULT_WORKER_POLL_TIMEOUT = 0.2
DEFAULT_STATE_BATCH_SIZE = 100
DEFAULT_STATE_BATCH_TIMEOUT = 0.02
DEFAULT_STATE_WORKER_COUNT = 2
DEFAULT_MQTT_MAX_INFLIGHT = 20
SHUTDOWN_JOIN_TIMEOUT = 5.0
SHUTDOWN_STATS_JOIN_TIMEOUT = 2.0

# 토픽 형식
TOPIC_PREFIX = "/KEY12"
KEY12_LEN = 12
UPLINK_ACK_SUFFIX = "UPLINK_ACK"


# ============================================================
# Utilities
# ============================================================

@dataclass(frozen=True)
class ReceiverConfig:
    """Receiver 설정 (CLI/테스트에서 한 번에 전달)."""
    mqtt_host: str
    mqtt_port: int
    mqtt_username: Optional[str]
    mqtt_password: Optional[str]
    mqtt_client_id: str
    redis_host: str
    redis_port: int
    redis_db: int
    redis_password: Optional[str]
    queue_raw: str
    stats_interval_sec: float
    queue_max_size: int
    worker_poll_timeout: float
    state_batch_size: int
    state_batch_timeout: float
    state_worker_count: int
    mqtt_max_inflight: int


def parse_state_topic(topic: str) -> Optional[str]:
    """STATE 토픽 /KEY12/{key12} 파싱. 유효하면 key12 반환, 아니면 None."""
    if not topic.startswith(TOPIC_PREFIX + "/"):
        return None
    parts = topic.split("/", 3)  # 최대 3개 분할로 불필요한 분할 방지
    if len(parts) != 3:
        return None
    key12 = parts[2]
    return key12 if len(key12) == KEY12_LEN else None

def decode_n510_payload(mqtt_payload: bytes) -> bytes:
    """
    N510 custom mode payload 디코딩
    
    Expected:
      b"B64:<base64>"
    Tolerate:
      b"topic,qos,retain,B64:<base64>"
    """
    text = ""
    try:
        text = mqtt_payload.decode("utf-8", errors="strict").strip()
    except UnicodeDecodeError:
        text = ""

    if text:
        if "," in text and "B64:" in text:
            text = text.split(",")[-1].strip()
        if text.startswith("B64:"):
            b64 = text[4:]
        else:
            b64 = text
        try:
            return base64.b64decode(b64, validate=False)
        except Exception as e:
            raise ValueError(f"base64 decode failed: {e}")

    try:
        return base64.b64decode(mqtt_payload, validate=False)
    except Exception as e:
        raise ValueError(f"base64 decode failed (raw bytes): {e}")


def parse_ref_seq_from_state_payload(mqtt_payload: bytes) -> int:
    """
    STATE MQTT payload의 텍스트 헤더에서 ref_seq 추출.

    기대 형식:
      b"SEQ:<uint32>,B64:<...>"
    미존재/파싱 실패 시 0 반환.
    """
    try:
        text = mqtt_payload.decode("utf-8", errors="strict").strip()
    except UnicodeDecodeError:
        return 0
    if not text or "," not in text:
        return 0
    head = text.split(",", 1)[0].strip()
    if not head.startswith("SEQ:"):
        return 0
    raw = head[4:].strip()
    if not raw:
        return 0
    try:
        return int(raw) & 0xFFFFFFFF
    except Exception:
        return 0


# ============================================================
# Receiver 클래스
# ============================================================

class Receiver:
    """
    MQTT Receiver (수집 전용).
    STATE 수신 (/KEY12/{key12}) -> Redis raw 큐(rx:v2:raw) 적재.
    명령(DOWNLINK)·STATE_ACK는 Commander에서 처리.
    """
    
    def __init__(
        self,
        config: ReceiverConfig,
        redis_client: Optional[Any] = None,
        mqtt_client: Optional[Any] = None,
    ):
        c = config
        self.mqtt_host = c.mqtt_host
        self.mqtt_port = c.mqtt_port
        self.mqtt_username = c.mqtt_username
        self.mqtt_password = c.mqtt_password
        self.mqtt_client_id = c.mqtt_client_id
        self.redis_host = c.redis_host
        self.redis_port = c.redis_port
        self.redis_db = c.redis_db
        self.redis_password = c.redis_password
        self.queue_raw = c.queue_raw
        self._worker_poll_timeout = max(0.01, float(c.worker_poll_timeout))
        self._state_batch_size = max(1, int(c.state_batch_size))
        self._state_batch_timeout = max(0.001, float(c.state_batch_timeout))

        queue_max = max(1, int(c.queue_max_size))
        # STATE 처리 비동기: 콜백은 큐에만 넣고, 워커가 Redis만 처리 (블로킹 방지)
        self._state_queue: queue.Queue[Tuple[str, bytes, int]] = queue.Queue(maxsize=queue_max)
        self._worker_stop = threading.Event()
        state_worker_count = max(1, int(c.state_worker_count))
        self._state_workers: List[threading.Thread] = [
            threading.Thread(target=self._state_worker, daemon=True) for _ in range(state_worker_count)
        ]
        for w in self._state_workers:
            w.start()

        # 버스트 측정용 통계
        self._stats_state_received = 0
        self._stats_state_dropped = 0
        self._stats_redis_pushed = 0
        self._stats_lock = threading.Lock()
        self._stats_interval_sec = max(0.0, float(c.stats_interval_sec))
        if self._stats_interval_sec > 0:
            self._stats_logger = threading.Thread(target=self._stats_loop, daemon=True)
            self._stats_logger.start()

        if redis_client is not None:
            self.r = redis_client
        else:
            self.r = redis.Redis(
                host=c.redis_host,
                port=c.redis_port,
                db=c.redis_db,
                password=c.redis_password,
                socket_timeout=2,
                socket_connect_timeout=2,
                health_check_interval=30,
            )
            try:
                self.r.ping()
                logging.info(f"[Redis] Connected: {c.redis_host}:{c.redis_port} db={c.redis_db}")
            except Exception as e:
                logging.error(f"[Redis] Connection failed: {e}")
                raise

        if mqtt_client is not None:
            self.client = mqtt_client
        else:
            self.client = mqtt.Client(client_id=c.mqtt_client_id, clean_session=True)
            if c.mqtt_username:
                self.client.username_pw_set(c.mqtt_username, c.mqtt_password or "")
            max_inflight = max(1, int(c.mqtt_max_inflight))
            self.client.max_inflight_messages_set(max_inflight)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
    
    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Dict[str, Any], rc: int):
        if rc == 0:
            logging.info(f"[MQTT] Connected to {self.mqtt_host}:{self.mqtt_port}")
            client.subscribe(f"{TOPIC_PREFIX}/+", qos=1)  # STATE: /KEY12/{key12}
            logging.info(f"[MQTT] Subscribed to {TOPIC_PREFIX}/+ (STATE)")
        else:
            logging.error(f"[MQTT] Connection failed, rc={rc}")
    
    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        topic = msg.topic if isinstance(msg.topic, str) else str(msg.topic)
        try:
            key12 = parse_state_topic(topic)
            if key12 is not None:
                self._handle_state(topic, msg.payload, key12)
                return
            logging.debug(f"[MQTT] Unknown topic: {topic}")
        except Exception as e:
            logging.exception(f"[MQTT] Message handling error (topic={topic}): {e}")

    def _build_uplink_ack_topic(self, key12: str) -> str:
        return f"{TOPIC_PREFIX}/{key12}/{UPLINK_ACK_SUFFIX}"

    def _publish_uplink_ack(self, key12: str, ref_seq: int) -> None:
        """Redis raw 적재 성공 후 local-sim으로 UPLINK_ACK 발행."""
        ack_ts_ms = int(time.time() * 1000)
        ack_binary = pack_uplink_ack(
            key12=key12,
            ack_ts_ms=ack_ts_ms,
            result=UPLINK_ACK_OK,
            flags=0x01,  # bit0: redis_push_ok
            ref_seq=ref_seq,
        )
        ack_payload = encode_b64_payload(ack_binary)
        try:
            result = self.client.publish(
                self._build_uplink_ack_topic(key12),
                ack_payload,
                qos=1,
                retain=False,
            )
            result.wait_for_publish(timeout=1.0)
            rc = getattr(result, "rc", 0)
            if rc != 0:
                logging.warning(f"[ACK] publish rc={rc} key12={key12} ref_seq={ref_seq}")
            else:
                logging.info(f"[ACK] published key12={key12} ref_seq={ref_seq} ack_ts_ms={ack_ts_ms}")
        except Exception as e:
            logging.warning(f"[ACK] publish failed key12={key12} ref_seq={ref_seq}: {e}")

    def _state_worker(self) -> None:
        """워커: 배치로 STATE를 꺼내 Redis pipeline 적재 (버스트 대응)."""
        batch: List[Tuple[str, bytes, int]] = []
        first_wait = min(self._worker_poll_timeout, self._state_batch_timeout)
        while not self._worker_stop.is_set():
            batch.clear()
            try:
                item = self._state_queue.get(timeout=first_wait)
                batch.append(item)
            except queue.Empty:
                continue
            while len(batch) < self._state_batch_size:
                try:
                    item = self._state_queue.get_nowait()
                    batch.append(item)
                except queue.Empty:
                    break
            if not batch:
                continue
            try:
                pipe = self.r.pipeline()
                for _, bin_pl, _ in batch:
                    pipe.rpush(self.queue_raw, bin_pl)
                pipe.execute()
            except Exception as e:
                logging.error(f"[STATE] Redis pipeline error: {e}")
                for _ in batch:
                    self._state_queue.task_done()
                continue
            with self._stats_lock:
                self._stats_redis_pushed += len(batch)
            # Redis raw 적재 성공한 건에 대해서만 ACK 발행
            for key12, _, ref_seq in batch:
                self._publish_uplink_ack(key12, ref_seq)
            for _ in batch:
                self._state_queue.task_done()
    
    def _handle_state(self, topic: str, mqtt_payload: bytes, key12: Optional[str] = None):
        """STATE 수신: 검증·디코딩 후 워커 큐에만 넣고 즉시 반환 (블로킹 방지). key12는 _on_message에서 이미 파싱된 경우 전달(재파싱 방지)."""
        if key12 is None:
            key12 = parse_state_topic(topic)
            if key12 is None:
                logging.debug(f"[STATE] Invalid topic (expected {TOPIC_PREFIX}/{{key12}}): {topic}")
                return
        
        try:
            bin_payload = decode_n510_payload(mqtt_payload)
        except Exception as e:
            logging.error(f"[STATE] Decode error (topic={topic}): {e}")
            return

        # 가드: DOWNLINK 형식(01 30)은 rx:v2:raw에 적재하지 않음 (무한루프·Assembler 파싱 실패 방지)
        if len(bin_payload) >= 2 and bin_payload[0] == VER and bin_payload[1] == TYPE_DOWNLINK:
            logging.debug(f"[STATE] Ignoring DOWNLINK-format payload (topic={topic} len={len(bin_payload)})")
            return

        with self._stats_lock:
            self._stats_state_received += 1
        try:
            ref_seq = parse_ref_seq_from_state_payload(mqtt_payload)
            self._state_queue.put_nowait((key12, bin_payload, ref_seq))
        except queue.Full:
            with self._stats_lock:
                self._stats_state_dropped += 1
            logging.error(f"[STATE] Worker queue full, dropping message topic={topic}")
    
    def _collect_stats(self) -> Dict[str, Any]:
        """큐 크기·Redis 길이·카운터 수집 (통계 로그용)."""
        with self._stats_lock:
            recv = self._stats_state_received
            drop = self._stats_state_dropped
            pushed = self._stats_redis_pushed
        q_state = self._state_queue.qsize()
        try:
            redis_len = int(self.r.llen(self.queue_raw))
        except Exception:
            redis_len = -1
        return {
            "state_q": q_state,
            "redis_raw": redis_len,
            "received": recv,
            "dropped": drop,
            "redis_pushed": pushed,
        }

    def _stats_loop(self) -> None:
        """주기적으로 큐 크기·통계 로그."""
        while not self._worker_stop.is_set():
            time.sleep(self._stats_interval_sec)
            if self._worker_stop.is_set():
                break
            s = self._collect_stats()
            logging.info(
                f"[STATS] state_q={s['state_q']} redis_raw={s['redis_raw']} "
                f"received={s['received']} dropped={s['dropped']} redis_pushed={s['redis_pushed']}"
            )

    def log_stats_once(self) -> None:
        """현재 큐 크기·통계를 한 줄 로그 (종료 시 또는 수동 호출용)."""
        s = self._collect_stats()
        logging.info(
            f"[STATS FINAL] state_q={s['state_q']} redis_raw={s['redis_raw']} "
            f"received={s['received']} dropped={s['dropped']} redis_pushed={s['redis_pushed']}"
        )

    def connect(self):
        try:
            self.client.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
        except Exception as e:
            logging.error(f"[MQTT] Connection error: {e}")
            raise
    
    def run(self):
        logging.info("[Receiver] Running. Press Ctrl+C to stop.")
        try:
            self.client.loop_forever(retry_first_connection=True)
        except KeyboardInterrupt:
            logging.info("[Receiver] Stopped by user.")
        finally:
            self._worker_stop.set()
            for w in self._state_workers:
                w.join(timeout=SHUTDOWN_JOIN_TIMEOUT)
            if self._stats_interval_sec > 0 and hasattr(self, "_stats_logger"):
                self._stats_logger.join(timeout=SHUTDOWN_STATS_JOIN_TIMEOUT)


# ============================================================
# Main
# ============================================================

def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="MQTT receiver -> Redis raw queue (STATE 수집 전용)")

    parser.add_argument("--mqtt-host", default=DEFAULT_MQTT_HOST, help="MQTT broker host")
    parser.add_argument("--mqtt-port", type=int, default=DEFAULT_MQTT_PORT, help="MQTT broker port")
    parser.add_argument("--mqtt-username", default=None, help="MQTT username")
    parser.add_argument("--mqtt-password", default=None, help="MQTT password")
    parser.add_argument("--mqtt-client-id", default="receiver_v2", help="MQTT client id")

    parser.add_argument("--redis-host", default=DEFAULT_REDIS_HOST, help="Redis host")
    parser.add_argument("--redis-port", type=int, default=DEFAULT_REDIS_PORT, help="Redis port")
    parser.add_argument("--redis-db", type=int, default=DEFAULT_REDIS_DB, help="Redis DB")
    parser.add_argument("--redis-password", default=None, help="Redis password")
    parser.add_argument("--queue-raw", default=DEFAULT_QUEUE_RAW, help="Redis raw queue")

    parser.add_argument("--stats-interval", type=float, default=DEFAULT_STATS_INTERVAL, help="Stats log interval (sec). 0=off. 기본 30")
    parser.add_argument("--queue-max-size", type=int, default=DEFAULT_QUEUE_MAX_SIZE, help="Max size of state/downlink queues")
    parser.add_argument("--worker-poll-timeout", type=float, default=DEFAULT_WORKER_POLL_TIMEOUT, help="Worker queue get timeout (sec). 버스트 시 0.1~0.2")
    parser.add_argument("--state-batch-size", type=int, default=DEFAULT_STATE_BATCH_SIZE, help="STATE Redis pipeline batch size")
    parser.add_argument("--state-batch-timeout", type=float, default=DEFAULT_STATE_BATCH_TIMEOUT, help="STATE batch collect timeout (sec)")
    parser.add_argument("--state-worker-count", type=int, default=DEFAULT_STATE_WORKER_COUNT, help="Number of state worker threads")
    parser.add_argument("--mqtt-max-inflight", type=int, default=DEFAULT_MQTT_MAX_INFLIGHT, help="MQTT max in-flight messages (QoS>0)")
    parser.add_argument("--log-level", default="INFO", help="Logging level")

    args = parser.parse_args(argv)
    setup_logging(args.log_level)

    if mqtt is None:
        logging.error("paho-mqtt not installed. pip install paho-mqtt")
        return 2
    if redis is None:
        logging.error("redis-py not installed. pip install redis")
        return 2

    config = ReceiverConfig(
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_username=args.mqtt_username,
        mqtt_password=args.mqtt_password,
        mqtt_client_id=args.mqtt_client_id,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        redis_password=args.redis_password,
        queue_raw=args.queue_raw,
        stats_interval_sec=args.stats_interval,
        queue_max_size=args.queue_max_size,
        worker_poll_timeout=args.worker_poll_timeout,
        state_batch_size=args.state_batch_size,
        state_batch_timeout=args.state_batch_timeout,
        state_worker_count=args.state_worker_count,
        mqtt_max_inflight=args.mqtt_max_inflight,
    )
    receiver = Receiver(config=config)

    receiver.connect()
    try:
        receiver.run()
    finally:
        receiver.log_stats_once()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
