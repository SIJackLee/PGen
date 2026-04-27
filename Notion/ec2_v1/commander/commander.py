# commander.py
# ============================================================
# 역할: DB 명령을 MQTT DOWNLINK로 직접 장비에 전송. STATE_ACK 수신 후 cmd:ack → DB 반영.
# 입력: Supabase room_command_queue_v3 (RPC claim_new_commands), MQTT STATE_ACK
# 출력: MQTT DOWNLINK (/KEY12/{key12}/DOWNLINK), Redis cmd:ack, DB 상태 업데이트 (ACKED/FAILED)
# 환경변수: MQTT_*, REDIS_*, SUPABASE_*, SCAN_INTERVAL, ACK_INTERVAL
# ============================================================

from __future__ import annotations

import os
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except ImportError:
    pass

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

try:
    import redis
except ImportError:
    redis = None

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

# protocol 및 supabase_rest 경로
_COMMANDER_DIR = os.path.dirname(os.path.abspath(__file__))
_EC2_ROOT = os.path.dirname(_COMMANDER_DIR)
sys.path.insert(0, _EC2_ROOT)

from protocol.binary_protocol import (
    pack_downlink,
    unpack_state_ack,
    decode_b64_payload,
    encode_b64_payload,
    STATE_ACK_OK,
)

try:
    from supabase_rest import SupabaseCommandDB
except ImportError:
    SupabaseCommandDB = None

# ============================================================
# 상수
# ============================================================

TOPIC_PREFIX = "/KEY12"
STATE_ACK_SUFFIX = "/STATE_ACK"
DOWNLINK_SUFFIX = "/DOWNLINK"

DEFAULT_MQTT_HOST = "127.0.0.1"
DEFAULT_MQTT_PORT = 1883
DEFAULT_REDIS_HOST = "127.0.0.1"
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DB = 0
DEFAULT_QUEUE_CMD_ACK = "cmd:ack"
DEFAULT_SCAN_INTERVAL = 5.0
DEFAULT_ACK_INTERVAL = 1.0
DEFAULT_SCAN_LIMIT = 100


# ============================================================
# DB Mock
# ============================================================

class DBMock:
    """Supabase 미설정 시 테스트용."""

    def __init__(self, _db_url: str = ""):
        logging.warning("[DB] Using MOCK DB (Supabase not configured)")

    def scan_and_claim_commands(self, limit: int = 100) -> List[Dict[str, Any]]:
        return []

    def rollback_to_new(self, cmd_id: str) -> None:
        pass

    def update_ack(
        self,
        cmd_id: str,
        result: str,
        detail: Any,
        ack_ts: Optional[int],
        last_error: Optional[str] = None,
    ) -> None:
        logging.debug(f"[DB] Mock update_ack: cmd_id={cmd_id}, result={result}")


# ============================================================
# Commander 클래스
# ============================================================

class Commander:
    """
    DB 명령 → MQTT DOWNLINK 직접 발행. STATE_ACK 구독 → cmd:ack → DB 반영.
    Redis cmd:pending 미사용.
    """

    def __init__(
        self,
        mqtt_host: str,
        mqtt_port: int,
        mqtt_username: Optional[str],
        mqtt_password: Optional[str],
        mqtt_client_id: str,
        redis_host: str,
        redis_port: int,
        redis_db: int,
        redis_password: Optional[str],
        queue_cmd_ack: str,
        db: Any,
        scan_interval: float,
        ack_interval: float,
        scan_limit: int = DEFAULT_SCAN_LIMIT,
    ):
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.mqtt_username = mqtt_username
        self.mqtt_password = mqtt_password
        self.mqtt_client_id = mqtt_client_id
        self.queue_cmd_ack = queue_cmd_ack
        self.db = db
        self.scan_interval = scan_interval
        self.ack_interval = ack_interval
        self.scan_limit = scan_limit

        self.r = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            socket_timeout=2,
            socket_connect_timeout=2,
            health_check_interval=30,
        )
        try:
            self.r.ping()
            logging.info(f"[Redis] Connected: {redis_host}:{redis_port} db={redis_db}")
        except Exception as e:
            logging.error(f"[Redis] Connection failed: {e}")
            raise

        self.client = mqtt.Client(client_id=mqtt_client_id, clean_session=True)
        if mqtt_username:
            self.client.username_pw_set(mqtt_username, mqtt_password or "")
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

        logging.info("[Commander] Initialized")

    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Dict[str, Any], rc: int):
        if rc == 0:
            logging.info(f"[MQTT] Connected to {self.mqtt_host}:{self.mqtt_port}")
            client.subscribe(f"{TOPIC_PREFIX}/+{STATE_ACK_SUFFIX}", qos=1)
            logging.info(f"[MQTT] Subscribed to {TOPIC_PREFIX}/+{STATE_ACK_SUFFIX}")
        else:
            logging.error(f"[MQTT] Connection failed, rc={rc}")

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        topic = msg.topic if isinstance(msg.topic, str) else str(msg.topic)
        if STATE_ACK_SUFFIX not in topic:
            return
        try:
            binary = decode_b64_payload(msg.payload)
            parsed = unpack_state_ack(binary)
            key12 = parsed.get("key12", "")
            cmd_acks = parsed.get("cmd_acks", [])
            for ca in cmd_acks:
                ack_obj = {
                    "kind": "ACK_CMD",
                    "cmd_id": ca.get("cmd_id"),
                    "key12": key12,
                    "result": ca.get("result", "UNKNOWN"),
                    "detail": ca.get("detail", []),
                    "ack_ts": ca.get("ack_ts"),
                    "err": None,
                }
                ack_json = json.dumps(ack_obj, ensure_ascii=False)
                self.r.rpush(self.queue_cmd_ack, ack_json)
            if cmd_acks:
                logging.info(f"[STATE_ACK] Received: key12={key12}, {len(cmd_acks)} cmd_acks")
        except Exception as e:
            logging.error(f"[STATE_ACK] Handling error: {e}", exc_info=True)

    @staticmethod
    def _normalize_cmd(cmd_id: str, cmd_json_str: str) -> Dict[str, Any]:
        """DB cmd_json을 pack_downlink용 cmds[] 한 항목으로 정규화."""
        try:
            body = json.loads(cmd_json_str) if cmd_json_str else {}
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}
        return {
            "cmd_id": str(cmd_id),
            "ttl_sec": int(body.get("ttl_sec", 0)) & 0xFFFF,
            "actions": body.get("actions", []),
        }

    def _publish_downlink(self, key12: str, cmd_id: str, cmds: List[Dict[str, Any]]) -> bool:
        """DOWNLINK 1건 MQTT 발행. 실패 시 False."""
        received_at = int(time.time())
        rx_id = f"cmd:{cmd_id}"
        try:
            binary = pack_downlink(
                key12=key12,
                rx_id=rx_id,
                received_at=received_at,
                result=STATE_ACK_OK,
                cmds=cmds,
            )
            payload = encode_b64_payload(binary)
            topic = f"{TOPIC_PREFIX}/{key12}{DOWNLINK_SUFFIX}"
            self.client.publish(topic, payload, qos=1, retain=False)
            logging.info(f"[DOWNLINK] Published: key12={key12}, cmd_id={cmd_id}")
            return True
        except Exception as e:
            logging.error(f"[DOWNLINK] Publish error key12={key12} cmd_id={cmd_id}: {e}")
            return False

    def scan_and_publish_commands(self):
        """DB claim → DOWNLINK MQTT 직접 발행. Redis cmd:pending 미사용."""
        try:
            commands = self.db.scan_and_claim_commands(self.scan_limit)
            if not commands:
                logging.debug("[Commander] No commands claimed")
                return

            logging.info(f"[Commander] Claimed {len(commands)} command(s)")

            for cmd in commands:
                cmd_id = cmd.get("cmd_id")
                key12 = cmd.get("key12")
                cmd_json_str = cmd.get("cmd_json")

                if not cmd_id or not key12:
                    logging.warning(f"[Commander] Invalid command: {cmd}")
                    continue

                key12 = str(key12).strip()
                if len(key12) != 12:
                    logging.warning(f"[Commander] Invalid key12 length: {key12}")
                    continue

                normalized = self._normalize_cmd(str(cmd_id), cmd_json_str or "{}")
                ok = self._publish_downlink(key12, str(cmd_id), [normalized])
                if not ok:
                    self.db.rollback_to_new(str(cmd_id))

        except Exception as e:
            logging.error(f"[Commander] Scan/publish error: {e}", exc_info=True)

    def process_ack_commands(self):
        """Redis cmd:ack LPOP → DB update_ack."""
        try:
            batch_size = 100
            acks = []
            for _ in range(batch_size):
                ack_json = self.r.lpop(self.queue_cmd_ack)
                if ack_json is None:
                    break
                acks.append(ack_json)

            if not acks:
                logging.debug("[Commander] No ACKs")
                return

            logging.info(f"[Commander] Processing {len(acks)} ACK(s)")

            for ack_json in acks:
                try:
                    raw = ack_json.decode("utf-8") if isinstance(ack_json, bytes) else ack_json
                    ack = json.loads(raw)
                    cmd_id = ack.get("cmd_id")
                    result = ack.get("result", "UNKNOWN")
                    detail = ack.get("detail", [])
                    ack_ts = ack.get("ack_ts")
                    err = ack.get("err")
                    if not cmd_id:
                        logging.warning(f"[Commander] Invalid ACK: {ack}")
                        continue
                    self.db.update_ack(
                        cmd_id=cmd_id,
                        result=result,
                        detail=detail,
                        ack_ts=ack_ts,
                        last_error=err,
                    )
                    logging.info(f"[Commander] ACK applied: cmd_id={cmd_id}, result={result}")
                except Exception as e:
                    logging.error(f"[Commander] ACK processing error: {e}", exc_info=True)

        except Exception as e:
            logging.error(f"[Commander] ACK error: {e}", exc_info=True)

    def run(self):
        logging.info("[Commander] Running. Press Ctrl+C to stop.")
        try:
            self.client.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
        except Exception as e:
            logging.error(f"[MQTT] Connection error: {e}")
            raise

        self.client.loop_start()
        last_scan_time = 0
        last_ack_time = 0
        try:
            while True:
                now = time.time()
                if now - last_scan_time >= self.scan_interval:
                    self.scan_and_publish_commands()
                    last_scan_time = now
                if now - last_ack_time >= self.ack_interval:
                    self.process_ack_commands()
                    last_ack_time = now
                time.sleep(0.1)
        except KeyboardInterrupt:
            logging.info("[Commander] Stopped by user.")
        finally:
            self.client.loop_stop()
            self.client.disconnect()


# ============================================================
# Main
# ============================================================

def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Commander: DB room_command_queue_v3 → MQTT DOWNLINK direct, STATE_ACK → cmd:ack → DB"
    )
    parser.add_argument("--mqtt-host", default=os.getenv("MQTT_HOST", DEFAULT_MQTT_HOST), help="MQTT broker host")
    parser.add_argument("--mqtt-port", type=int, default=int(os.getenv("MQTT_PORT", DEFAULT_MQTT_PORT)), help="MQTT broker port")
    parser.add_argument("--mqtt-username", default=os.getenv("MQTT_USERNAME"), help="MQTT username")
    parser.add_argument("--mqtt-password", default=os.getenv("MQTT_PASSWORD"), help="MQTT password")
    parser.add_argument("--mqtt-client-id", default=os.getenv("MQTT_CLIENT_ID", "commander_v1"), help="MQTT client id")

    parser.add_argument("--redis-host", default=os.getenv("REDIS_HOST", DEFAULT_REDIS_HOST), help="Redis host")
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", DEFAULT_REDIS_PORT)), help="Redis port")
    parser.add_argument("--redis-db", type=int, default=int(os.getenv("REDIS_DB", DEFAULT_REDIS_DB)), help="Redis DB")
    parser.add_argument("--redis-password", default=os.getenv("REDIS_PASSWORD"), help="Redis password")
    parser.add_argument("--queue-cmd-ack", default=os.getenv("QUEUE_CMD_ACK", DEFAULT_QUEUE_CMD_ACK), help="Redis cmd:ack queue")

    parser.add_argument("--scan-interval", type=float, default=float(os.getenv("SCAN_INTERVAL", DEFAULT_SCAN_INTERVAL)), help="DB scan interval (sec)")
    parser.add_argument("--ack-interval", type=float, default=float(os.getenv("ACK_INTERVAL", DEFAULT_ACK_INTERVAL)), help="ACK processing interval (sec)")
    parser.add_argument("--scan-limit", type=int, default=DEFAULT_SCAN_LIMIT, help="Max NEW commands per scan")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"), help="Logging level")

    args = parser.parse_args(argv)
    setup_logging(args.log_level)

    if redis is None:
        logging.error("redis-py not installed. pip install redis")
        return 2
    if mqtt is None:
        logging.error("paho-mqtt not installed. pip install paho-mqtt")
        return 2

    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    if supabase_url and supabase_key and SupabaseCommandDB is not None:
        db = SupabaseCommandDB(base_url=supabase_url, service_role_key=supabase_key)
        logging.info("[Commander] Using Supabase REST (room_command_queue_v3)")
    else:
        if not supabase_url or not supabase_key:
            logging.warning("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set; using DBMock (no-op)")
        else:
            logging.warning("supabase_rest not available; using DBMock")
        db = DBMock()

    commander = Commander(
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_username=args.mqtt_username,
        mqtt_password=args.mqtt_password,
        mqtt_client_id=args.mqtt_client_id,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        redis_password=args.redis_password,
        queue_cmd_ack=args.queue_cmd_ack,
        db=db,
        scan_interval=args.scan_interval,
        ack_interval=args.ack_interval,
        scan_limit=args.scan_limit,
    )
    commander.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
