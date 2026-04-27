# api/generate.py
# ============================================================
# 성일기전 프로토콜 자동생성기 — Vercel Python Serverless Function
#
# POST /api/generate
# → CONNECT HEX, PUBLISH HEX, Topic, Payload, STATE Binary, Base64를 계산해 반환
# 실제 MQTT 송신은 수행하지 않는다.
# ============================================================

from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler

# Vercel 런타임 환경에서 프로젝트 루트를 경로에 추가
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from protocol.protocol_generator import (
    build_key12,
    build_key9,
    build_mqtt_connect_hex,
    build_mqtt_payload,
    build_mqtt_publish_hex,
    build_state_base64,
    build_state_binary,
    pack_time4,
)


def _generate(body: dict) -> dict:
    """
    요청 body에서 파라미터를 추출해 모든 결과를 계산한다.

    필수 파라미터:
        regist_no, species, stall_ty_code, stall_no, room_no
        ref_seq, year, month, day, hour, minute, blocks

    선택 파라미터:
        keep_alive (기본 60), username, password, clean_session (기본 true)
    """
    regist_no     = str(body["regist_no"])
    species       = str(body["species"])
    stall_ty_code = int(body["stall_ty_code"])
    stall_no      = int(body["stall_no"])
    room_no       = int(body["room_no"])
    ref_seq       = int(body["ref_seq"])
    year          = int(body["year"])
    month         = int(body["month"])
    day           = int(body["day"])
    hour          = int(body["hour"])
    minute        = int(body["minute"])
    blocks        = list(body["blocks"])

    keep_alive    = int(body.get("keep_alive", 60))
    username      = body.get("username") or None
    password      = body.get("password") or None
    clean_session = bool(body.get("clean_session", True))

    key12 = build_key12(regist_no, stall_ty_code, stall_no, room_no)
    client_id = f"cm_{key12}"
    topic = f"/KEY12/{key12}"

    state_bin  = build_state_binary(
        regist_no, species, stall_ty_code, stall_no, room_no,
        year, month, day, hour, minute, blocks,
    )
    state_hex    = state_bin.hex(" ").upper()
    state_base64 = build_state_base64(state_bin)   # 순수 base64, "B64:" 없음
    mqtt_payload = build_mqtt_payload(ref_seq, state_bin)

    connect_hex = build_mqtt_connect_hex(
        client_id=client_id,
        keep_alive=keep_alive,
        username=username,
        password=password,
        clean_session=clean_session,
    )

    publish_hex = build_mqtt_publish_hex(
        topic=topic,
        payload_text=mqtt_payload,
        packet_id=1,
        qos=1,
    )

    # Expected CONNACK: 세션 없음(clean_session=True) → 20 02 00 00
    # clean_session=False일 때 서버가 세션 재개 시 20 02 01 00 을 반환할 수 있으나
    # 클라이언트가 예측할 수 있는 값은 clean_session=True일 때 20 02 00 00 이다.
    expected_connack = "20 02 00 00"

    return {
        "key12":            key12,
        "connect_hex":      connect_hex,
        "expected_connack": expected_connack,
        "topic":            topic,
        "state_hex":        state_hex,
        "state_base64":     state_base64,
        "mqtt_payload":     mqtt_payload,
        "publish_hex":      publish_hex,
    }


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            raw_body = self.rfile.read(content_length)
            body = json.loads(raw_body)
        except Exception as e:
            self._send_json({"error": f"요청 파싱 실패: {e}"}, status=400)
            return

        try:
            result = _generate(body)
        except (KeyError, TypeError) as e:
            self._send_json({"error": f"필수 파라미터 누락 또는 타입 오류: {e}"}, status=422)
            return
        except ValueError as e:
            self._send_json({"error": str(e)}, status=400)
            return
        except Exception as e:
            self._send_json({"error": f"내부 오류: {e}"}, status=500)
            return

        self._send_json(result, status=200)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors_headers()
        self.end_headers()

    def _cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _send_json(self, data: dict, status: int = 200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # 불필요한 콘솔 로그 억제
