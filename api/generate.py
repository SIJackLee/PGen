# api/generate.py
# ============================================================
# 성일기전 프로토콜 자동생성기 — Vercel Python Serverless Function
#
# POST /api/generate
# → CONNECT HEX, PUBLISH HEX, Topic, Payload, STATE Binary, Base64를 계산해 반환
# 실제 MQTT 송신은 수행하지 않는다.
#
# protocol/ 폴더에 의존하지 않는 자체 완결 구현.
# ============================================================

from __future__ import annotations

import base64
import json
import struct
from http.server import BaseHTTPRequestHandler
from typing import Any

# ============================================================
# 상수
# ============================================================

NOT_INSTALLED = 131071   # 0x1FFFF — 물리적으로 장비 없음
NO_DATA       = 131070   # 0x1FFFE — 장비는 있으나 값 미수신

SPECIES_MAP: dict[str, int] = {
    "W": 0, "D": 1, "P": 2, "H": 3,
    "T": 4, "R": 5, "I": 6, "B": 7,
}

EQ_TYPE_MAP: dict[str, int] = {
    "ES": 0, "EC": 1, "PC": 2, "BI": 3,
    "AF": 4, "SF": 5, "VI": 6,
}

# ============================================================
# 내부 유틸리티
# ============================================================

def _mqtt_string(s: str) -> bytes:
    """MQTT UTF-8 문자열 인코딩: 2B big-endian length + UTF-8 bytes"""
    encoded = s.encode("utf-8")
    return struct.pack(">H", len(encoded)) + encoded


def _encode_remaining_length(n: int) -> bytes:
    """MQTT variable-length encoding (Remaining Length)"""
    buf = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n > 0:
            byte |= 0x80
        buf.append(byte)
        if n == 0:
            break
    return bytes(buf)


def _parse_eq_code(eq_code: str) -> tuple[int, int]:
    """
    "ES01" → (type3=0, no_raw=0)
    접두사 2자 + 숫자 부분
    """
    prefix = eq_code[:2].upper()
    if prefix not in EQ_TYPE_MAP:
        raise ValueError(f"알 수 없는 장비 접두사: {prefix!r}")
    type3 = EQ_TYPE_MAP[prefix]
    try:
        no_raw = int(eq_code[2:]) - 1
    except ValueError:
        raise ValueError(f"장비 코드 번호 파싱 실패: {eq_code!r}")
    if no_raw < 0 or no_raw > 15:
        raise ValueError(f"장비 번호 범위 초과 (1~16): {eq_code!r}")
    return type3, no_raw


def _normalize_block(block: Any) -> dict:
    """
    blocks 입력을 내부 dict 형태로 정규화한다.
    - 단순형: ["ES01", 253] 또는 ("ES01", 253)
    - dict형: {"eq_code":"ES01","values":[253],"value_set_len":1,"instance_count":1}
    """
    if isinstance(block, dict):
        return {
            "eq_code":        str(block["eq_code"]),
            "values":         list(block.get("values", [block.get("value", 0)])),
            "value_set_len":  int(block.get("value_set_len", 1)),
            "instance_count": int(block.get("instance_count", 1)),
        }
    if isinstance(block, (list, tuple)) and len(block) >= 2:
        return {
            "eq_code":        str(block[0]),
            "values":         [int(block[1])],
            "value_set_len":  1,
            "instance_count": 1,
        }
    raise ValueError(f"block 형식을 인식할 수 없습니다: {block!r}")


# ============================================================
# 공개 프로토콜 함수
# ============================================================

def build_key12(
    regist_no: str,
    stall_ty_code: int,
    stall_no: int,
    room_no: int,
) -> str:
    """12자리 ASCII 식별자를 생성한다."""
    if len(regist_no) != 6:
        raise ValueError(
            f"regist_no는 정확히 6자여야 합니다. 입력값: {regist_no!r} ({len(regist_no)}자)"
        )
    return f"{regist_no}{stall_ty_code:02d}{stall_no:02d}{room_no:02d}"


def build_key9(
    regist_no: str,
    species: str,
    stall_ty_code: int,
    stall_no: int,
    room_no: int,
) -> bytes:
    """Key9 (9바이트) 식별 정보를 생성한다."""
    if len(regist_no) != 6:
        raise ValueError(
            f"regist_no는 정확히 6자여야 합니다. 입력값: {regist_no!r} ({len(regist_no)}자)"
        )
    species_upper = species.upper()
    if species_upper not in SPECIES_MAP:
        raise ValueError(f"알 수 없는 축종 문자: {species!r}")
    species_bits = SPECIES_MAP[species_upper]
    stall_ty_norm = stall_ty_code - 1
    b6 = (species_bits << 5) | (stall_ty_norm << 1) | 0
    return regist_no.encode("ascii") + bytes([b6, stall_no & 0xFF, room_no & 0xFF])


def pack_time4(year: int, month: int, day: int, hour: int, minute: int) -> bytes:
    """측정 시각을 27비트로 압축해 4바이트 big-endian으로 반환한다."""
    yy = year % 100
    packed = (yy << 20) | ((month - 1) << 16) | ((day - 1) << 11) | (hour << 6) | minute
    return struct.pack(">I", packed)


def pack_val17(value: int) -> bytes:
    """17bit unsigned 값을 3바이트로 패킹한다."""
    value = int(value) & 0x1FFFF
    return bytes([(value >> 16) & 0x01, (value >> 8) & 0xFF, value & 0xFF])


def pack_block(
    eq_code: str,
    value_set_len: int,
    instance_count: int,
    values: list[int],
) -> bytes:
    """단일 Block 바이너리를 생성한다."""
    type3, no_raw = _parse_eq_code(eq_code)
    eqpmn = (type3 << 5) | (no_raw << 1) | 0
    buf = bytearray([eqpmn, value_set_len & 0xFF, instance_count & 0xFF])
    for v in values:
        buf.extend(pack_val17(v))
    return bytes(buf)


def build_state_binary(
    regist_no: str,
    species: str,
    stall_ty_code: int,
    stall_no: int,
    room_no: int,
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    blocks: list,
) -> bytes:
    """STATE 바이너리 페이로드 전체를 생성한다."""
    normalized = [_normalize_block(b) for b in blocks]
    block_count = len(normalized)
    block_header = (2 << 5) | (block_count << 1) | 0  # ver=2 고정

    buf = bytearray()
    buf.extend(build_key9(regist_no, species, stall_ty_code, stall_no, room_no))
    buf.extend(pack_time4(year, month, day, hour, minute))
    buf.append(block_header)
    for nb in normalized:
        buf.extend(pack_block(
            nb["eq_code"],
            nb["value_set_len"],
            nb["instance_count"],
            nb["values"],
        ))
    return bytes(buf)


def build_state_base64(state_binary: bytes) -> str:
    """
    바이너리 STATE 페이로드를 순수 base64 문자열로 반환한다.
    "B64:" 접두사는 붙이지 않는다.
    """
    return base64.b64encode(state_binary).decode("ascii")


def build_mqtt_payload(ref_seq: int, state_binary: bytes) -> str:
    """MQTT STATE 페이로드 문자열을 생성한다. → "SEQ:{ref_seq},B64:{base64}" """
    return f"SEQ:{ref_seq},B64:{build_state_base64(state_binary)}"


def build_mqtt_connect_hex(
    client_id: str,
    keep_alive: int = 60,
    username: str | None = None,
    password: str | None = None,
    clean_session: bool = True,
) -> str:
    """MQTT 3.1.1 CONNECT 패킷을 16진수 공백구분 문자열로 반환한다."""
    if password is not None and username is None:
        raise ValueError("username 없이 password만 설정할 수 없습니다.")

    username_flag = 1 if username is not None else 0
    password_flag = 1 if password is not None else 0
    connect_flags = (username_flag << 7) | (password_flag << 6) | (int(clean_session) << 1)

    variable_header = bytearray([
        0x00, 0x04, 0x4D, 0x51, 0x54, 0x54,
        0x04,
        connect_flags,
    ])
    variable_header.extend(struct.pack(">H", keep_alive))

    payload = bytearray(_mqtt_string(client_id))
    if username is not None:
        payload.extend(_mqtt_string(username))
    if password is not None:
        payload.extend(_mqtt_string(password))

    remaining = variable_header + payload
    packet = bytearray([0x10])
    packet.extend(_encode_remaining_length(len(remaining)))
    packet.extend(remaining)
    return packet.hex(" ").upper()


def build_mqtt_publish_hex(
    topic: str,
    payload_text: str,
    packet_id: int = 1,
    qos: int = 1,
    retain: bool = False,
    dup: bool = False,
) -> str:
    """MQTT 3.1.1 PUBLISH 패킷을 16진수 공백구분 문자열로 반환한다."""
    fixed_byte = 0x30 | (int(dup) << 3) | (qos << 1) | int(retain)

    variable_header = bytearray(_mqtt_string(topic))
    if qos > 0:
        variable_header.extend(struct.pack(">H", packet_id))

    payload_bytes = payload_text.encode("utf-8")
    remaining = bytes(variable_header) + payload_bytes
    packet = bytearray([fixed_byte])
    packet.extend(_encode_remaining_length(len(remaining)))
    packet.extend(remaining)
    return packet.hex(" ").upper()


# ============================================================
# 내부 생성 함수
# ============================================================

def _generate(body: dict) -> dict:
    """
    요청 body에서 파라미터를 추출해 모든 결과를 계산한다.
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

    key12     = build_key12(regist_no, stall_ty_code, stall_no, room_no)
    client_id = f"cm_{key12}"
    topic     = f"/KEY12/{key12}"

    state_bin    = build_state_binary(
        regist_no, species, stall_ty_code, stall_no, room_no,
        year, month, day, hour, minute, blocks,
    )
    state_hex    = state_bin.hex(" ").upper()
    state_base64 = build_state_base64(state_bin)
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

    return {
        "key12":            key12,
        "connect_hex":      connect_hex,
        "expected_connack": "20 02 00 00",
        "topic":            topic,
        "state_hex":        state_hex,
        "state_base64":     state_base64,
        "mqtt_payload":     mqtt_payload,
        "publish_hex":      publish_hex,
    }


# ============================================================
# Vercel Serverless Handler
# ============================================================

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
        pass
