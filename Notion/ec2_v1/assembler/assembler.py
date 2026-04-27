#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""assembler.py

Redis Queue Consumer -> v2 Payload Parser -> Redis Parsed Queue Writer

요구사항:
- Redis "rx:v2:raw" 큐에서 바이너리 payload를 배치로 소비
- v2 데이터폼 엄격 파싱 및 검증
- 파싱된 데이터를 JSON으로 직렬화하여 Redis "rx:v2:parsed" 큐에 저장
- 실패 시 DLQ 전송
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

try:
    import redis
except ImportError:
    redis = None

try:
    import orjson
    ORJSON_AVAILABLE = True
except ImportError:
    ORJSON_AVAILABLE = False

# ============================================================
# Constants
# ============================================================

KST = ZoneInfo("Asia/Seoul")

SPECIES_MAP = {"W": 0, "D": 1, "P": 2, "H": 3, "T": 4, "R": 5, "I": 6, "B": 7}
SPECIES_MAP_REV = {v: k for k, v in SPECIES_MAP.items()}
SPECIES_CODE = ["W", "D", "P", "H", "T", "R", "I", "B"]  # species(0~7) -> species_code 매핑

EQ_TYPE_MAP = {"ES": 0, "EC": 1, "PC": 2, "BI": 3, "AF": 4, "SF": 5, "VI": 6}
EQ_TYPE_MAP_REV = {v: k for k, v in EQ_TYPE_MAP.items()}

# 실장비 기준: 센서 2개 (온도, 습도) + 모터 3개 (EC01, EC02, EC03)
# EC02/EC03 둘 다 존재하며, 한쪽이 0(OFF)일 수 있음 (운용 정책: 입기/배기)
SENSOR_EQS = ["ES01", "ES02"]
EXPECTED_BLOCK_COUNT = 5

# 센티널 (assembler에서 NULL 처리 금지, saver에서만 변환)
NOT_INSTALLED = 131071
NO_DATA = 131070

# ============================================================
# Parsing Functions
# ============================================================


def unpack_key9(key9: bytes) -> Dict[str, Any]:
    """Key9(9B) 파싱"""
    if len(key9) != 9:
        raise ValueError(f"key9 must be 9 bytes, got {len(key9)}")
    regist_no = key9[0:6].decode("ascii", errors="replace")
    b6 = key9[6]
    species_bits = (b6 >> 5) & 0x7
    stall_type_norm = (b6 >> 1) & 0xF
    stall_no = key9[7]
    room_no = key9[8]

    return {
        "regist_no": regist_no,
        "species_bits": species_bits,
        "species_char": SPECIES_MAP_REV.get(species_bits, "?"),
        "stall_type_norm": stall_type_norm,
        "stall_ty_code": stall_type_norm + 1,
        "stall_no": stall_no,
        "room_no": room_no,
    }


def unpack_time4(b: bytes) -> datetime:
    """Time4(4B) 파싱 -> Asia/Seoul timezone-aware datetime"""
    if len(b) != 4:
        raise ValueError(f"time4 must be 4 bytes, got {len(b)}")
    packed = int.from_bytes(b, "big", signed=False)
    yy = (packed >> 20) & 0x7F
    mm_st = (packed >> 16) & 0x0F
    dd_st = (packed >> 11) & 0x1F
    hh = (packed >> 6) & 0x1F
    mm = packed & 0x3F

    year = 2000 + yy
    month = mm_st + 1
    day = dd_st + 1

    return datetime(year, month, day, hh, mm, 0, tzinfo=KST)


def unpack_block_header(b: int) -> Dict[str, Any]:
    """BlockHeader(1B) 파싱"""
    return {
        "ver": (b >> 5) & 0x7,
        "block_count": (b >> 1) & 0xF,
        "spare": b & 0x1,
    }


def unpack_eq(b: int) -> str:
    """Eq(1B) -> eq_code 문자열"""
    type3 = (b >> 5) & 0x7
    no_raw = (b >> 1) & 0xF
    prefix = EQ_TYPE_MAP_REV.get(type3, "??")
    no_dec = no_raw + 1
    return f"{prefix}{no_dec:02d}"


def unpack_val17_u(b3: bytes) -> int:
    """17bit unsigned 값 디코딩"""
    if len(b3) != 3:
        raise ValueError(f"val17 must be 3 bytes, got {len(b3)}")
    msb = b3[0] & 1
    low16 = (b3[1] << 8) | b3[2]
    return (msb << 16) | low16


def parse_payload(payload: bytes) -> Dict[str, Any]:
    """전체 payload 파싱"""
    if len(payload) < 14:
        raise ValueError(f"payload too short: {len(payload)} bytes")

    pos = 0

    # Key9
    key9 = payload[pos : pos + 9]
    pos += 9
    key_info = unpack_key9(key9)

    # Time4
    time4 = payload[pos : pos + 4]
    pos += 4
    measure_ts = unpack_time4(time4)

    # BlockHeader
    bh_raw = payload[pos]
    pos += 1
    bh_info = unpack_block_header(bh_raw)

    # Blocks
    blocks: List[Dict[str, Any]] = []
    for _ in range(bh_info["block_count"]):
        if pos + 3 > len(payload):
            raise ValueError("payload truncated while reading block header")
        eqb = payload[pos]
        vlen = payload[pos + 1]
        inst = payload[pos + 2]
        pos += 3

        eq_code = unpack_eq(eqb)
        nvals = vlen * inst
        needed = nvals * 3
        if pos + needed > len(payload):
            raise ValueError(f"payload truncated: need {needed} bytes for {eq_code}")
        values = [unpack_val17_u(payload[pos + i : pos + i + 3]) for i in range(0, needed, 3)]
        pos += needed

        blocks.append({"eq_code": eq_code, "vlen": vlen, "inst": inst, "values": values})

    if pos != len(payload):
        raise ValueError(f"payload has trailing bytes: {len(payload) - pos} bytes")

    return {
        "key": key_info,
        "measure_ts": measure_ts,
        "block_header": bh_info,
        "blocks": blocks,
    }


# ============================================================
# Validation Functions
# ============================================================


def validate_payload(parsed: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """엄격한 검증"""
    bh = parsed["block_header"]
    blocks = parsed["blocks"]

    # Block count 검증
    if bh["block_count"] != EXPECTED_BLOCK_COUNT:
        return False, f"block_count={bh['block_count']}, expected={EXPECTED_BLOCK_COUNT}"

    # 센서/모터 블록 검증
    sensor_blocks = {}
    motor_blocks = {}
    ec02_found = False
    ec03_found = False

    for block in blocks:
        eq_code = block["eq_code"]
        prefix = eq_code[:2]

        # type3 검증 (ES/EC만 허용)
        if prefix not in ("ES", "EC"):
            return False, f"invalid eq_code type: {eq_code}"

        if prefix == "ES":
            if eq_code not in SENSOR_EQS:
                return False, f"unknown sensor: {eq_code}"
            if block["vlen"] != 1:
                return False, f"{eq_code} vlen={block['vlen']}, expected=1"
            # 실장비 기준: 센서 1개만 있으므로 inst=1
            if block["inst"] != 1:
                return False, f"{eq_code} inst={block['inst']}, expected=1"
            if len(block["values"]) != 1:
                return False, f"{eq_code} values len={len(block['values'])}, expected=1"
            sensor_blocks[eq_code] = block

        elif prefix == "EC":
            if eq_code not in ("EC01", "EC02", "EC03"):
                return False, f"unknown motor: {eq_code}"
            if block["vlen"] != 1:
                return False, f"{eq_code} vlen={block['vlen']}, expected=1"
            if eq_code == "EC01":
                # 실장비 기준: 모터 1개씩 (inst=1)
                if block["inst"] != 1:
                    return False, f"EC01 inst={block['inst']}, expected=1"
                if len(block["values"]) != 1:
                    return False, f"EC01 values len={len(block['values'])}, expected=1"
                motor_blocks["EC01"] = block
            elif eq_code == "EC02":
                ec02_found = True
                # 실장비 기준: 모터 1개씩 (inst=1)
                if block["inst"] != 1:
                    return False, f"EC02 inst={block['inst']}, expected=1"
                if len(block["values"]) != 1:
                    return False, f"EC02 values len={len(block['values'])}, expected=1"
                motor_blocks["EC02"] = block
            elif eq_code == "EC03":
                ec03_found = True
                # 실장비 기준: 모터 1개씩 (inst=1)
                if block["inst"] != 1:
                    return False, f"EC03 inst={block['inst']}, expected=1"
                if len(block["values"]) != 1:
                    return False, f"EC03 values len={len(block['values'])}, expected=1"
                motor_blocks["EC03"] = block

    # 센서 2종 모두 존재 검증 (실장비 기준)
    if len(sensor_blocks) != 2:
        return False, f"sensor blocks count={len(sensor_blocks)}, expected=2"
    for eq in SENSOR_EQS:
        if eq not in sensor_blocks:
            return False, f"missing sensor: {eq}"

    # EC01 필수 검증
    if "EC01" not in motor_blocks:
        return False, "missing EC01 (blower)"

    # EC02/EC03 둘 다 존재해야 함 (운용 정책: 입기/배기)
    # 한쪽은 활성(RPM>0 또는 센티널), 한쪽은 비활성(OFF=0)
    if not ec02_found:
        return False, "missing EC02"
    if not ec03_found:
        return False, "missing EC03"

    # EC02, EC03: 입기/배기 정책 해제 (둘 다 RPM>0 허용)
    return True, None


# ============================================================
# Data Transformation
# ============================================================


def build_key12_text(regist_no: str, stall_ty_code: int, stall_no: int, room_no: int) -> str:
    """key12_text 생성"""
    return f"{regist_no}{stall_ty_code:02d}{stall_no:02d}{room_no:02d}"


def transform_to_db_format(parsed: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """파싱된 데이터를 DB 형식으로 변환"""
    key_info = parsed["key"]
    measure_ts = parsed["measure_ts"]
    blocks = parsed["blocks"]

    key12 = build_key12_text(
        key_info["regist_no"],
        key_info["stall_ty_code"],
        key_info["stall_no"],
        key_info["room_no"],
    )

    # 블록 분류
    sensor_data = {}
    ec01_data = None
    ec02_data = None
    ec03_data = None

    for block in blocks:
        eq_code = block["eq_code"]
        if eq_code in SENSOR_EQS:
            sensor_data[eq_code.lower()] = block["values"]
        elif eq_code == "EC01":
            ec01_data = block["values"]
        elif eq_code == "EC02":
            ec02_data = block["values"]
        elif eq_code == "EC03":
            ec03_data = block["values"]

    # vent_mode 판단 (입기/배기 정책 해제: 둘 다 활성 가능)
    # EC02>0 => exhaust 우선, EC03>0 => intake 우선, 둘 다 있으면 intake
    ec02_val = ec02_data[0] if ec02_data else 0
    ec03_val = ec03_data[0] if ec03_data else 0
    if ec02_val > 0 and ec02_val < NO_DATA and (ec03_val == 0 or ec03_val >= NO_DATA):
        vent_mode = "exhaust"
    elif ec03_val > 0 and ec03_val < NO_DATA:
        vent_mode = "intake"
    else:
        vent_mode = "intake"
    blower_count = len(ec01_data) if ec01_data else 0
    vent_count = 1  # 실장비: EC02 또는 EC03 중 1개 활성

    # key9 바이너리 (bytea용)
    key9_bytes = b""
    key9_bytes += key_info["regist_no"].encode("ascii")
    b6 = (key_info["species_bits"] << 5) | ((key_info["stall_ty_code"] - 1) << 1) | 0
    key9_bytes += bytes([b6, key_info["stall_no"], key_info["room_no"]])
    key_bin_hex = "\\x" + key9_bytes.hex()

    # species_code 계산 및 검증
    species_bits = key_info["species_bits"]
    if not (0 <= species_bits <= 7):
        raise ValueError(f"invalid species_bits={species_bits}, must be 0~7")
    species_code = SPECIES_CODE[species_bits]

    # mapping 데이터
    mapping_data = {
        "key12": key12,
        "isind_regist_no": key_info["regist_no"],
        "species": key_info["species_bits"],
        "species_code": species_code,
        "stall_ty_code": key_info["stall_ty_code"],
        "stall_no": key_info["stall_no"],
        "room_no": key_info["room_no"],
        "vent_mode": vent_mode,
        "blower_count": blower_count,
        "vent_count": vent_count,
        "key_bin": key_bin_hex,
    }

    # snapshot 데이터 (실장비 기준: ES01, ES02만)
    snapshot_data = {
        "key12": key12,
        "measure_ts": measure_ts.isoformat(),
        "es01": sensor_data.get("es01"),
        "es02": sensor_data.get("es02"),
        "ec01": ec01_data,
        "ec02": ec02_data if ec02_data else None,
        "ec03": ec03_data if ec03_data else None,
        "updated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
    }

    # log 데이터 (실장비 기준: ES01, ES02만)
    log_data = {
        "key12": key12,
        "measure_ts": measure_ts.isoformat(),
        "es01": sensor_data.get("es01"),
        "es02": sensor_data.get("es02"),
        "ec01": ec01_data,
        "ec02": ec02_data if ec02_data else None,
        "ec03": ec03_data if ec03_data else None,
    }

    return mapping_data, snapshot_data, log_data


# ============================================================
# JSON Serialization
# ============================================================


def serialize_parsed_data(
    mapping_data: Dict[str, Any],
    snapshot_data: Dict[str, Any],
    log_data: Dict[str, Any],
    payload_len: int,
    payload_head_hex: str,
) -> str:
    """파싱된 데이터를 JSON으로 직렬화"""
    parsed_data = {
        "mapping_data": mapping_data,
        "snapshot_data": snapshot_data,
        "log_data": log_data,
        "metadata": {
            "version": 1,
            "parsed_at": datetime.now(KST).isoformat(),
            "payload_len": payload_len,
            "payload_head_hex": payload_head_hex,
        },
    }
    if ORJSON_AVAILABLE:
        # orjson 사용 (더 빠름)
        return orjson.dumps(parsed_data, option=orjson.OPT_NON_STR_KEYS).decode('utf-8')
    else:
        # 표준 json 사용 (fallback)
        return json.dumps(parsed_data, ensure_ascii=False, separators=(",", ":"))


# ============================================================
# Redis Batch Pop (Lua Script)
# ============================================================

BATCH_POP_SCRIPT = """
local queue = KEYS[1]
local batch_size = tonumber(ARGV[1])
local items = redis.call('LRANGE', queue, 0, batch_size - 1)
if #items > 0 then
    redis.call('LTRIM', queue, batch_size, -1)
end
return items
"""


def batch_pop_from_redis(redis_client: redis.Redis, queue: str, batch_size: int) -> List[bytes]:
    """원자적 배치 pop"""
    script = redis_client.register_script(BATCH_POP_SCRIPT)
    result = script(keys=[queue], args=[batch_size])
    if result is None:
        return []
    return [item if isinstance(item, bytes) else bytes(item) for item in result]


# ============================================================
# Redis Operations
# ============================================================


def save_to_redis_parsed(
    redis_client: redis.Redis,
    out_queue: str,
    json_str: str,
    retry_count: int = 3,
    retry_delays: List[float] = [0.1, 0.2, 0.5],
) -> Tuple[bool, Optional[str]]:
    """JSON 문자열을 Redis 큐에 저장 (재시도 포함)
    
    Args:
        redis_client: Redis 클라이언트
        out_queue: 출력 큐 이름
        json_str: JSON 문자열
        retry_count: 최대 재시도 횟수
        retry_delays: 재시도 간격 (초)
        
    Returns:
        (success, error_message)
    """
    last_error = None
    
    for attempt in range(retry_count):
        try:
            redis_client.rpush(out_queue, json_str)
            return True, None
        except Exception as e:
            last_error = str(e)
            if attempt < retry_count - 1:
                time.sleep(retry_delays[attempt])
    
    return False, last_error


def save_to_redis_parsed_batch(
    redis_client: redis.Redis,
    out_queue: str,
    json_str_list: List[str],
    retry_count: int = 3,
    retry_delays: List[float] = [0.1, 0.2, 0.5],
) -> Tuple[bool, Optional[str]]:
    """JSON 문자열 리스트를 Redis 큐에 배치 저장 (재시도 포함)
    
    Args:
        redis_client: Redis 클라이언트
        out_queue: 출력 큐 이름
        json_str_list: JSON 문자열 리스트
        retry_count: 최대 재시도 횟수
        retry_delays: 재시도 간격 (초)
        
    Returns:
        (success, error_message)
    """
    if not json_str_list:
        return True, None
    
    last_error = None
    
    for attempt in range(retry_count):
        try:
            # 배치로 한 번에 저장
            redis_client.rpush(out_queue, *json_str_list)
            return True, None
        except Exception as e:
            last_error = str(e)
            if attempt < retry_count - 1:
                time.sleep(retry_delays[attempt])
    
    return False, last_error


# ============================================================
# DLQ Operations
# ============================================================


def send_to_dlq_raw(
    redis_client: redis.Redis,
    dlq_queue: str,
    payload: bytes,
    reason: str,
    error_type: str,
    hint: Optional[Dict[str, Any]] = None,
) -> None:
    """Raw payload를 DLQ에 저장 (파싱/검증/변환 실패 시)
    
    Args:
        redis_client: Redis 클라이언트
        dlq_queue: DLQ 큐 이름
        payload: 원본 payload (bytes)
        reason: 에러 원인
        error_type: 에러 타입 (parse_error, validation_error, transform_error)
        hint: 힌트 정보
    """
    try:
        dlq_entry = {
            "ts": datetime.now(ZoneInfo("UTC")).isoformat(),
            "source": "assembler",
            "reason": reason,
            "error_type": error_type,
            "data": {
                "raw_b64": base64.b64encode(payload).decode("ascii"),
                "raw_head_hex": payload[:64].hex() if len(payload) >= 64 else payload.hex(),
                "payload_len": len(payload),
                "hint": hint or {},
            },
            "metadata": {
                "stage": error_type.replace("_error", ""),
                "queue": "rx:v2:raw",
            },
        }
        if ORJSON_AVAILABLE:
            dlq_json = orjson.dumps(dlq_entry, option=orjson.OPT_NON_STR_KEYS).decode('utf-8')
        else:
            dlq_json = json.dumps(dlq_entry, ensure_ascii=False)
        redis_client.rpush(dlq_queue, dlq_json)
    except Exception as e:
        logging.error(f"Failed to send to DLQ: {e}")


def send_to_dlq_parsed(
    redis_client: redis.Redis,
    dlq_queue: str,
    mapping_data: Dict[str, Any],
    snapshot_data: Dict[str, Any],
    log_data: Dict[str, Any],
    reason: str,
    error_type: str = "redis_error",
    retry_count: int = 0,
) -> None:
    """Parsed data를 DLQ에 저장 (Redis 저장 실패 시)
    
    Args:
        redis_client: Redis 클라이언트
        dlq_queue: DLQ 큐 이름
        mapping_data: mapping 테이블 데이터
        snapshot_data: snapshot 테이블 데이터
        log_data: log 테이블 데이터
        reason: 에러 원인
        error_type: 에러 타입 (기본: redis_error)
        retry_count: 재시도 횟수
    """
    try:
        dlq_entry = {
            "ts": datetime.now(ZoneInfo("UTC")).isoformat(),
            "source": "assembler",
            "reason": reason,
            "error_type": error_type,
            "data": {
                "mapping_data": mapping_data,
                "snapshot_data": snapshot_data,
                "log_data": log_data,
            },
            "metadata": {
                "stage": "redis_save",
                "queue": "rx:v2:parsed",
                "retry_count": retry_count,
            },
        }
        if ORJSON_AVAILABLE:
            dlq_json = orjson.dumps(dlq_entry, option=orjson.OPT_NON_STR_KEYS).decode('utf-8')
        else:
            dlq_json = json.dumps(dlq_entry, ensure_ascii=False)
        redis_client.rpush(dlq_queue, dlq_json)
    except Exception as e:
        logging.error(f"Failed to send to DLQ: {e}")


# ============================================================
# Main
# ============================================================


def setup_logging(level: str) -> None:
    """로깅 설정"""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    """메인 함수"""
    parser = argparse.ArgumentParser(description="Assembler: Redis -> Parse -> Redis Parsed Queue")
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis DB")
    parser.add_argument("--redis-password", default=None, help="Redis password")
    parser.add_argument("--in-queue", default="rx:v2:raw", help="Input queue name")
    parser.add_argument("--out-queue", default="rx:v2:parsed", help="Output queue name")
    parser.add_argument("--dlq-queue", default="rx:v2:dlq", help="DLQ queue name")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size")
    parser.add_argument("--sleep-idle", type=float, default=0.01, help="Sleep when idle (seconds)")
    parser.add_argument("--stats-interval-sec", type=float, default=30, help="Stats log interval (seconds)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run (no Redis write)")
    parser.add_argument("--log-level", default="INFO", help="Log level")

    args = parser.parse_args()

    setup_logging(args.log_level)

    if redis is None:
        logging.error("redis not installed. pip install redis")
        return 1

    # Redis 연결
    try:
        r = redis.Redis(
            host=args.redis_host,
            port=args.redis_port,
            db=args.redis_db,
            password=args.redis_password,
            socket_timeout=2,
            socket_connect_timeout=2,
            decode_responses=False,
        )
        r.ping()
    except Exception as e:
        logging.error(f"Redis connect failed: {e}")
        return 2

    logging.info("Assembler started")
    logging.info(f"Input queue: {args.in_queue}, Output queue: {args.out_queue}, DLQ: {args.dlq_queue}, batch_size: {args.batch_size}")
    if ORJSON_AVAILABLE:
        logging.info("JSON serialization: orjson (optimized)")
    else:
        logging.info("JSON serialization: standard json (install orjson for better performance: pip install orjson)")
    if args.dry_run:
        logging.info("DRY RUN mode (no Redis write)")

    stats = {"ok": 0, "dlq": 0, "total": 0}
    last_stats_time = time.time()

    try:
        while True:
            # 배치 pop
            batch = batch_pop_from_redis(r, args.in_queue, args.batch_size)

            if not batch:
                time.sleep(args.sleep_idle)
                # 주기마다 통계 출력
                if time.time() - last_stats_time >= args.stats_interval_sec:
                    logging.info(f"[STATS] total={stats['total']} ok={stats['ok']} dlq={stats['dlq']}")
                    last_stats_time = time.time()
                continue

            batch_ok = 0
            batch_dlq = 0

            # 배치 데이터 수집
            json_batch = []  # 성공한 JSON 문자열 리스트
            parsed_batch = []  # (mapping_data, snapshot_data, log_data, json_str) 튜플 리스트

            # 1단계: 파싱, 검증, 변환, 직렬화
            for payload in batch:
                stats["total"] += 1
                hint = {}

                try:
                    # 파싱
                    parsed = parse_payload(payload)
                    key_info = parsed["key"]
                    species_bits = key_info["species_bits"]
                    species_code = SPECIES_CODE[species_bits] if 0 <= species_bits <= 7 else "?"
                    hint = {
                        "regist_no": key_info["regist_no"],
                        "stall_no": key_info["stall_no"],
                        "room_no": key_info["room_no"],
                        "stall_ty_code": key_info["stall_ty_code"],
                        "species": species_bits,
                        "species_code": species_code,
                    }

                    # 검증
                    valid, reason = validate_payload(parsed)
                    if not valid:
                        send_to_dlq_raw(
                            r,
                            args.dlq_queue,
                            payload,
                            f"validation failed: {reason}",
                            "validation_error",
                            hint,
                        )
                        batch_dlq += 1
                        stats["dlq"] += 1
                        continue

                    # 변환 (species 범위 검증 포함)
                    try:
                        mapping_data, snapshot_data, log_data = transform_to_db_format(parsed)
                    except ValueError as e:
                        if "invalid species_bits" in str(e):
                            send_to_dlq_raw(
                                r,
                                args.dlq_queue,
                                payload,
                                f"invalid_species: {str(e)}",
                                "transform_error",
                                hint,
                            )
                            batch_dlq += 1
                            stats["dlq"] += 1
                            continue
                        raise

                    # JSON 직렬화
                    try:
                        json_str = serialize_parsed_data(
                            mapping_data,
                            snapshot_data,
                            log_data,
                            len(payload),
                            payload[:64].hex() if len(payload) >= 64 else payload.hex(),
                        )
                    except Exception as e:
                        send_to_dlq_raw(
                            r,
                            args.dlq_queue,
                            payload,
                            f"serialization error: {str(e)}",
                            "transform_error",
                            hint,
                        )
                        batch_dlq += 1
                        stats["dlq"] += 1
                        continue

                    # 배치에 추가 (나중에 한 번에 저장)
                    json_batch.append(json_str)
                    parsed_batch.append((mapping_data, snapshot_data, log_data, json_str))

                    # DEBUG 모드에서만 상세 출력
                    if logging.getLogger().level == logging.DEBUG:
                        key12 = mapping_data["key12"]
                        measure_ts = snapshot_data["measure_ts"]
                        vent_mode = mapping_data["vent_mode"]
                        blower_count = mapping_data["blower_count"]
                        vent_count = mapping_data["vent_count"]
                        species_code_val = mapping_data.get("species_code", "?")
                        logging.debug(
                            f"[OK] key12={key12} ts={measure_ts} vent={vent_mode} "
                            f"blower={blower_count} vent={vent_count} species_code={species_code_val}"
                        )

                except Exception as e:
                    send_to_dlq_raw(
                        r,
                        args.dlq_queue,
                        payload,
                        f"parse error: {str(e)}",
                        "parse_error",
                        hint,
                    )
                    batch_dlq += 1
                    stats["dlq"] += 1
                    logging.debug(f"Parse error: {e}", exc_info=True)

            # 2단계: 배치로 Redis 저장
            if json_batch:
                if args.dry_run:
                    batch_ok += len(json_batch)
                    stats["ok"] += len(json_batch)
                else:
                    success, error = save_to_redis_parsed_batch(
                        r,
                        args.out_queue,
                        json_batch,
                    )
                    if success:
                        batch_ok += len(json_batch)
                        stats["ok"] += len(json_batch)
                    else:
                        # 배치 저장 실패 시 개별 메시지로 DLQ 처리
                        for mapping_data, snapshot_data, log_data, json_str in parsed_batch:
                            send_to_dlq_parsed(
                                r,
                                args.dlq_queue,
                                mapping_data,
                                snapshot_data,
                                log_data,
                                f"redis batch save failed: {error}",
                                "redis_error",
                                retry_count=3,
                            )
                            batch_dlq += 1
                            stats["dlq"] += 1

            # dlq 발생 시 해당 배치에 한해 경고 1회
            if batch_dlq > 0:
                logging.warning(f"[ASM] dlq batch: ok={batch_ok} dlq={batch_dlq}")

            # 주기마다 통계 출력 (배치 처리 후에도 경과 시 출력)
            if time.time() - last_stats_time >= args.stats_interval_sec:
                logging.info(f"[STATS] total={stats['total']} ok={stats['ok']} dlq={stats['dlq']}")
                last_stats_time = time.time()

    except KeyboardInterrupt:
        logging.info("Stopped by user")
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
