#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""saver.py

Redis Parsed Queue Consumer -> Supabase DB Writer

요구사항:
- Redis "rx:v2:parsed" 큐에서 JSON을 배치로 소비
- Supabase PostgreSQL에 3개 테이블 저장 (mapping, snapshot, log)
- 병렬 저장으로 성능 최적화
- 실패 시 DLQ 전송
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

try:
    import redis
except ImportError:
    redis = None

try:
    import requests
except ImportError:
    requests = None

try:
    from supabase_config import SUPABASE_URL, HEADERS
except ImportError:
    SUPABASE_URL = None
    HEADERS = None

# ============================================================
# Constants
# ============================================================

RETRY_DELAYS = [0.2, 0.5, 1.0]
RETRY_COUNT = 3
SUPABASE_TIMEOUT = 5

# 센티널 (물리적 장비 없음/미수신) - DB 저장 직전에 NULL로 변환
# 절대 변환 금지: 0 (RPM=0은 정상 OFF 값)
NOT_INSTALLED = 131071  # 0x1FFFF - 물리적 장비 없음
NO_DATA = 131070        # 0x1FFFE - 값 미수신/타임아웃

# Redis Lua script for atomic batch pop
BATCH_POP_SCRIPT = """
local queue = KEYS[1]
local batch_size = tonumber(ARGV[1])
local items = redis.call('LRANGE', queue, 0, batch_size - 1)
if #items > 0 then
    redis.call('LTRIM', queue, batch_size, -1)
end
return items
"""

# ============================================================
# JSON Parsing Functions
# ============================================================


def deserialize_parsed_data(json_str: str) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """JSON 문자열을 파싱하여 데이터 추출
    
    Args:
        json_str: JSON 문자열
        
    Returns:
        (mapping_data, snapshot_data, log_data, metadata)
        
    Raises:
        KeyError: 필수 필드가 없을 때
        json.JSONDecodeError: JSON 파싱 실패 시
    """
    parsed = json.loads(json_str)
    
    # 필수 필드 확인
    if "mapping_data" not in parsed:
        raise KeyError("mapping_data")
    if "snapshot_data" not in parsed:
        raise KeyError("snapshot_data")
    if "log_data" not in parsed:
        raise KeyError("log_data")
    
    return (
        parsed["mapping_data"],
        parsed["snapshot_data"],
        parsed["log_data"],
        parsed.get("metadata", {}),
    )


# ============================================================
# Sentinel to NULL Conversion
# ============================================================

def convert_sentinel_to_null(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    센티널 값을 NULL로 변환 (DB 저장 직전)
    
    변환 대상: 131071(NOT_INSTALLED), 131070(NO_DATA)
    절대 변환 금지: 0 (OFF는 정상값), 그 외 정상 값
    
    Args:
        data: 데이터 딕셔너리
        
    Returns:
        변환된 데이터 딕셔너리 (shallow copy)
    """
    result = data.copy()
    
    for key, value in result.items():
        if isinstance(value, list):
            # 리스트 내 센티널만 변환 (0은 절대 NULL 변환 금지)
            result[key] = [
                None if v in (NOT_INSTALLED, NO_DATA) else v
                for v in value
            ]
        elif isinstance(value, int) and value in (NOT_INSTALLED, NO_DATA):
            # 단일 값 센티널 변환 (0은 여기 해당 안 함)
            result[key] = None
    
    return result


# ============================================================
# Supabase Save Functions
# ============================================================


def save_mapping(mapping_data: Dict[str, Any]) -> None:
    """mapping 테이블 UPSERT (센티널 변환 없음)"""
    if SUPABASE_URL is None or HEADERS is None:
        raise ValueError("SUPABASE_URL or HEADERS not configured")
    
    base_url = f"{SUPABASE_URL}/rest/v1"
    headers = HEADERS.copy()
    headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
    
    resp = requests.post(
        f"{base_url}/eqpmn_mapping_set_v3?on_conflict=key12",
        headers=headers,
        json=mapping_data,
        timeout=SUPABASE_TIMEOUT,
    )
    resp.raise_for_status()


def save_snapshot(snapshot_data: Dict[str, Any]) -> None:
    """snapshot 테이블 UPSERT (센티널 -> NULL 변환)"""
    if SUPABASE_URL is None or HEADERS is None:
        raise ValueError("SUPABASE_URL or HEADERS not configured")
    
    # 조회된 값만 전송 (es03/es04/es09 미설치 시 DB에서 NULL 허용)
    snapshot_data = convert_sentinel_to_null(snapshot_data)
    
    base_url = f"{SUPABASE_URL}/rest/v1"
    headers = HEADERS.copy()
    headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
    
    resp = requests.post(
        f"{base_url}/room_raw_snapshot_v3?on_conflict=key12",
        headers=headers,
        json=snapshot_data,
        timeout=SUPABASE_TIMEOUT,
    )
    resp.raise_for_status()


def save_log(log_data: Dict[str, Any]) -> None:
    """log 테이블 INSERT (센티널 -> NULL 변환)"""
    if SUPABASE_URL is None or HEADERS is None:
        raise ValueError("SUPABASE_URL or HEADERS not configured")
    
    # 조회된 값만 전송 (es03/es04/es09 미설치 시 DB에서 NULL 허용)
    log_data = convert_sentinel_to_null(log_data)
    
    base_url = f"{SUPABASE_URL}/rest/v1"
    headers = HEADERS.copy()
    headers["Prefer"] = "resolution=ignore-duplicates,return=minimal"
    
    resp = requests.post(
        f"{base_url}/room_state_log_v3?on_conflict=key12,measure_ts",
        headers=headers,
        json=log_data,
        timeout=SUPABASE_TIMEOUT,
    )
    resp.raise_for_status()


def save_mapping_batch(mapping_data_list: List[Dict[str, Any]], session: requests.Session) -> None:
    """mapping 테이블 배치 UPSERT
    
    Args:
        mapping_data_list: mapping 테이블 데이터 리스트
        session: HTTP Session (연결 풀링)
    """
    if SUPABASE_URL is None or HEADERS is None:
        raise ValueError("SUPABASE_URL or HEADERS not configured")
    
    if not mapping_data_list:
        return
    
    base_url = f"{SUPABASE_URL}/rest/v1"
    headers = {"Prefer": "resolution=merge-duplicates,return=minimal"}
    
    resp = session.post(
        f"{base_url}/eqpmn_mapping_set_v3?on_conflict=key12",
        headers=headers,
        json=mapping_data_list,  # 배열로 전송
        timeout=SUPABASE_TIMEOUT * 3,  # 배치 처리 시 타임아웃 증가
    )
    resp.raise_for_status()


def save_snapshot_batch(snapshot_data_list: List[Dict[str, Any]], session: requests.Session) -> None:
    """snapshot 테이블 배치 UPSERT (센티널 -> NULL 변환)
    
    Args:
        snapshot_data_list: snapshot 테이블 데이터 리스트
        session: HTTP Session (연결 풀링)
    """
    if SUPABASE_URL is None or HEADERS is None:
        raise ValueError("SUPABASE_URL or HEADERS not configured")
    
    if not snapshot_data_list:
        return
    
    # 조회된 값만 전송 (es03/es04/es09 미설치 시 DB에서 NULL 허용)
    snapshot_data_list = [convert_sentinel_to_null(data) for data in snapshot_data_list]
    
    base_url = f"{SUPABASE_URL}/rest/v1"
    headers = {"Prefer": "resolution=merge-duplicates,return=minimal"}
    
    resp = session.post(
        f"{base_url}/room_raw_snapshot_v3?on_conflict=key12",
        headers=headers,
        json=snapshot_data_list,  # 배열로 전송
        timeout=SUPABASE_TIMEOUT * 3,  # 배치 처리 시 타임아웃 증가
    )
    resp.raise_for_status()


def save_log_batch(log_data_list: List[Dict[str, Any]], session: requests.Session) -> None:
    """log 테이블 배치 INSERT (센티널 -> NULL 변환)
    
    Args:
        log_data_list: log 테이블 데이터 리스트
        session: HTTP Session (연결 풀링)
    """
    if SUPABASE_URL is None or HEADERS is None:
        raise ValueError("SUPABASE_URL or HEADERS not configured")
    
    if not log_data_list:
        return
    
    # 조회된 값만 전송 (es03/es04/es09 미설치 시 DB에서 NULL 허용)
    log_data_list = [convert_sentinel_to_null(data) for data in log_data_list]
    
    base_url = f"{SUPABASE_URL}/rest/v1"
    headers = {"Prefer": "resolution=ignore-duplicates,return=minimal"}
    
    resp = session.post(
        f"{base_url}/room_state_log_v3?on_conflict=key12,measure_ts",
        headers=headers,
        json=log_data_list,  # 배열로 전송
        timeout=SUPABASE_TIMEOUT * 3,  # 배치 처리 시 타임아웃 증가
    )
    resp.raise_for_status()


def save_to_supabase_parallel(
    mapping_data: Dict[str, Any],
    snapshot_data: Dict[str, Any],
    log_data: Dict[str, Any],
) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
    """병렬로 Supabase에 3개 테이블 저장
    
    Args:
        mapping_data: mapping 테이블 데이터
        snapshot_data: snapshot 테이블 데이터
        log_data: log 테이블 데이터
        
    Returns:
        (success, error_message, error_details)
        - success: 저장 성공 여부
        - error_message: 실패 시 에러 메시지
        - error_details: 실패 시 상세 에러 정보 (테이블별)
    """
    if SUPABASE_URL is None or HEADERS is None:
        return False, "SUPABASE_URL or HEADERS not configured", None
    
    errors = []
    error_details = {}
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(save_mapping, mapping_data): "mapping",
            executor.submit(save_snapshot, snapshot_data): "snapshot",
            executor.submit(save_log, log_data): "log",
        }
        
        for future in as_completed(futures):
            table_name = futures[future]
            try:
                future.result()
            except Exception as e:
                error_msg = str(e)
                errors.append(f"{table_name}: {error_msg}")
                error_details[table_name] = error_msg
    
    if errors:
        return False, "; ".join(errors), error_details
    return True, None, None


def save_to_supabase_batch(
    mapping_data_list: List[Dict[str, Any]],
    snapshot_data_list: List[Dict[str, Any]],
    log_data_list: List[Dict[str, Any]],
    session: Optional[requests.Session] = None,
) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]]]:
    """배치로 Supabase에 3개 테이블 저장
    
    Args:
        mapping_data_list: mapping 테이블 데이터 리스트
        snapshot_data_list: snapshot 테이블 데이터 리스트
        log_data_list: log 테이블 데이터 리스트
        session: HTTP Session (없으면 새로 생성)
        
    Returns:
        (success, error_message, error_details)
        - success: 저장 성공 여부
        - error_message: 실패 시 에러 메시지
        - error_details: 실패 시 상세 에러 정보 (테이블별)
    """
    if SUPABASE_URL is None or HEADERS is None:
        return False, "SUPABASE_URL or HEADERS not configured", None
    
    # Session이 없으면 새로 생성 (연결 풀링)
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
        close_session = True
    else:
        close_session = False
    
    errors = []
    error_details = {}
    
    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(save_mapping_batch, mapping_data_list, session): "mapping",
                executor.submit(save_snapshot_batch, snapshot_data_list, session): "snapshot",
                executor.submit(save_log_batch, log_data_list, session): "log",
            }
            
            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    future.result()
                except Exception as e:
                    error_msg = str(e)
                    if hasattr(e, "response") and e.response is not None:
                        try:
                            body = (e.response.text or "")[:500]
                            if body:
                                error_msg = f"{error_msg} | response: {body}"
                        except Exception:
                            pass
                    errors.append(f"{table_name}: {error_msg}")
                    error_details[table_name] = error_msg
        
        if errors:
            return False, "; ".join(errors), error_details
        return True, None, None
    
    finally:
        if close_session:
            session.close()


def save_to_supabase_with_retry(
    mapping_data: Dict[str, Any],
    snapshot_data: Dict[str, Any],
    log_data: Dict[str, Any],
    retry_count: int = RETRY_COUNT,
    retry_delays: List[float] = RETRY_DELAYS,
) -> Tuple[bool, Optional[str], int, Optional[Dict[str, Any]]]:
    """재시도 포함 Supabase 저장
    
    Args:
        mapping_data: mapping 테이블 데이터
        snapshot_data: snapshot 테이블 데이터
        log_data: log 테이블 데이터
        retry_count: 최대 재시도 횟수
        retry_delays: 재시도 간격 (초)
        
    Returns:
        (success, error_message, actual_retry_count, error_details)
        - success: 저장 성공 여부
        - error_message: 실패 시 마지막 에러 메시지
        - actual_retry_count: 실제 재시도 횟수 (0부터 시작)
        - error_details: 실패 시 상세 에러 정보 (테이블별)
    """
    last_error = None
    last_error_details = None
    actual_retry = 0
    
    for attempt in range(retry_count):
        actual_retry = attempt
        success, error, error_details = save_to_supabase_parallel(
            mapping_data,
            snapshot_data,
            log_data,
        )
        
        if success:
            return True, None, actual_retry, None
        
        last_error = error
        last_error_details = error_details
        if attempt < retry_count - 1:
            time.sleep(retry_delays[attempt])
    
    return False, last_error, actual_retry, last_error_details


# ============================================================
# DLQ Operations
# ============================================================


def send_to_dlq_json(
    redis_client: redis.Redis,
    dlq_queue: str,
    json_str: str,
    reason: str,
) -> None:
    """JSON 문자열을 DLQ에 저장 (JSON 파싱 실패 시)
    
    Args:
        redis_client: Redis 클라이언트
        dlq_queue: DLQ 큐 이름
        json_str: 원본 JSON 문자열
        reason: 에러 원인
    """
    try:
        dlq_entry = {
            "ts": datetime.now(ZoneInfo("UTC")).isoformat(),
            "source": "saver",
            "reason": reason,
            "error_type": "json_error",
            "data": {
                "json_str": json_str,
                "json_len": len(json_str),
            },
            "metadata": {
                "stage": "json_parse",
                "queue": "rx:v2:parsed",
            },
        }
        redis_client.rpush(dlq_queue, json.dumps(dlq_entry, ensure_ascii=False))
    except Exception as e:
        logging.error(f"Failed to send to DLQ: {e}")


def send_to_dlq_parsed(
    redis_client: redis.Redis,
    dlq_queue: str,
    mapping_data: Dict[str, Any],
    snapshot_data: Dict[str, Any],
    log_data: Dict[str, Any],
    reason: str,
    retry_count: int = 0,
    error_details: Optional[Dict[str, Any]] = None,
) -> None:
    """Parsed data를 DLQ에 저장 (Supabase 저장 실패 시)
    
    Args:
        redis_client: Redis 클라이언트
        dlq_queue: DLQ 큐 이름
        mapping_data: mapping 테이블 데이터
        snapshot_data: snapshot 테이블 데이터
        log_data: log 테이블 데이터
        reason: 에러 원인
        retry_count: 재시도 횟수
        error_details: 상세 에러 정보
    """
    try:
        dlq_entry = {
            "ts": datetime.now(ZoneInfo("UTC")).isoformat(),
            "source": "saver",
            "reason": reason,
            "error_type": "db_error",
            "data": {
                "mapping_data": mapping_data,
                "snapshot_data": snapshot_data,
                "log_data": log_data,
                "error_details": error_details or {},
            },
            "metadata": {
                "stage": "supabase_save",
                "queue": "rx:v2:parsed",
                "retry_count": retry_count,
                "retry_delays": RETRY_DELAYS,
            },
        }
        redis_client.rpush(dlq_queue, json.dumps(dlq_entry, ensure_ascii=False))
    except Exception as e:
        logging.error(f"Failed to send to DLQ: {e}")


# ============================================================
# Redis Operations
# ============================================================


def batch_pop_from_redis(redis_client: redis.Redis, queue: str, batch_size: int) -> List[str]:
    """원자적 배치 pop (JSON 문자열 리스트)
    
    Args:
        redis_client: Redis 클라이언트
        queue: 큐 이름
        batch_size: 배치 크기
        
    Returns:
        JSON 문자열 리스트 (빈 리스트 가능)
    """
    script = redis_client.register_script(BATCH_POP_SCRIPT)
    result = script(keys=[queue], args=[batch_size])
    if result is None:
        return []
    # bytes를 문자열로 변환
    return [item.decode('utf-8') if isinstance(item, bytes) else item for item in result]


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
    parser = argparse.ArgumentParser(description="Saver: Redis -> Parse -> Supabase")
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis DB")
    parser.add_argument("--redis-password", default=None, help="Redis password")
    parser.add_argument("--in-queue", default="rx:v2:parsed", help="Input queue name")
    parser.add_argument("--dlq-queue", default="rx:v2:dlq", help="DLQ queue name")
    parser.add_argument("--batch-size", type=int, default=200, help="Batch size")
    parser.add_argument("--sleep-idle", type=float, default=0.05, help="Sleep when idle (seconds)")
    parser.add_argument("--stats-interval-sec", type=float, default=30, help="Stats log interval (seconds)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run (no DB write)")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    
    args = parser.parse_args()
    
    setup_logging(args.log_level)
    
    # 초기화 검증
    if redis is None:
        logging.error("redis not installed. pip install redis")
        return 1
    
    if requests is None:
        logging.error("requests not installed. pip install requests")
        return 1
    
    if SUPABASE_URL is None or HEADERS is None:
        logging.error("supabase_config not found or incomplete")
        return 1
    
    # Redis 연결
    try:
        r = redis.Redis(
            host=args.redis_host,
            port=args.redis_port,
            db=args.redis_db,
            password=args.redis_password,
            decode_responses=False,  # Lua script 결과 처리를 위해 False
        )
        r.ping()
    except Exception as e:
        logging.error(f"Failed to connect to Redis: {e}")
        return 1
    
    logging.info(
        f"Saver started: in_queue={args.in_queue} "
        f"dlq_queue={args.dlq_queue} batch_size={args.batch_size} "
        f"dry_run={args.dry_run} batch_upload=enabled"
    )
    
    # HTTP Session 생성 (연결 풀링)
    if not args.dry_run:
        session = requests.Session()
        session.headers.update(HEADERS)
    
    # 통계
    stats = {"total": 0, "ok": 0, "dlq": 0}
    last_stats_time = time.time()
    
    try:
        while True:
            # 배치 pop
            batch = batch_pop_from_redis(r, args.in_queue, args.batch_size)
            
            if not batch:
                time.sleep(args.sleep_idle)
                # 통계 출력 (주기마다)
                now = time.time()
                if now - last_stats_time >= args.stats_interval_sec:
                    if stats["total"] > 0:
                        elapsed = now - last_stats_time
                        rate = stats["ok"] / elapsed if elapsed > 0 else 0
                        logging.info(
                            f"[STATS] total={stats['total']} ok={stats['ok']} dlq={stats['dlq']} "
                            f"rate={rate:.2f} msg/sec"
                        )
                    last_stats_time = now
                    stats = {"total": 0, "ok": 0, "dlq": 0}
                continue
            
            batch_ok = 0
            batch_dlq = 0
            
            # 배치 데이터 수집
            parsed_batch = []  # (json_str, mapping_data, snapshot_data, log_data, metadata)
            failed_json = []  # (json_str, error_reason)
            
            # 1단계: JSON 파싱
            for json_str in batch:
                stats["total"] += 1
                
                try:
                    # JSON 파싱
                    mapping_data, snapshot_data, log_data, metadata = deserialize_parsed_data(json_str)
                    parsed_batch.append((json_str, mapping_data, snapshot_data, log_data, metadata))
                    
                except json.JSONDecodeError as e:
                    # JSON 파싱 실패
                    send_to_dlq_json(
                        r,
                        args.dlq_queue,
                        json_str,
                        f"JSON parse error: {str(e)}",
                    )
                    failed_json.append((json_str, f"JSON parse error: {str(e)}"))
                    batch_dlq += 1
                    stats["dlq"] += 1
                    logging.debug(f"JSON parse error: {e}", exc_info=True)
                    
                except KeyError as e:
                    # 필수 필드 누락
                    send_to_dlq_json(
                        r,
                        args.dlq_queue,
                        json_str,
                        f"Missing required field: {str(e)}",
                    )
                    failed_json.append((json_str, f"Missing required field: {str(e)}"))
                    batch_dlq += 1
                    stats["dlq"] += 1
                    logging.debug(f"Missing required field: {e}", exc_info=True)
                    
                except Exception as e:
                    # 기타 오류
                    send_to_dlq_json(
                        r,
                        args.dlq_queue,
                        json_str,
                        f"Unexpected error: {str(e)}",
                    )
                    failed_json.append((json_str, f"Unexpected error: {str(e)}"))
                    batch_dlq += 1
                    stats["dlq"] += 1
                    logging.debug(f"Unexpected error: {e}", exc_info=True)
            
            # 2단계: 배치로 Supabase 저장
            if parsed_batch:
                if args.dry_run:
                    batch_ok += len(parsed_batch)
                    stats["ok"] += len(parsed_batch)
                else:
                    # 배치 데이터 리스트 생성
                    mapping_data_list = [m for _, m, _, _, _ in parsed_batch]
                    snapshot_data_list = [s for _, _, s, _, _ in parsed_batch]
                    log_data_list = [l for _, _, _, l, _ in parsed_batch]
                    
                    # 배치 저장 시도 (재시도 포함)
                    success = False
                    retry_count = 0
                    last_error = None
                    last_error_details = None
                    
                    for attempt in range(RETRY_COUNT):
                        retry_count = attempt
                        success, error, error_details = save_to_supabase_batch(
                            mapping_data_list,
                            snapshot_data_list,
                            log_data_list,
                            session,
                        )
                        
                        if success:
                            batch_ok += len(parsed_batch)
                            stats["ok"] += len(parsed_batch)
                            break
                        
                        last_error = error
                        last_error_details = error_details
                        if attempt < RETRY_COUNT - 1:
                            time.sleep(RETRY_DELAYS[attempt])
                    
                    # 배치 저장 실패 시 에러 출력 후 개별 메시지로 DLQ 처리
                    if not success:
                        logging.error(
                            f"[SAV] Supabase save failed: {last_error}"
                            + (f" (details: {last_error_details})" if last_error_details else "")
                        )
                        for json_str, mapping_data, snapshot_data, log_data, metadata in parsed_batch:
                            send_to_dlq_parsed(
                                r,
                                args.dlq_queue,
                                mapping_data,
                                snapshot_data,
                                log_data,
                                f"DB batch write failed after {retry_count + 1} retries: {last_error}",
                                retry_count=retry_count + 1,
                                error_details=last_error_details,
                            )
                            batch_dlq += 1
                            stats["dlq"] += 1
            
            # dlq 발생 시 해당 배치에 한해 경고 1회
            if batch_dlq > 0:
                logging.warning(f"[SAV] dlq batch: ok={batch_ok} dlq={batch_dlq}")
            
            # 주기마다 통계 출력 (배치 처리 후에도 경과 시 출력 후 리셋)
            now = time.time()
            if now - last_stats_time >= args.stats_interval_sec:
                if stats["total"] > 0:
                    elapsed = now - last_stats_time
                    rate = stats["ok"] / elapsed if elapsed > 0 else 0
                    logging.info(
                        f"[STATS] total={stats['total']} ok={stats['ok']} dlq={stats['dlq']} "
                        f"rate={rate:.2f} msg/sec"
                    )
                last_stats_time = now
                stats = {"total": 0, "ok": 0, "dlq": 0}
    
    except KeyboardInterrupt:
        logging.info("Saver stopped by user")
        return 0
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        return 1
    finally:
        # HTTP Session 종료
        if not args.dry_run and 'session' in locals():
            session.close()


if __name__ == "__main__":
    sys.exit(main())
