# supabase_rest.py
# ============================================================
# 역할: Supabase REST(PostgREST) 기반 명령 큐 DB 연동
# cmd_id URL 필터 회피: RPC 함수 사용 (claim_new_commands, rollback_command_to_new, update_command_ack)
# 입력: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
# 출력: room_command_queue_v3 연동
# ============================================================

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:
    requests = None

# ============================================================
# 상수
# ============================================================

VIEW_PUBLISH = "room_command_queue_v3_publish"
TABLE_QUEUE = "room_command_queue_v3"
DEFAULT_TIMEOUT = 10

# RPC 함수명 (supabase_rpc.sql에서 생성)
RPC_CLAIM = "claim_new_commands"
RPC_ROLLBACK = "rollback_command_to_new"
RPC_UPDATE_ACK = "update_command_ack"


# ============================================================
# SupabaseCommandDB
# ============================================================

class SupabaseCommandDB:
    """
    Supabase REST 기반 명령 큐 DB 클라이언트.
    
    - claim: RPC claim_new_commands (cmd_id URL 필터 없음)
    - rollback/update_ack: RPC 호출 (cmd_id를 JSON body로 전달)
    """

    def __init__(self, base_url: str, service_role_key: str, timeout: int = DEFAULT_TIMEOUT):
        if requests is None:
            raise ImportError("requests not installed. pip install requests")
        
        self.base_url = base_url.rstrip("/")
        self.rest_url = f"{self.base_url}/rest/v1"
        self.service_role_key = service_role_key
        self.timeout = timeout
        
        self._session = requests.Session()
        self._session.headers.update({
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json",
            "Accept-Profile": "public",
            "Content-Profile": "public",
        })
        
        logging.info(f"[SupabaseCommandDB] REST base={self.rest_url} (RPC mode)")

    def _now_utc_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def scan_and_claim_commands(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        RPC claim_new_commands 호출 → NEW를 QUEUED로 전이하고 claimed 행 반환.
        cmd_id URL 필터 없이 DB 내부에서 원자적 처리.
        
        Returns:
            [{"cmd_id": str, "key12": str, "cmd_json": str}, ...]
        """
        url = f"{self.rest_url}/rpc/{RPC_CLAIM}"
        body = {"p_limit": limit}
        try:
            resp = self._session.post(url, json=body, timeout=self.timeout)
            resp.raise_for_status()
            rows = resp.json()
        except Exception as e:
            err_body = ""
            if hasattr(e, "response") and e.response is not None:
                try:
                    err_body = e.response.text[:500] if e.response.text else ""
                except Exception:
                    pass
            logging.error(f"[SupabaseCommandDB] claim_new_commands failed: {e}")
            if err_body:
                logging.error(f"[SupabaseCommandDB] response: {err_body}")
            return []
        
        if not isinstance(rows, list):
            return []
        
        out = []
        for row in rows:
            cmd_id = row.get("cmd_id")
            key12 = row.get("key12")
            cmd_json_raw = row.get("cmd_json")
            if not cmd_id or not key12:
                continue
            if isinstance(cmd_json_raw, dict):
                cmd_json_str = json.dumps(cmd_json_raw, ensure_ascii=False)
            elif isinstance(cmd_json_raw, str):
                cmd_json_str = cmd_json_raw
            else:
                cmd_json_str = json.dumps(cmd_json_raw, ensure_ascii=False) if cmd_json_raw is not None else "{}"
            out.append({
                "cmd_id": str(cmd_id),
                "key12": str(key12).strip(),
                "cmd_json": cmd_json_str,
            })
        return out

    def rollback_to_new(self, cmd_id: str) -> None:
        """RPC rollback_command_to_new 호출 (cmd_id를 body로 전달)."""
        url = f"{self.rest_url}/rpc/{RPC_ROLLBACK}"
        body = {"p_cmd_id": str(cmd_id)}
        try:
            resp = self._session.post(url, json=body, timeout=self.timeout)
            resp.raise_for_status()
            logging.info(f"[SupabaseCommandDB] rollback_to_new cmd_id={cmd_id}")
        except Exception as e:
            logging.error(f"[SupabaseCommandDB] rollback_to_new failed cmd_id={cmd_id}: {e}")

    def update_ack(
        self,
        cmd_id: str,
        result: str,
        detail: Any,
        ack_ts: Optional[int],
        last_error: Optional[str] = None,
    ) -> None:
        """RPC update_command_ack 호출 (cmd_id를 body로 전달)."""
        if result in ("OK", "PARTIAL"):
            status = "ACKED"
            ack_result = result
            err_val = None
        else:
            status = "FAILED"
            ack_result = "FAIL" if result == "FAIL" else result
            err_val = last_error if last_error is not None else str(result)
        
        url = f"{self.rest_url}/rpc/{RPC_UPDATE_ACK}"
        body = {
            "p_cmd_id": str(cmd_id),
            "p_status": status,
            "p_ack_result": ack_result,
            "p_ack_detail": detail,
            "p_ack_ts": ack_ts,
            "p_acked_at": self._now_utc_iso(),
            "p_last_error": err_val,
        }
        try:
            resp = self._session.post(url, json=body, timeout=self.timeout)
            resp.raise_for_status()
            logging.debug(f"[SupabaseCommandDB] update_ack cmd_id={cmd_id} status={status}")
        except Exception as e:
            logging.error(f"[SupabaseCommandDB] update_ack failed cmd_id={cmd_id}: {e}")
