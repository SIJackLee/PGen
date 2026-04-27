# tests/test_protocol.py
# ============================================================
# 성일기전 프로토콜 자동생성기 — pytest 테스트
# ============================================================

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from protocol.protocol_generator import (
    build_key12,
    build_key9,
    pack_time4,
    pack_val17,
    pack_block,
    build_state_binary,
    build_state_base64,
    build_mqtt_payload,
    build_mqtt_connect_hex,
    build_mqtt_publish_hex,
    NOT_INSTALLED,
    NO_DATA,
)

# 테스트 벡터 — protocol-spec.md §11 예시와 동일
REGIST_NO     = "FARM01"
SPECIES       = "P"
STALL_TY_CODE = 1
STALL_NO      = 1
ROOM_NO       = 1
YEAR, MONTH, DAY, HOUR, MINUTE = 2025, 4, 13, 14, 30

TEST_BLOCKS = [
    ["ES01", 253],
    ["ES02", 605],
    ["EC01", 1200],
    ["EC02", 800],
    ["EC03", NOT_INSTALLED],
]


# ============================================================
# 기존 6개 테스트
# ============================================================

def test_key12():
    """key12가 FARM01010101인지 검증"""
    assert build_key12(REGIST_NO, STALL_TY_CODE, STALL_NO, ROOM_NO) == "FARM01010101"


def test_key9_hex():
    """Key9 hex가 46 41 52 4D 30 31 40 01 01인지 검증"""
    key9 = build_key9(REGIST_NO, SPECIES, STALL_TY_CODE, STALL_NO, ROOM_NO)
    assert key9.hex(" ").upper() == "46 41 52 4D 30 31 40 01 01"


def test_time4_hex():
    """Time4 hex가 01 93 63 9E인지 검증"""
    t4 = pack_time4(YEAR, MONTH, DAY, HOUR, MINUTE)
    assert t4.hex(" ").upper() == "01 93 63 9E"


def test_block_header():
    """BlockHeader가 0x4A(ver=2, count=5)인지 검증"""
    binary = build_state_binary(
        REGIST_NO, SPECIES, STALL_TY_CODE, STALL_NO, ROOM_NO,
        YEAR, MONTH, DAY, HOUR, MINUTE,
        TEST_BLOCKS,
    )
    # Key9(9B) + Time4(4B) = offset 13 → BlockHeader
    assert binary[13] == 0x4A


def test_connect_hex_starts_with_10():
    """CONNECT HEX가 '10'으로 시작하는지 검증 (Fixed Header)"""
    hex_str = build_mqtt_connect_hex("cm_FARM01010101")
    assert hex_str.startswith("10")


def test_publish_payload_prefix():
    """build_mqtt_payload() 결과가 'SEQ:1,B64:'로 시작하는지 검증"""
    payload = build_mqtt_payload(1, b"\x00")
    assert payload.startswith("SEQ:1,B64:")


# ============================================================
# 수정사항 반영 4개 테스트
# ============================================================

def test_state_base64_no_prefix():
    """build_state_base64() 결과에 'B64:'가 포함되지 않아야 한다"""
    binary = build_state_binary(
        REGIST_NO, SPECIES, STALL_TY_CODE, STALL_NO, ROOM_NO,
        YEAR, MONTH, DAY, HOUR, MINUTE,
        TEST_BLOCKS,
    )
    b64 = build_state_base64(binary)
    assert not b64.startswith("B64:"), "build_state_base64()가 'B64:' 접두사를 붙여서는 안 됩니다."
    assert "B64:" not in b64, "build_state_base64() 결과에 'B64:'가 포함되어서는 안 됩니다."


def test_mqtt_payload_starts_with_seq_b64():
    """build_mqtt_payload()는 'SEQ:{seq},B64:' 형식이어야 한다"""
    payload = build_mqtt_payload(1, b"\x46\x41")
    assert payload.startswith("SEQ:1,B64:")


def test_build_key12_invalid_regist_no():
    """regist_no가 6자가 아니면 ValueError를 발생시켜야 한다"""
    with pytest.raises(ValueError):
        build_key12("FARM", 1, 1, 1)        # 4자
    with pytest.raises(ValueError):
        build_key12("FARM0123", 1, 1, 1)    # 8자
    with pytest.raises(ValueError):
        build_key12("", 1, 1, 1)            # 0자
    # 정확히 6자는 정상
    assert build_key12("FARM01", 1, 1, 1) == "FARM01010101"


def test_connect_password_without_username():
    """username 없이 password만 있으면 ValueError를 발생시켜야 한다"""
    with pytest.raises(ValueError):
        build_mqtt_connect_hex("cm_TEST", password="secret123")
    # username + password 조합은 정상
    hex_str = build_mqtt_connect_hex("cm_TEST", username="user1", password="secret123")
    assert hex_str.startswith("10")


# ============================================================
# 추가 검증 테스트
# ============================================================

def test_full_state_binary_hex():
    """전체 STATE 바이너리를 spec §11 예시와 비교 검증"""
    binary = build_state_binary(
        REGIST_NO, SPECIES, STALL_TY_CODE, STALL_NO, ROOM_NO,
        YEAR, MONTH, DAY, HOUR, MINUTE,
        TEST_BLOCKS,
    )
    expected_hex = (
        "46 41 52 4D 30 31 40 01 01"  # Key9
        " 01 93 63 9E"                 # Time4
        " 4A"                          # BlockHeader (ver=2, count=5)
        " 00 01 01 00 00 FD"           # ES01 (253)
        " 02 01 01 00 02 5D"           # ES02 (605)
        " 20 01 01 00 04 B0"           # EC01 (1200)
        " 22 01 01 00 03 20"           # EC02 (800)
        " 24 01 01 01 FF FF"           # EC03 (NOT_INSTALLED=131071)
    )
    assert binary.hex(" ").upper() == expected_hex


def test_pack_val17_sentinel():
    """센티널 값 NOT_INSTALLED, NO_DATA의 val17 패킹 검증"""
    assert pack_val17(NOT_INSTALLED) == bytes([0x01, 0xFF, 0xFF])  # 0x1FFFF
    assert pack_val17(NO_DATA)       == bytes([0x01, 0xFF, 0xFE])  # 0x1FFFE
    assert pack_val17(0)             == bytes([0x00, 0x00, 0x00])


def test_blocks_dict_form():
    """dict 형태 blocks 입력도 올바르게 처리되어야 한다"""
    dict_blocks = [
        {"eq_code": "ES01", "values": [253], "value_set_len": 1, "instance_count": 1},
        {"eq_code": "ES02", "values": [605], "value_set_len": 1, "instance_count": 1},
    ]
    list_blocks = [["ES01", 253], ["ES02", 605]]
    binary_dict = build_state_binary(
        REGIST_NO, SPECIES, STALL_TY_CODE, STALL_NO, ROOM_NO,
        YEAR, MONTH, DAY, HOUR, MINUTE, dict_blocks,
    )
    binary_list = build_state_binary(
        REGIST_NO, SPECIES, STALL_TY_CODE, STALL_NO, ROOM_NO,
        YEAR, MONTH, DAY, HOUR, MINUTE, list_blocks,
    )
    assert binary_dict == binary_list


def test_publish_hex_fixed_header():
    """PUBLISH HEX Fixed Header: QoS=1 → 0x32"""
    topic = "/KEY12/FARM01010101"
    payload = "SEQ:1,B64:test"
    hex_str = build_mqtt_publish_hex(topic, payload)
    assert hex_str.startswith("32")
