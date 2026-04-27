# 프로토콜 명세서 (Protocol Specification)

> 대상: 통합 컨트롤러(Integrator) ↔ 수신 서버(Receiver) 간 MQTT 바이너리 프로토콜
> 버전: v2 데이터폼
> 최종 수정: 2026-04-13

---

## 1. 전체 통신 흐름

```
통합 컨트롤러                  MQTT Broker                  수신 서버(Receiver)
(main_farm)                                                (receiver)
     │                            │                            │
     │─── STATE 발행 ────────────►│                            │
     │    토픽: /KEY12/{key12}    │─── STATE 전달 ────────────►│
     │    QoS: 1                  │                            │
     │                            │                            │── base64 디코딩
     │                            │                            │── Redis 적재
     │                            │                            │
     │                            │◄── UPLINK_ACK 발행 ────────│
     │◄── UPLINK_ACK 전달 ────────│    토픽: /KEY12/{key12}/UPLINK_ACK
     │                            │    QoS: 1
     │                            │                            │
     │── ref_seq 매칭 ─►(완료)    │                            │
```

### 메시지 종류

| 메시지 | 방향 | 목적 |
|--------|------|------|
| **STATE** | 컨트롤러 → 서버 | 센서/모터 상태 데이터 전송 |
| **UPLINK_ACK** | 서버 → 컨트롤러 | STATE 수신 확인 응답 |
| **DOWNLINK** | 서버 → 컨트롤러 | 제어 명령 전달 |
| **STATE_ACK** | 컨트롤러 → 서버 | DOWNLINK 명령 실행 결과 응답 |

---

## 2. MQTT 토픽 규칙

### 토픽 형식

| 메시지 | 토픽 | 예시 |
|--------|------|------|
| STATE | `/KEY12/{key12}` | `/KEY12/FARM01010101` |
| UPLINK_ACK | `/KEY12/{key12}/UPLINK_ACK` | `/KEY12/FARM01010101/UPLINK_ACK` |
| DOWNLINK | `/KEY12/{key12}/DOWNLINK` | `/KEY12/FARM01010101/DOWNLINK` |
| STATE_ACK | `/KEY12/{key12}/STATE_ACK` | `/KEY12/FARM01010101/STATE_ACK` |

### key12 생성 규칙

key12는 항상 **12자리 ASCII 문자열**이다.

```
FARM01 + 01 + 01 + 01 = "FARM01010101"
├─────┘   ├──┘ ├──┘ ├──┘
등록번호   축사  축사  방
(6자)     타입  번호  번호
          (2자) (2자) (2자)
```

| 구성요소 | 길이 | 형식 | 범위 | 예시 |
|----------|------|------|------|------|
| regist_no | 6자 | ASCII | 고정 6자 | `FARM01` |
| stall_ty_code | 2자 | 0-패딩 정수 | 01~10 | `01` |
| stall_no | 2자 | 0-패딩 정수 | 00~99 | `01` |
| room_no | 2자 | 0-패딩 정수 | 00~99 | `01` |

### 구독 패턴

| 주체 | 구독 토픽 | 목적 |
|------|----------|------|
| 컨트롤러 | `/KEY12/+/UPLINK_ACK` | 모든 key12의 ACK 수신 |
| 컨트롤러 | `/KEY12/{key12}/DOWNLINK` | 특정 key12 명령 수신 |
| 수신 서버 | `/KEY12/+` | 모든 key12의 STATE 수신 |

---

## 3. MQTT 페이로드 인코딩 (텍스트 래핑)

MQTT payload는 바이너리를 직접 보내지 않고, **텍스트 형식으로 감싸서** 전송한다.

### STATE 발행 시

```
ref_seq 있을 때:  "SEQ:12345,B64:AQIDBA..."
ref_seq 없을 때:  "B64:AQIDBA..."
```

| 부분 | 설명 |
|------|------|
| `SEQ:12345` | 시퀀스 번호 (uint32, 10진수 문자열) |
| `,` | 구분자 |
| `B64:` | 접두사 (고정) |
| `AQIDBA...` | 바이너리 payload의 standard base64 인코딩 |

### UPLINK_ACK / DOWNLINK / STATE_ACK 발행 시

```
"B64:AQIDBA..."
```

SEQ 접두사 없이 `B64:` + base64 만 사용한다.

### 수신 측 디코딩 순서

```
1. UTF-8 텍스트로 변환
2. "SEQ:" 접두사가 있으면 → 첫 번째 "," 앞부분에서 ref_seq 추출
3. 나머지에서 "B64:" 접두사 제거
4. standard base64 디코딩 → 바이너리 payload
```

---

## 4. STATE 바이너리 페이로드 (v2 데이터폼)

base64 디코딩 후 얻어지는 바이너리의 구조이다.

### 전체 레이아웃

```
Offset  크기   필드명           설명
──────────────────────────────────────────────────
 0       9B   Key9             식별 정보
 9       4B   Time4            측정 시각
13       1B   BlockHeader      버전 + 블록 개수
14       ~    Block[0]         장비 데이터 블록 #0
 ~       ~    Block[1]         장비 데이터 블록 #1
 ...     ...  Block[N-1]       장비 데이터 블록 #N-1
```

---

### 4.1 Key9 (9바이트)

```
Offset  크기  필드명      설명
─────────────────────────────────────
 0      6B   regist_no   등록번호 (ASCII)
 6      1B   b6          축종 + 축사타입 (비트패킹)
 7      1B   stall_no    축사 번호 (uint8)
 8      1B   room_no     방 번호 (uint8)
```

#### b6 바이트 비트 레이아웃

```
bit:  7   6   5   4   3   2   1   0
      ├───species───┤├──stall_ty──┤ spare
          3bit            4bit      1bit
```

**계산 공식:**

```
b6 = (species << 5) | (stall_ty_norm << 1) | 0
```

#### species 매핑 (3bit)

| 비트값 | 10진수 | 축종 문자 | 의미 |
|--------|--------|-----------|------|
| `000` | 0 | W | 한우 |
| `001` | 1 | D | 젖소 |
| `010` | 2 | P | 돼지 |
| `011` | 3 | H | 말 |
| `100` | 4 | T | 칠면조 |
| `101` | 5 | R | 토끼 |
| `110` | 6 | I | 곤충 |
| `111` | 7 | B | 육우 |

#### stall_ty 매핑 (4bit)

```
저장값(stall_ty_norm) = stall_ty_code - 1
```

| stall_ty_code | 저장값 (4bit) | 비트 |
|---------------|---------------|------|
| 1 | 0 | `0000` |
| 2 | 1 | `0001` |
| 3 | 2 | `0010` |
| ... | ... | ... |
| 10 | 9 | `1001` |

#### 패킹 예시

```
입력: regist_no="FARM01", species='P'(돼지), stall_ty_code=1, stall_no=3, room_no=7

species    = 2       → 비트: 010
stall_norm = 1-1 = 0 → 비트: 0000
spare      = 0       → 비트: 0

b6 = 010 0000 0 = 0x40

Key9 hex: 46 41 52 4D 30 31  40  03  07
          ├─ "FARM01" ASCII ─┤  b6  st  rm
```

---

### 4.2 Time4 (4바이트, big-endian)

측정 시각을 27비트로 압축한다. **초(second)는 버린다.**

```
bit: 26    20  19  16  15  11  10   6   5    0
     ├──YY──┤  ├─MM─┤  ├─DD─┤  ├─HH─┤  ├─mm─┤
      7bit     4bit    5bit    5bit    6bit
```

**계산 공식:**

```
packed = (YY << 20) | (MM_stored << 16) | (DD_stored << 11) | (HH << 6) | mm
→ 4바이트 big-endian uint32로 저장
```

#### 변환 규칙 (주의!)

| 필드 | 변환 | 범위 | 설명 |
|------|------|------|------|
| YY | `year % 100` | 0~99 | 2025년 → 25 |
| MM_stored | `month - 1` | 0~11 | 4월 → 3 |
| DD_stored | `day - 1` | 0~30 | 13일 → 12 |
| HH | 그대로 | 0~23 | |
| mm | 그대로 | 0~59 | |

#### 패킹 예시

```
입력: 2025-04-13 14:30 KST

YY = 25        = 0b0011001  (7bit)
MM = 4-1 = 3   = 0b0011     (4bit)
DD = 13-1 = 12 = 0b01100    (5bit)
HH = 14        = 0b01110    (5bit)
mm = 30        = 0b011110   (6bit)

계산:
  (25 << 20) = 0x01900000
  (3  << 16) = 0x00030000
  (12 << 11) = 0x00006000
  (14 << 6)  = 0x00000380
  (30)       = 0x0000001E

  합계       = 0x0193639E

big-endian bytes: 01 93 63 9E
```

---

### 4.3 BlockHeader (1바이트)

```
bit:  7   6   5   4   3   2   1   0
      ├───ver───┤├──block_count──┤ spare
        3bit          4bit        1bit
```

**계산 공식:**

```
block_header = (ver << 5) | (block_count << 1) | 0
```

| 필드 | 값 | 설명 |
|------|---|------|
| ver | `2` (고정) | v2 데이터폼 |
| block_count | 0~15 | 뒤따르는 블록 수 |
| spare | `0` | 항상 0 |

#### 예시

```
ver=2, block_count=5 (기본: ES01, ES02, EC01, EC02, EC03)

block_header = (2 << 5) | (5 << 1) | 0
             = 0x40 | 0x0A | 0x00
             = 0x4A

hex: 4A
```

---

### 4.4 Block (가변 길이)

```
Offset  크기   필드명          설명
─────────────────────────────────────────
 0      1B    eqpmn           장비 코드 (비트패킹)
 1      1B    value_set_len   값 세트 길이 (보통 1)
 2      1B    instance_count  인스턴스 수 (보통 1)
 3      3B    val17[0]        17bit 값 #0
 6      3B    val17[1]        17bit 값 #1 (있으면)
 ...
```

총 블록 크기 = `3 + (3 × value_set_len × instance_count)` 바이트

#### eqpmn 바이트 비트 레이아웃

```
bit:  7   6   5   4   3   2   1   0
      ├───type───┤├────no────┤ spare
        3bit          4bit     1bit
```

**계산 공식:**

```
eqpmn = (type3 << 5) | (no_raw << 1) | 0
  여기서 no_raw = 장비번호 - 1
```

#### type 매핑 (3bit)

| 비트값 | 10진수 | 접두사 | 의미 |
|--------|--------|--------|------|
| `000` | 0 | ES | 환경 센서 (Environmental Sensor) |
| `001` | 1 | EC | 환경 제어기 (Environmental Controller) |
| `010` | 2 | PC | 전력 제어기 (Power Controller) |
| `011` | 3 | BI | 생체 장비 (Bio Instrument) |
| `100` | 4 | AF | 공기 필터 (Air Filter) |
| `101` | 5 | SF | 안전 장비 (Safety) |
| `110` | 6 | VI | 영상 장비 (Video) |

#### no 매핑 (4bit)

```
저장값(no_raw) = 장비번호 - 1
```

| 장비코드 | type(3bit) | no_raw(4bit) | eqpmn 바이트 |
|----------|-----------|-------------|-------------|
| ES01 | 0 (`000`) | 0 (`0000`) | `0x00` |
| ES02 | 0 (`000`) | 1 (`0001`) | `0x02` |
| EC01 | 1 (`001`) | 0 (`0000`) | `0x20` |
| EC02 | 1 (`001`) | 1 (`0001`) | `0x22` |
| EC03 | 1 (`001`) | 2 (`0010`) | `0x24` |

#### val17 (17bit unsigned, 3바이트)

하나의 값은 17비트이며, 3바이트에 걸쳐 저장한다.

```
byte[0]:  xxxxxxx M     ← M = MSB (bit16)
byte[1]:  HHHHHHHH      ← high 8bit of low16
byte[2]:  LLLLLLLL      ← low 8bit of low16
```

**계산 공식:**

```
MSB    = (value >> 16) & 1       → byte[0]
high8  = (value >> 8) & 0xFF    → byte[1]
low8   = value & 0xFF           → byte[2]
```

**디코딩 공식:**

```
value = (byte[0] & 1) << 16 | byte[1] << 8 | byte[2]
```

#### 센티널 값 (중요!)

| 값 | 16진수 | 의미 | 사용 시점 |
|----|--------|------|----------|
| `131071` | `0x1FFFF` | NOT_INSTALLED | 물리적으로 장비가 없음 |
| `131070` | `0x1FFFE` | NO_DATA | 장비는 있지만 값 미수신/타임아웃 |
| `0` | `0x00000` | 정상 OFF | 장비가 켜져있으나 출력 0 (센티널 아님) |

#### 기본 블록 구성 (5개)

| 순서 | 장비코드 | 의미 | value_set_len | instance_count | 값 내용 |
|------|---------|------|---------------|----------------|---------|
| 0 | ES01 | 온도 | 1 | 1 | `temperature × 10` (예: 25.3℃ → 253) |
| 1 | ES02 | 습도 | 1 | 1 | `humidity × 10` (예: 60.5% → 605) |
| 2 | EC01 | 송풍팬 | 1 | 1 | RPM 또는 센티널 |
| 3 | EC02 | 입기팬 | 1 | 1 | RPM 또는 센티널 |
| 4 | EC03 | 배기팬 | 1 | 1 | RPM 또는 센티널 |

---

## 5. UPLINK_ACK 바이너리 (24바이트 고정)

수신 서버가 STATE를 Redis에 적재 성공한 후 보내는 확인 응답이다.

### 레이아웃

```
Offset  크기  필드명       타입           설명
──────────────────────────────────────────────────
 0      1B   ver          uint8          프로토콜 버전 (0x01 고정)
 1      1B   type         uint8          메시지 타입 (0x40 고정)
 2     12B   key12        ASCII          수신한 STATE의 key12
14      4B   ack_ts_ms    uint32 (LE)    ACK 생성 시각 (밀리초)
18      1B   result       uint8          처리 결과
19      1B   flags        uint8          비트 플래그
20      4B   ref_seq      uint32 (LE)    원본 STATE의 시퀀스 번호
```

주의: ack_ts_ms와 ref_seq는 **little-endian**이다.

### result 매핑

| 값 | 의미 |
|----|------|
| `0x00` | OK — 정상 수신 및 Redis 적재 완료 |
| `0x01` | ERR — 수신했으나 처리 실패 |
| `0x02` | DROPPED — 큐 초과 등으로 드롭됨 |

### flags 비트 필드

```
bit:  7  6  5  4  3  2  1  0
      (reserved)            │
                            └── bit0: redis_push_ok (1=성공)
```

현재는 bit0만 사용한다. 나머지는 0으로 채운다.

---

## 6. DOWNLINK 바이너리 (가변 길이)

서버가 컨트롤러에 제어 명령을 보낼 때 사용한다.

### 레이아웃

```
Offset  크기   필드명        타입           설명
──────────────────────────────────────────────────
 0      1B    ver           uint8          0x01 고정
 1      1B    type          uint8          0x30 고정
 2     12B    key12         ASCII          대상 컨트롤러
14      ~     rx_id         len-prefixed   수신 ID 문자열
 ~      4B    received_at   uint32 (LE)    수신 시각 (Unix timestamp)
 ~      1B    result        uint8          STATE 처리 결과
 ~      1B    n_cmds        uint8          명령 개수
 ~      ~     cmds[0..N]    가변           명령 배열
```

### len-prefixed 문자열

```
[1B 길이][N바이트 ASCII 문자열]
```

첫 1바이트가 뒤따르는 문자열의 길이를 나타낸다. 최대 255자.

### result 매핑

| 값 | 의미 |
|----|------|
| `0x00` | STATE_ACK_OK — STATE 정상 처리됨 |
| `0x01` | STATE_ACK_ERR — STATE 처리 실패 |

### 명령(cmd) 구조

```
Offset  크기   필드명        타입
──────────────────────────────────────
 0      ~     cmd_id        len-prefixed ASCII
 ~      2B    ttl_sec       uint16 (LE)
 ~      1B    n_actions     uint8
 ~      ~     actions[0..N] 가변
```

### 액션(action) 구조 (7바이트 고정)

```
Offset  크기   필드명  타입         설명
──────────────────────────────────────────
 0      4B    eq      ASCII        장비코드 ("EC01" 등, 4자 고정)
 4      1B    op      uint8        동작 유형
 5      2B    val     uint16 (LE)  RPM 또는 퍼센트
```

### op 매핑

| 값 | 이름 | val 의미 |
|----|------|----------|
| `0x00` | SET_RPM | val = 목표 RPM |
| `0x01` | SET_RPM_PCT | val = MaxRPM 대비 퍼센트 (0~100) |

---

## 7. STATE_ACK 바이너리 (가변 길이)

컨트롤러가 DOWNLINK 명령 실행 결과를 응답할 때 사용한다.

### 레이아웃

```
Offset  크기   필드명      타입           설명
──────────────────────────────────────────────────
 0      1B    ver         uint8          0x01 고정
 1      1B    type        uint8          0x20 고정
 2     12B    key12       ASCII          컨트롤러 key12
14      4B    state_ts    uint32 (LE)    STATE 기준 시각
18      1B    n_acks      uint8          응답 개수
19      ~     cmd_acks[]  가변           명령별 응답 배열
```

### cmd_ack 구조

```
Offset  크기   필드명       타입
──────────────────────────────────────
 0      ~     cmd_id       len-prefixed ASCII
 ~      1B    result       uint8
 ~      4B    ack_ts       uint32 (LE)
 ~      2B    detail_len   uint16 (LE)
 ~      ~     detail       UTF-8 JSON 바이트
```

### result 매핑

| 값 | 이름 |
|----|------|
| `0x00` | OK |
| `0x01` | PARTIAL |
| `0x02` | FAIL |

---

## 8. 바이트 오더 정리

| 위치 | 바이트 오더 |
|------|------------|
| Time4 (STATE payload 내부) | **big-endian** |
| val17 (Block 값) | MSB 1bit + 16bit **big-endian** |
| UPLINK_ACK의 ack_ts_ms, ref_seq | **little-endian** |
| DOWNLINK의 received_at, ttl_sec, val | **little-endian** |
| STATE_ACK의 state_ts, ack_ts, detail_len | **little-endian** |

---

## 9. 메시지 타입 코드 요약

| 코드 | 이름 | 방향 |
|------|------|------|
| `0x20` | STATE_ACK | 컨트롤러 → 서버 |
| `0x30` | DOWNLINK | 서버 → 컨트롤러 |
| `0x40` | UPLINK_ACK | 서버 → 컨트롤러 |

---

## 10. ACK 메커니즘

### 비블로킹 모드 (기본)

```
컨트롤러                          서버
   │── STATE (ref_seq=1) ────────►│
   │── STATE (ref_seq=2) ────────►│
   │                               │
   │◄── UPLINK_ACK (ref_seq=1) ───│
   │◄── UPLINK_ACK (ref_seq=2) ───│
```

- STATE 발행 후 `ref_seq`를 대기 큐에 적재
- ACK_RESEND_TIMEOUT_SEC 초과 시 미응답 건 재전송
- ACK_RESEND_MAX_RETRIES 초과 시 해당 건 drop

### 블로킹 모드 (Stop-and-Wait)

```
컨트롤러                          서버
   │── STATE (ref_seq=1) ────────►│
   │        (ACK 대기중...)        │
   │◄── UPLINK_ACK (ref_seq=1) ───│
   │                               │
   │── STATE (ref_seq=2) ────────►│  ← ACK 받은 후에야 다음 전송
   │        (ACK 대기중...)        │
   │◄── UPLINK_ACK (ref_seq=2) ───│
```

- 1건 전송 후 ACK가 올 때까지 대기
- 타임아웃 시 동일 payload를 동일 ref_seq로 재전송
- ACK 수신 전에는 다음 메시지 전송 금지

---

## 11. 전체 STATE 패킷 예시

다음 조건으로 전체 바이너리를 조립하는 예시이다.

```
입력 조건:
  regist_no    = "FARM01"
  species      = 'P' (돼지)
  stall_ty_code = 1
  stall_no     = 1
  room_no      = 1
  temperature  = 25.3 ℃
  humidity     = 60.5 %
  EC01 RPM     = 1200
  EC02 RPM     = 800
  EC03         = NOT_INSTALLED (장비 없음)
  시각         = 2025-04-13 14:30 KST
```

### 단계별 조립

```
[1] Key9 (9B)
    regist_no = "FARM01" → 46 41 52 4D 30 31
    species='P'=2, stall_ty_code=1 → norm=0
    b6 = (2<<5)|(0<<1)|0 = 0x40
    stall_no=1, room_no=1

    → 46 41 52 4D 30 31 40 01 01

[2] Time4 (4B)
    2025-04-13 14:30
    YY=25, MM=3, DD=12, HH=14, mm=30
    packed = (25<<20)|(3<<16)|(12<<11)|(14<<6)|30
           = 0x0193639E

    → 01 93 63 9E

[3] BlockHeader (1B)
    ver=2, count=5
    = (2<<5)|(5<<1)|0 = 0x4A

    → 4A

[4] Block #0: ES01 온도 (6B)
    eqpmn = (0<<5)|(0<<1)|0 = 0x00
    value_set_len = 1, instance_count = 1
    val = 25.3 × 10 = 253 = 0x000FD
    val17: MSB=0, high8=0x00, low8=0xFD

    → 00 01 01 00 00 FD

[5] Block #1: ES02 습도 (6B)
    eqpmn = (0<<5)|(1<<1)|0 = 0x02
    val = 60.5 × 10 = 605 = 0x0025D
    val17: MSB=0, high8=0x02, low8=0x5D

    → 02 01 01 00 02 5D

[6] Block #2: EC01 RPM (6B)
    eqpmn = (1<<5)|(0<<1)|0 = 0x20
    val = 1200 = 0x004B0
    val17: MSB=0, high8=0x04, low8=0xB0

    → 20 01 01 00 04 B0

[7] Block #3: EC02 RPM (6B)
    eqpmn = (1<<5)|(1<<1)|0 = 0x22
    val = 800 = 0x00320
    val17: MSB=0, high8=0x03, low8=0x20

    → 22 01 01 00 03 20

[8] Block #4: EC03 NOT_INSTALLED (6B)
    eqpmn = (1<<5)|(2<<1)|0 = 0x24
    val = 131071 = 0x1FFFF
    val17: MSB=1, high8=0xFF, low8=0xFF

    → 24 01 01 01 FF FF
```

### 최종 바이너리 (44바이트)

```
46 41 52 4D 30 31 40 01 01    Key9
01 93 63 9E                    Time4
4A                             BlockHeader
00 01 01 00 00 FD              ES01(온도 25.3)
02 01 01 00 02 5D              ES02(습도 60.5)
20 01 01 00 04 B0              EC01(RPM 1200)
22 01 01 00 03 20              EC02(RPM 800)
24 01 01 01 FF FF              EC03(미설치)
```

### MQTT 최종 발행 문자열

```
SEQ:1,B64:RkFSTTAxQAEBATljngBBAQAAD0IBAQACXSABAQAEUCIBAQADICQBAQH//w==
```

---

## 12. 구현 체크리스트

### 컨트롤러(발신) 측

- [ ] key12 생성 (12자리 ASCII)
- [ ] Key9 패킹 (species 매핑, stall_ty_code - 1 변환)
- [ ] Time4 패킹 (month-1, day-1 변환, big-endian)
- [ ] BlockHeader 패킹 (ver=2 고정)
- [ ] Block 패킹 (eqpmn 비트패킹, val17 3바이트)
- [ ] 센서값 × 10 변환 (25.3℃ → 253)
- [ ] 센티널 값 처리 (NOT_INSTALLED=0x1FFFF / NO_DATA=0x1FFFE)
- [ ] base64 인코딩 + "B64:" 접두사
- [ ] ref_seq 생성 + "SEQ:N," 접두사
- [ ] MQTT QoS 1 발행
- [ ] UPLINK_ACK 수신 처리 (ref_seq 매칭)

### 수신 서버 측

- [ ] "/KEY12/+" 구독
- [ ] SEQ 접두사 파싱 → ref_seq 추출
- [ ] base64 디코딩
- [ ] DOWNLINK 형식(0x01, 0x30) 페이로드 무시 (가드)
- [ ] Redis 적재
- [ ] UPLINK_ACK 24바이트 패킹 (little-endian)
- [ ] UPLINK_ACK base64 인코딩 + 발행
