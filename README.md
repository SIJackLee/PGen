# 성일기전 프로토콜 자동생성기

MQTT CONNECT / PUBLISH 데이터를 생성하는 계산기 도구입니다.  
실제 MQTT 송신은 수행하지 않으며, 통신모듈 개발자에게 전달할 **HEX 패킷 · Base64 · Payload 문자열**을 생성합니다.

근거 문서: `Notion/protocol-spec.md` (v2 데이터폼, 최종 수정 2026-04-13)

---

## 프로젝트 구조

```
.
├── protocol/
│   └── protocol_generator.py   # 핵심 모듈 (Python)
├── api/
│   └── generate.py             # Vercel Python Serverless Function
├── app/
│   ├── layout.tsx
│   ├── page.tsx                # 메인 웹 UI
│   ├── globals.css
│   └── api/generate/route.ts  # Next.js Route Handler (TypeScript 동일 로직)
├── tests/
│   └── test_protocol.py        # pytest 테스트 (14개)
├── Notion/                     # 원본 스펙 문서 (수정 없음)
├── package.json
├── vercel.json
├── requirements.txt
└── README.md
```

---

## 로컬 실행

### 1. Python 테스트

```bash
pip install -r requirements.txt
python -m pytest
```

정상 출력:
```
14 passed in 0.06s
```

### 2. Next.js 개발 서버

```bash
npm install
npm run dev
```

브라우저에서 `http://localhost:3000` 접속

---

## Vercel 배포

```bash
npx vercel
```

> **참고:** `vercel.json`에 Python 런타임(`python3.9`)을 선언했습니다.  
> 배포 오류 발생 시 `vercel.json`을 제거하고 재시도하세요.  
> Next.js Route Handler(`app/api/generate/route.ts`)가 동일 로직을 TypeScript로 구현하므로  
> Python 함수 없이도 완전히 동작합니다.

---

## API 요청 예시

### POST /api/generate

```bash
curl -X POST http://localhost:3000/api/generate \
  -H "Content-Type: application/json" \
  -d '{
    "regist_no": "FARM01",
    "species": "P",
    "stall_ty_code": 1,
    "stall_no": 1,
    "room_no": 1,
    "ref_seq": 1,
    "year": 2025,
    "month": 4,
    "day": 13,
    "hour": 14,
    "minute": 30,
    "blocks": [
      ["ES01", 253],
      ["ES02", 605],
      ["EC01", 1200],
      ["EC02", 800],
      ["EC03", 131071]
    ],
    "keep_alive": 60,
    "username": null,
    "password": null,
    "clean_session": true
  }'
```

### 응답 예시

```json
{
  "key12": "FARM01010101",
  "connect_hex": "10 1B 00 04 4D 51 54 54 04 02 00 3C 00 0F 63 6D 5F 46 41 52 4D 30 31 30 31 30 31 30 31",
  "expected_connack": "20 02 00 00",
  "topic": "/KEY12/FARM01010101",
  "state_hex": "46 41 52 4D 30 31 40 01 01 01 93 63 9E 4A 00 01 01 00 00 FD 02 01 01 00 02 5D 20 01 01 00 04 B0 22 01 01 00 03 20 24 01 01 01 FF FF",
  "state_base64": "RkFSTTAxQAEBATljngBBAQAAD0IBAQACXSABAQAEUCIBAQADICQBAQH//w==",
  "mqtt_payload": "SEQ:1,B64:RkFSTTAxQAEBATljngBBAQAAD0IBAQACXSABAQAEUCIBAQADICQBAQH//w==",
  "publish_hex": "32 ..."
}
```

---

## 통신모듈 개발자 전달용 예시

### CONNECT HEX 해석 (FARM01 / cm_FARM01010101)

```
10 1B                  Fixed Header (CONNECT), Remaining Length=27
00 04 4D 51 54 54      Protocol Name "MQTT" (UTF-8 MQTT string)
04                     Protocol Level (MQTT 3.1.1)
02                     Connect Flags: Clean Session=1
00 3C                  Keep Alive = 60초
00 0F                  Client ID 길이 = 15
63 6D 5F ...           Client ID "cm_FARM01010101"
```

### PUBLISH HEX 해석 (QoS 1)

```
32                     Fixed Header: PUBLISH, QoS=1
XX                     Remaining Length (variable)
00 13                  Topic 길이 = 19
2F 4B 45 59 31 32 ...  Topic "/KEY12/FARM01010101"
00 01                  Packet Identifier = 1
53 45 51 3A ...        Payload "SEQ:1,B64:..."
```

### STATE 바이너리 구조 (44바이트, 테스트 벡터)

```
46 41 52 4D 30 31 40 01 01    Key9  (regist_no=FARM01, species=P, ty=1, stall=1, room=1)
01 93 63 9E                    Time4 (2025-04-13 14:30)
4A                             BlockHeader (ver=2, block_count=5)
00 01 01 00 00 FD              ES01 (온도 25.3℃ = 253)
02 01 01 00 02 5D              ES02 (습도 60.5% = 605)
20 01 01 00 04 B0              EC01 (RPM 1200)
22 01 01 00 03 20              EC02 (RPM 800)
24 01 01 01 FF FF              EC03 (NOT_INSTALLED = 131071)
```

### 센티널 값

| 값 | 16진수 | 의미 |
|----|--------|------|
| `131071` | `0x1FFFF` | NOT_INSTALLED — 물리적으로 장비 없음 |
| `131070` | `0x1FFFE` | NO_DATA — 장비 있으나 값 미수신 |
| `0` | `0x00000` | 정상 OFF |
