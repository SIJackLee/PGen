// app/api/generate/route.ts
// Next.js Route Handler — Python api/generate.py를 직접 호출하는 대신
// 동일 로직을 TypeScript로 구현해 엣지/서버리스 어디서나 동작하도록 함.
// Vercel 배포 시 Python 함수와 병행해 사용할 수 있다.

import { NextRequest, NextResponse } from "next/server";

// ============================================================
// 상수
// ============================================================
const SPECIES_MAP: Record<string, number> = {
  W: 0, D: 1, P: 2, H: 3, T: 4, R: 5, I: 6, B: 7,
};
const EQ_TYPE_MAP: Record<string, number> = {
  ES: 0, EC: 1, PC: 2, BI: 3, AF: 4, SF: 5, VI: 6,
};
const NOT_INSTALLED = 131071;

// ============================================================
// 유틸리티
// ============================================================
function mqttString(s: string): Uint8Array {
  const enc = new TextEncoder().encode(s);
  const buf = new Uint8Array(2 + enc.length);
  buf[0] = (enc.length >> 8) & 0xff;
  buf[1] = enc.length & 0xff;
  buf.set(enc, 2);
  return buf;
}

function encodeRemainingLength(n: number): number[] {
  const bytes: number[] = [];
  do {
    let byte = n & 0x7f;
    n >>= 7;
    if (n > 0) byte |= 0x80;
    bytes.push(byte);
  } while (n > 0);
  return bytes;
}

function concat(...arrays: (Uint8Array | number[])[]): Uint8Array {
  const total = arrays.reduce((s, a) => s + a.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const a of arrays) {
    out.set(a instanceof Uint8Array ? a : new Uint8Array(a), offset);
    offset += a.length;
  }
  return out;
}

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0").toUpperCase())
    .join(" ");
}

// ============================================================
// 프로토콜 함수
// ============================================================
function buildKey12(
  registNo: string,
  stallTyCode: number,
  stallNo: number,
  roomNo: number
): string {
  if (registNo.length !== 6)
    throw new Error(`regist_no는 정확히 6자여야 합니다: "${registNo}" (${registNo.length}자)`);
  return `${registNo}${String(stallTyCode).padStart(2, "0")}${String(stallNo).padStart(2, "0")}${String(roomNo).padStart(2, "0")}`;
}

function buildKey9(
  registNo: string,
  species: string,
  stallTyCode: number,
  stallNo: number,
  roomNo: number
): Uint8Array {
  if (registNo.length !== 6) throw new Error("regist_no는 6자여야 합니다.");
  const speciesBits = SPECIES_MAP[species.toUpperCase()];
  if (speciesBits === undefined) throw new Error(`알 수 없는 축종: ${species}`);
  const stallTyNorm = stallTyCode - 1;
  const b6 = ((speciesBits & 0x7) << 5) | ((stallTyNorm & 0xf) << 1);
  const regBytes = new TextEncoder().encode(registNo);
  return concat(regBytes, [b6, stallNo & 0xff, roomNo & 0xff]);
}

function packTime4(
  year: number,
  month: number,
  day: number,
  hour: number,
  minute: number
): Uint8Array {
  const yy = year % 100;
  const packed =
    ((yy & 0x7f) << 20) |
    (((month - 1) & 0xf) << 16) |
    (((day - 1) & 0x1f) << 11) |
    ((hour & 0x1f) << 6) |
    (minute & 0x3f);
  return new Uint8Array([
    (packed >> 24) & 0xff,
    (packed >> 16) & 0xff,
    (packed >> 8) & 0xff,
    packed & 0xff,
  ]);
}

function packVal17(value: number): Uint8Array {
  const v = value & 0x1ffff;
  return new Uint8Array([(v >> 16) & 0x01, (v >> 8) & 0xff, v & 0xff]);
}

function packBlock(
  eqCode: string,
  valueSetLen: number,
  instanceCount: number,
  values: number[]
): Uint8Array {
  const prefix = eqCode.slice(0, 2).toUpperCase();
  const type3 = EQ_TYPE_MAP[prefix];
  if (type3 === undefined) throw new Error(`알 수 없는 장비 접두사: ${prefix}`);
  const noRaw = parseInt(eqCode.slice(2)) - 1;
  const eqpmn = ((type3 & 0x7) << 5) | ((noRaw & 0xf) << 1);
  const parts: Uint8Array[] = [new Uint8Array([eqpmn, valueSetLen & 0xff, instanceCount & 0xff])];
  for (const v of values) parts.push(packVal17(v));
  return concat(...parts);
}

type BlockInput =
  | [string, number]
  | { eq_code: string; values: number[]; value_set_len?: number; instance_count?: number };

function normalizeBlock(block: BlockInput): {
  eq_code: string;
  values: number[];
  value_set_len: number;
  instance_count: number;
} {
  if (Array.isArray(block)) {
    return { eq_code: String(block[0]), values: [Number(block[1])], value_set_len: 1, instance_count: 1 };
  }
  return {
    eq_code: block.eq_code,
    values: block.values,
    value_set_len: block.value_set_len ?? 1,
    instance_count: block.instance_count ?? 1,
  };
}

function buildStateBinary(
  registNo: string,
  species: string,
  stallTyCode: number,
  stallNo: number,
  roomNo: number,
  year: number,
  month: number,
  day: number,
  hour: number,
  minute: number,
  blocks: BlockInput[]
): Uint8Array {
  const normalized = blocks.map(normalizeBlock);
  const blockHeader = ((2 & 0x7) << 5) | ((normalized.length & 0xf) << 1);
  const parts: Uint8Array[] = [
    buildKey9(registNo, species, stallTyCode, stallNo, roomNo),
    packTime4(year, month, day, hour, minute),
    new Uint8Array([blockHeader]),
    ...normalized.map((nb) =>
      packBlock(nb.eq_code, nb.value_set_len, nb.instance_count, nb.values)
    ),
  ];
  return concat(...parts);
}

function buildStateBase64(stateBinary: Uint8Array): string {
  return btoa(Array.from(stateBinary, (b) => String.fromCharCode(b)).join(""));
}

function buildMqttPayload(refSeq: number, stateBinary: Uint8Array): string {
  return `SEQ:${refSeq},B64:${buildStateBase64(stateBinary)}`;
}

function buildMqttConnectHex(
  clientId: string,
  keepAlive = 60,
  username?: string | null,
  password?: string | null,
  cleanSession = true
): string {
  if (password && !username) throw new Error("username 없이 password만 설정할 수 없습니다.");
  const usernameFlag = username ? 1 : 0;
  const passwordFlag = password ? 1 : 0;
  const connectFlags = (usernameFlag << 7) | (passwordFlag << 6) | (Number(cleanSession) << 1);
  const varHeader = new Uint8Array([
    0x00, 0x04, 0x4d, 0x51, 0x54, 0x54, 0x04, connectFlags,
    (keepAlive >> 8) & 0xff, keepAlive & 0xff,
  ]);
  const payloadParts: Uint8Array[] = [mqttString(clientId)];
  if (username) payloadParts.push(mqttString(username));
  if (password) payloadParts.push(mqttString(password));
  const remaining = concat(varHeader, ...payloadParts);
  const packet = concat(
    [0x10],
    encodeRemainingLength(remaining.length),
    remaining
  );
  return toHex(packet);
}

function buildMqttPublishHex(
  topic: string,
  payloadText: string,
  packetId = 1,
  qos = 1,
  retain = false,
  dup = false
): string {
  const fixedByte = 0x30 | (Number(dup) << 3) | (qos << 1) | Number(retain);
  const topicBytes = mqttString(topic);
  const varHeaderParts: Uint8Array[] = [topicBytes];
  if (qos > 0) varHeaderParts.push(new Uint8Array([(packetId >> 8) & 0xff, packetId & 0xff]));
  const payloadBytes = new TextEncoder().encode(payloadText);
  const remaining = concat(...varHeaderParts, payloadBytes);
  const packet = concat(
    [fixedByte],
    encodeRemainingLength(remaining.length),
    remaining
  );
  return toHex(packet);
}

// ============================================================
// Route Handler
// ============================================================
export async function POST(req: NextRequest) {
  let body: Record<string, unknown>;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "JSON 파싱 실패" }, { status: 400 });
  }

  try {
    const registNo     = String(body.regist_no ?? "");
    const species      = String(body.species ?? "P");
    const stallTyCode  = Number(body.stall_ty_code ?? 1);
    const stallNo      = Number(body.stall_no ?? 1);
    const roomNo       = Number(body.room_no ?? 1);
    const refSeq       = Number(body.ref_seq ?? 1);
    const year         = Number(body.year ?? 2025);
    const month        = Number(body.month ?? 1);
    const day          = Number(body.day ?? 1);
    const hour         = Number(body.hour ?? 0);
    const minute       = Number(body.minute ?? 0);
    const blocks       = (body.blocks as BlockInput[]) ?? [];
    const keepAlive    = Number(body.keep_alive ?? 60);
    const username     = (body.username as string | null) || null;
    const password     = (body.password as string | null) || null;
    const cleanSession = body.clean_session !== false;

    const key12      = buildKey12(registNo, stallTyCode, stallNo, roomNo);
    const clientId   = `cm_${key12}`;
    const topic      = `/KEY12/${key12}`;
    const stateBin   = buildStateBinary(registNo, species, stallTyCode, stallNo, roomNo, year, month, day, hour, minute, blocks);
    const stateHex   = toHex(stateBin);
    const stateBase64 = buildStateBase64(stateBin);
    const mqttPayload = buildMqttPayload(refSeq, stateBin);
    const connectHex  = buildMqttConnectHex(clientId, keepAlive, username, password, cleanSession);
    const publishHex  = buildMqttPublishHex(topic, mqttPayload);

    return NextResponse.json({
      key12,
      connect_hex:      connectHex,
      expected_connack: "20 02 00 00",
      topic,
      state_hex:        stateHex,
      state_base64:     stateBase64,
      mqtt_payload:     mqttPayload,
      publish_hex:      publishHex,
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json({ error: msg }, { status: 400 });
  }
}
