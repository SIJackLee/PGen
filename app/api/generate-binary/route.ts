import { NextRequest, NextResponse } from "next/server";

const SPECIES_MAP: Record<string, number> = {
  W: 0,
  D: 1,
  P: 2,
  H: 3,
  T: 4,
  R: 5,
  I: 6,
  B: 7,
};

const EQ_TYPE_MAP: Record<string, number> = {
  ES: 0,
  EC: 1,
  PC: 2,
  BI: 3,
  AF: 4,
  SF: 5,
  VI: 6,
};

function mqttString(value: string): Uint8Array {
  const encoded = new TextEncoder().encode(value);
  const result = new Uint8Array(2 + encoded.length);
  result[0] = (encoded.length >> 8) & 0xff;
  result[1] = encoded.length & 0xff;
  result.set(encoded, 2);
  return result;
}

function encodeRemainingLength(value: number): number[] {
  const bytes: number[] = [];
  do {
    let byte = value & 0x7f;
    value >>= 7;
    if (value > 0) byte |= 0x80;
    bytes.push(byte);
  } while (value > 0);
  return bytes;
}

function concat(...arrays: (Uint8Array | number[])[]): Uint8Array {
  const total = arrays.reduce((sum, array) => sum + array.length, 0);
  const result = new Uint8Array(total);
  let offset = 0;

  for (const array of arrays) {
    result.set(array instanceof Uint8Array ? array : new Uint8Array(array), offset);
    offset += array.length;
  }

  return result;
}

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((byte) => byte.toString(16).padStart(2, "0").toUpperCase())
    .join(" ");
}

function toAscii(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => (byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : ".")).join("");
}

function buildKey12(registNo: string, stallTyCode: number, stallNo: number, roomNo: number): string {
  if (registNo.length !== 6) {
    throw new Error(`regist_no must be exactly 6 characters. Received "${registNo}".`);
  }

  return `${registNo}${String(stallTyCode).padStart(2, "0")}${String(stallNo).padStart(2, "0")}${String(roomNo).padStart(2, "0")}`;
}

function buildKey9(
  registNo: string,
  species: string,
  stallTyCode: number,
  stallNo: number,
  roomNo: number
): Uint8Array {
  if (registNo.length !== 6) {
    throw new Error("regist_no must be exactly 6 characters.");
  }

  const speciesBits = SPECIES_MAP[species.toUpperCase()];
  if (speciesBits === undefined) {
    throw new Error(`Unknown species: ${species}`);
  }

  const stallTyNorm = stallTyCode - 1;
  const encodedRegistNo = new TextEncoder().encode(registNo);
  const b6 = ((speciesBits & 0x7) << 5) | ((stallTyNorm & 0xf) << 1);

  return concat(encodedRegistNo, [b6, stallNo & 0xff, roomNo & 0xff]);
}

function packTime4(year: number, month: number, day: number, hour: number, minute: number): Uint8Array {
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
  const normalized = value & 0x1ffff;
  return new Uint8Array([(normalized >> 16) & 0x01, (normalized >> 8) & 0xff, normalized & 0xff]);
}

function packBlock(eqCode: string, valueSetLen: number, instanceCount: number, values: number[]): Uint8Array {
  const prefix = eqCode.slice(0, 2).toUpperCase();
  const type3 = EQ_TYPE_MAP[prefix];
  if (type3 === undefined) {
    throw new Error(`Unknown equipment prefix: ${prefix}`);
  }

  const noRaw = parseInt(eqCode.slice(2), 10) - 1;
  const eqpmn = ((type3 & 0x7) << 5) | ((noRaw & 0xf) << 1);
  const parts: Uint8Array[] = [new Uint8Array([eqpmn, valueSetLen & 0xff, instanceCount & 0xff])];

  for (const value of values) {
    parts.push(packVal17(value));
  }

  return concat(...parts);
}

type BlockInput =
  | [string, number]
  | { eq_code: string; values: number[]; value_set_len?: number; instance_count?: number };

function normalizeBlock(block: BlockInput) {
  if (Array.isArray(block)) {
    return {
      eq_code: String(block[0]),
      values: [Number(block[1])],
      value_set_len: 1,
      instance_count: 1,
    };
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
  const normalizedBlocks = blocks.map(normalizeBlock);
  const blockHeader = ((2 & 0x7) << 5) | ((normalizedBlocks.length & 0xf) << 1);

  return concat(
    buildKey9(registNo, species, stallTyCode, stallNo, roomNo),
    packTime4(year, month, day, hour, minute),
    new Uint8Array([blockHeader]),
    ...normalizedBlocks.map((block) =>
      packBlock(block.eq_code, block.value_set_len, block.instance_count, block.values)
    )
  );
}

function buildStateBase64(stateBinary: Uint8Array): string {
  return btoa(Array.from(stateBinary, (byte) => String.fromCharCode(byte)).join(""));
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
  if (password && !username) {
    throw new Error("username is required when password is provided.");
  }

  return toHex(buildMqttConnectPacket(clientId, keepAlive, username, password, cleanSession));
}

function buildMqttConnectAscii(
  clientId: string,
  keepAlive = 60,
  username?: string | null,
  password?: string | null,
  cleanSession = true
): string {
  if (password && !username) {
    throw new Error("username is required when password is provided.");
  }

  return toAscii(buildMqttConnectPacket(clientId, keepAlive, username, password, cleanSession));
}

function buildMqttConnectPacket(
  clientId: string,
  keepAlive = 60,
  username?: string | null,
  password?: string | null,
  cleanSession = true
): Uint8Array {
  if (password && !username) {
    throw new Error("username is required when password is provided.");
  }

  const usernameFlag = username ? 1 : 0;
  const passwordFlag = password ? 1 : 0;
  const connectFlags = (usernameFlag << 7) | (passwordFlag << 6) | (Number(cleanSession) << 1);
  const variableHeader = new Uint8Array([
    0x00,
    0x04,
    0x4d,
    0x51,
    0x54,
    0x54,
    0x04,
    connectFlags,
    (keepAlive >> 8) & 0xff,
    keepAlive & 0xff,
  ]);
  const payloadParts: Uint8Array[] = [mqttString(clientId)];

  if (username) payloadParts.push(mqttString(username));
  if (password) payloadParts.push(mqttString(password));

  const remaining = concat(variableHeader, ...payloadParts);
  return concat([0x10], encodeRemainingLength(remaining.length), remaining);
}

function buildMqttPublishHex(
  topic: string,
  payloadText: string,
  packetId = 1,
  qos = 1,
  retain = false,
  dup = false
): string {
  return toHex(buildMqttPublishPacket(topic, payloadText, packetId, qos, retain, dup));
}

function buildMqttPublishAscii(
  topic: string,
  payloadText: string,
  packetId = 1,
  qos = 1,
  retain = false,
  dup = false
): string {
  return toAscii(buildMqttPublishPacket(topic, payloadText, packetId, qos, retain, dup));
}

function buildMqttPublishPacket(
  topic: string,
  payloadText: string,
  packetId = 1,
  qos = 1,
  retain = false,
  dup = false
): Uint8Array {
  const fixedByte = 0x30 | (Number(dup) << 3) | (qos << 1) | Number(retain);
  const topicBytes = mqttString(topic);
  const variableHeaderParts: Uint8Array[] = [topicBytes];

  if (qos > 0) {
    variableHeaderParts.push(new Uint8Array([(packetId >> 8) & 0xff, packetId & 0xff]));
  }

  const payloadBytes = new TextEncoder().encode(payloadText);
  const remaining = concat(...variableHeaderParts, payloadBytes);
  return concat([fixedByte], encodeRemainingLength(remaining.length), remaining);
}

export async function POST(request: NextRequest) {
  let body: Record<string, unknown>;

  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON body." }, { status: 400 });
  }

  try {
    const registNo = String(body.regist_no ?? "");
    const species = String(body.species ?? "P");
    const stallTyCode = Number(body.stall_ty_code ?? 1);
    const stallNo = Number(body.stall_no ?? 1);
    const roomNo = Number(body.room_no ?? 1);
    const refSeq = Number(body.ref_seq ?? 1);
    const year = Number(body.year ?? 2025);
    const month = Number(body.month ?? 1);
    const day = Number(body.day ?? 1);
    const hour = Number(body.hour ?? 0);
    const minute = Number(body.minute ?? 0);
    const blocks = (body.blocks as BlockInput[]) ?? [];
    const keepAlive = Number(body.keep_alive ?? 60);
    const username = (body.username as string | null) || null;
    const password = (body.password as string | null) || null;
    const cleanSession = body.clean_session !== false;

    const key12 = buildKey12(registNo, stallTyCode, stallNo, roomNo);
    const clientId = `cm_${key12}`;
    const topic = `/KEY12/${key12}`;
    const stateBin = buildStateBinary(
      registNo,
      species,
      stallTyCode,
      stallNo,
      roomNo,
      year,
      month,
      day,
      hour,
      minute,
      blocks
    );

    return NextResponse.json({
      key12,
      connect_hex: buildMqttConnectHex(clientId, keepAlive, username, password, cleanSession),
      connect_ascii: buildMqttConnectAscii(clientId, keepAlive, username, password, cleanSession),
      expected_connack: "20 02 00 00",
      topic,
      state_hex: toHex(stateBin),
      state_ascii: toAscii(stateBin),
      state_base64: buildStateBase64(stateBin),
      mqtt_payload: buildMqttPayload(refSeq, stateBin),
      publish_hex: buildMqttPublishHex(topic, buildMqttPayload(refSeq, stateBin)),
      publish_ascii: buildMqttPublishAscii(topic, buildMqttPayload(refSeq, stateBin)),
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
