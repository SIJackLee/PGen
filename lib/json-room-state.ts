export const JSON_PROTOCOL_NAME = "JSON Room State Protocol v2";

export const EQPMN_NAME_OPTIONS = [
  "es01",
  "es02",
  "es03",
  "es04",
  "es09",
  "ec01",
  "ec02",
  "ec03",
] as const;

export type EqpmnName = (typeof EQPMN_NAME_OPTIONS)[number];

export const EQPMN_NAME_LABELS: Record<EqpmnName, string> = {
  es01: "온도",
  es02: "습도",
  es03: "CO2",
  es04: "암모니아",
  es09: "음압센서",
  ec01: "송풍팬",
  ec02: "입기팬",
  ec03: "배기팬",
};

export const SPECIES_OPTIONS = [
  { value: "W", label: "W - 한우" },
  { value: "D", label: "D - 낙농" },
  { value: "P", label: "P - 양돈" },
  { value: "H", label: "H - 양계" },
  { value: "T", label: "T - 오리" },
  { value: "R", label: "R - 사슴" },
  { value: "I", label: "I - 곤충" },
  { value: "B", label: "B - 양봉" },
] as const;

export const STALL_TYPE_OPTIONS = [
  { value: "SW01", label: "SW01 - 우사" },
  { value: "SW02", label: "SW02 - 착유장" },
  { value: "SW03", label: "SW03 - 퇴비장(분뇨처리장)" },
  { value: "SD01", label: "SD01 - 우사" },
  { value: "SD02", label: "SD02 - 착유장" },
  { value: "SD03", label: "SD03 - 퇴비장(분뇨처리장)" },
  { value: "SP01", label: "SP01 - 후보돈사" },
  { value: "SP02", label: "SP02 - 임신사" },
  { value: "SP03", label: "SP03 - 분만사" },
  { value: "SP04", label: "SP04 - 베이비하우스" },
  { value: "SP05", label: "SP05 - 자돈사" },
  { value: "SP06", label: "SP06 - 육성사" },
  { value: "SP07", label: "SP07 - 비육사" },
  { value: "SP08", label: "SP08 - 검정사" },
  { value: "SP09", label: "SP09 - 종부사" },
  { value: "SP10", label: "SP10 - 퇴비장(분뇨처리장)" },
  { value: "SH01", label: "SH01 - 산란육성계사" },
  { value: "SH02", label: "SH02 - 산란계사" },
  { value: "SH03", label: "SH03 - 종계육성계사" },
  { value: "SH04", label: "SH04 - 종계사" },
  { value: "SH05", label: "SH05 - 육계사" },
  { value: "SH06", label: "SH06 - 선별장" },
  { value: "SH07", label: "SH07 - 계사(공통)" },
  { value: "SH08", label: "SH08 - 퇴비장(분뇨처리장)" },
  { value: "ST01", label: "ST01 - 종오리사" },
  { value: "ST02", label: "ST02 - 육용오리사" },
  { value: "SR01", label: "SR01 - 사육장" },
  { value: "SI01", label: "SI01 - 곤충사육사" },
  { value: "SB01", label: "SB01 - 양봉장" },
] as const;

export type SpeciesCode = (typeof SPECIES_OPTIONS)[number]["value"];
export type StallTypeCode = (typeof STALL_TYPE_OPTIONS)[number]["value"];

export interface EqpmnRow {
  id: string;
  name: EqpmnName;
  valuesText: string;
}

export interface JsonRoomStateForm {
  broker_ip: string;
  broker_port: string;
  isnd_regist_no: string;
  measure_ts: string;
  species: SpeciesCode;
  stall_ty_code: string;
  stall_no: string;
  room_no: string;
  eqpmn: EqpmnRow[];
}

export interface JsonRoomStatePayload {
  ver: 2;
  typ: "room_state";
  measure_ts: string;
  species: SpeciesCode;
  stall_ty_code: StallTypeCode;
  stall_no: number;
  room_no: number;
  eqpmn: Array<{
    name: EqpmnName;
    values: number[];
  }>;
}

export interface JsonRoomStateOutput {
  protocol_name: string;
  state_topic: string;
  ack_topic: string;
  payload: JsonRoomStatePayload;
  payload_pretty: string;
  payload_compact: string;
  mqtt_client_id: string;
  raw_mqtt_connect_hex: string;
  raw_mqtt_publish_hex: string;
  mosquitto_pub_command: string;
  mosquitto_sub_ack_command: string;
  warnings: string[];
}

export interface ValidationIssue {
  field: string;
  message: string;
}

export interface ValidationResult {
  errors: ValidationIssue[];
  warnings: ValidationIssue[];
}

export const DEFAULT_EQPMN_ROWS: EqpmnRow[] = [
  { id: "eq-1", name: "es01", valuesText: "253,254,252" },
  { id: "eq-2", name: "es02", valuesText: "612,610" },
  { id: "eq-3", name: "ec01", valuesText: "1,1,0,1" },
  { id: "eq-4", name: "ec02", valuesText: "45,50" },
];

export const DEFAULT_JSON_FORM: JsonRoomStateForm = {
  broker_ip: "54.116.16.1",
  broker_port: "1883",
  isnd_regist_no: "FARM01",
  measure_ts: "2026-05-13T08:30:00",
  species: "P",
  stall_ty_code: "SP01",
  stall_no: "1",
  room_no: "1",
  eqpmn: DEFAULT_EQPMN_ROWS,
};

export function createEqpmnRow(name: EqpmnName = "es01", valuesText = ""): EqpmnRow {
  return {
    id: `${name}-${Math.random().toString(36).slice(2, 10)}`,
    name,
    valuesText,
  };
}

function parseInteger(field: string, raw: string, errors: ValidationIssue[]): number {
  const value = Number(raw);
  if (!Number.isInteger(value)) {
    errors.push({ field, message: `${field} 값은 정수여야 합니다.` });
  }
  return value;
}

function parseValues(valuesText: string): number[] {
  return valuesText
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean)
    .map(Number);
}

export function validateJsonRoomStateForm(form: JsonRoomStateForm): ValidationResult {
  const errors: ValidationIssue[] = [];
  const warnings: ValidationIssue[] = [];

  if (!form.isnd_regist_no.trim()) {
    errors.push({ field: "isnd_regist_no", message: "축산업등록번호는 비어 있을 수 없습니다." });
  }

  if (!form.measure_ts.trim()) {
    errors.push({ field: "measure_ts", message: "측정 시각은 비어 있을 수 없습니다." });
  }

  if (!form.broker_ip.trim()) {
    errors.push({ field: "broker_ip", message: "브로커 IP는 비어 있을 수 없습니다." });
  }

  parseInteger("broker_port", form.broker_port, errors);

  if (!SPECIES_OPTIONS.some((option) => option.value === form.species)) {
    errors.push({ field: "species", message: "축종 코드가 허용 목록에 없습니다." });
  }

  if (!STALL_TYPE_OPTIONS.some((option) => option.value === form.stall_ty_code)) {
    errors.push({ field: "stall_ty_code", message: "축사 유형 코드가 허용 목록에 없습니다." });
  }

  parseInteger("stall_no", form.stall_no, errors);
  parseInteger("room_no", form.room_no, errors);

  if (form.eqpmn.length < 1) {
    errors.push({ field: "eqpmn", message: "장비 목록은 1개 이상이어야 합니다." });
  }

  const seenNames = new Map<string, number>();

  form.eqpmn.forEach((row, index) => {
    if (!EQPMN_NAME_OPTIONS.includes(row.name)) {
      errors.push({
        field: `eqpmn[${index}].name`,
        message: `eqpmn[${index}].name 값이 허용 목록에 없습니다.`,
      });
    }

    const values = parseValues(row.valuesText);
    if (values.length < 1 || values.some((value) => !Number.isFinite(value))) {
      errors.push({
        field: `eqpmn[${index}].values`,
        message: `eqpmn[${index}].values 값은 숫자 배열이어야 합니다.`,
      });
    }

    const count = seenNames.get(row.name) ?? 0;
    seenNames.set(row.name, count + 1);
  });

  seenNames.forEach((count, name) => {
    if (count > 1) {
      warnings.push({
        field: "eqpmn",
        message: `장비명 ${name} 이(가) 중복되었습니다. 가능하면 values를 하나의 행으로 합치세요.`,
      });
    }
  });

  return { errors, warnings };
}

export function buildJsonRoomStatePayload(form: JsonRoomStateForm): JsonRoomStatePayload {
  return {
    ver: 2,
    typ: "room_state",
    measure_ts: form.measure_ts.trim(),
    species: form.species,
    stall_ty_code: form.stall_ty_code as StallTypeCode,
    stall_no: Number(form.stall_no),
    room_no: Number(form.room_no),
    eqpmn: form.eqpmn.map((row) => ({
      name: row.name,
      values: parseValues(row.valuesText),
    })),
  };
}

export function buildStateTopic(isndRegistNo: string): string {
  return `/${isndRegistNo}/STATE_JSON`;
}

export function buildAckTopic(isndRegistNo: string): string {
  return `/${isndRegistNo}/ACK_JSON`;
}

export function buildMosquittoPubCommand(stateTopic: string, payloadCompact: string): string {
  const escapedPayload = payloadCompact.replace(/'/g, `'\"'\"'`);
  return `mosquitto_pub -h 127.0.0.1 -p 1883 -t '${stateTopic}' -m '${escapedPayload}'`;
}

export function buildMosquittoSubAckCommand(ackTopic: string): string {
  return `mosquitto_sub -h 127.0.0.1 -p 1883 -t '${ackTopic}' -v`;
}

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

export function buildMqttClientId(form: JsonRoomStateForm): string {
  return `json_${form.isnd_regist_no}_${form.stall_ty_code}_${form.stall_no}_${form.room_no}`;
}

export function buildRawMqttConnectHex(clientId: string, keepAlive = 60): string {
  const variableHeader = new Uint8Array([
    0x00,
    0x04,
    0x4d,
    0x51,
    0x54,
    0x54,
    0x04,
    0x02,
    (keepAlive >> 8) & 0xff,
    keepAlive & 0xff,
  ]);
  const payload = mqttString(clientId);
  const remaining = concat(variableHeader, payload);
  const packet = concat([0x10], encodeRemainingLength(remaining.length), remaining);
  return toHex(packet);
}

export function buildRawMqttPublishHex(topic: string, payloadText: string, packetId = 1): string {
  const topicBytes = mqttString(topic);
  const packetIdBytes = new Uint8Array([(packetId >> 8) & 0xff, packetId & 0xff]);
  const payloadBytes = new TextEncoder().encode(payloadText);
  const remaining = concat(topicBytes, packetIdBytes, payloadBytes);
  const packet = concat([0x32], encodeRemainingLength(remaining.length), remaining);
  return toHex(packet);
}

export function generateJsonRoomStateOutput(form: JsonRoomStateForm): JsonRoomStateOutput {
  const validation = validateJsonRoomStateForm(form);
  if (validation.errors.length > 0) {
    throw new Error(validation.errors[0].message);
  }

  const trimmedRegistNo = form.isnd_regist_no.trim();
  const payload = buildJsonRoomStatePayload({
    ...form,
    isnd_regist_no: trimmedRegistNo,
  });
  const payloadPretty = JSON.stringify(payload, null, 2);
  const payloadCompact = JSON.stringify(payload);
  const stateTopic = buildStateTopic(trimmedRegistNo);
  const ackTopic = buildAckTopic(trimmedRegistNo);
  const mqttClientId = buildMqttClientId(form);
  const rawMqttConnectHex = buildRawMqttConnectHex(mqttClientId);
  const rawMqttPublishHex = buildRawMqttPublishHex(stateTopic, payloadCompact);

  return {
    protocol_name: JSON_PROTOCOL_NAME,
    state_topic: stateTopic,
    ack_topic: ackTopic,
    payload,
    payload_pretty: payloadPretty,
    payload_compact: payloadCompact,
    mqtt_client_id: mqttClientId,
    raw_mqtt_connect_hex: rawMqttConnectHex,
    raw_mqtt_publish_hex: rawMqttPublishHex,
    mosquitto_pub_command: buildMosquittoPubCommand(stateTopic, payloadCompact),
    mosquitto_sub_ack_command: buildMosquittoSubAckCommand(ackTopic),
    warnings: validation.warnings.map((warning) => warning.message),
  };
}

export function nowIsoWithOffset(date = new Date()): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hour = String(date.getHours()).padStart(2, "0");
  const minute = String(date.getMinutes()).padStart(2, "0");
  const second = String(date.getSeconds()).padStart(2, "0");

  return `${year}-${month}-${day}T${hour}:${minute}:${second}`;
}
