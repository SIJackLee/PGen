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

export interface EqpmnRow {
  id: string;
  name: EqpmnName;
  valuesText: string;
}

export interface JsonRoomStateForm {
  isnd_regist_no: string;
  seq: string;
  measure_ts: string;
  species: string;
  stall_ty_code: string;
  stall_no: string;
  room_no: string;
  eqpmn: EqpmnRow[];
}

export interface JsonRoomStatePayload {
  ver: 2;
  typ: "room_state";
  seq: number;
  measure_ts: string;
  species: number;
  stall_ty_code: number;
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
  isnd_regist_no: "FARM01",
  seq: "1001",
  measure_ts: "2026-05-13T08:30:00+09:00",
  species: "1",
  stall_ty_code: "2",
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
    errors.push({ field, message: `${field} must be an integer.` });
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
    errors.push({ field: "isnd_regist_no", message: "isnd_regist_no is required." });
  }

  if (!form.measure_ts.trim()) {
    errors.push({ field: "measure_ts", message: "measure_ts is required." });
  }

  parseInteger("seq", form.seq, errors);
  parseInteger("species", form.species, errors);
  parseInteger("stall_ty_code", form.stall_ty_code, errors);
  parseInteger("stall_no", form.stall_no, errors);
  parseInteger("room_no", form.room_no, errors);

  if (form.eqpmn.length < 1) {
    errors.push({ field: "eqpmn", message: "eqpmn must contain at least one row." });
  }

  const seenNames = new Map<string, number>();

  form.eqpmn.forEach((row, index) => {
    if (!EQPMN_NAME_OPTIONS.includes(row.name)) {
      errors.push({
        field: `eqpmn[${index}].name`,
        message: `eqpmn[${index}].name must be one of the allowed values.`,
      });
    }

    const values = parseValues(row.valuesText);
    if (values.length < 1 || values.some((value) => !Number.isFinite(value))) {
      errors.push({
        field: `eqpmn[${index}].values`,
        message: `eqpmn[${index}].values must be a numeric array.`,
      });
    }

    const count = seenNames.get(row.name) ?? 0;
    seenNames.set(row.name, count + 1);
  });

  seenNames.forEach((count, name) => {
    if (count > 1) {
      warnings.push({
        field: "eqpmn",
        message: `Duplicate eqpmn.name "${name}" detected. Merge values into one row when possible.`,
      });
    }
  });

  return { errors, warnings };
}

export function buildJsonRoomStatePayload(form: JsonRoomStateForm): JsonRoomStatePayload {
  return {
    ver: 2,
    typ: "room_state",
    seq: Number(form.seq),
    measure_ts: form.measure_ts.trim(),
    species: Number(form.species),
    stall_ty_code: Number(form.stall_ty_code),
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

  return {
    protocol_name: JSON_PROTOCOL_NAME,
    state_topic: stateTopic,
    ack_topic: ackTopic,
    payload,
    payload_pretty: payloadPretty,
    payload_compact: payloadCompact,
    mosquitto_pub_command: buildMosquittoPubCommand(stateTopic, payloadCompact),
    mosquitto_sub_ack_command: buildMosquittoSubAckCommand(ackTopic),
    warnings: validation.warnings.map((warning) => warning.message),
  };
}

export function nowIsoWithOffset(date = new Date()): string {
  const offsetMinutes = -date.getTimezoneOffset();
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const absMinutes = Math.abs(offsetMinutes);
  const hours = String(Math.floor(absMinutes / 60)).padStart(2, "0");
  const minutes = String(absMinutes % 60).padStart(2, "0");

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hour = String(date.getHours()).padStart(2, "0");
  const minute = String(date.getMinutes()).padStart(2, "0");
  const second = String(date.getSeconds()).padStart(2, "0");

  return `${year}-${month}-${day}T${hour}:${minute}:${second}${sign}${hours}:${minutes}`;
}
