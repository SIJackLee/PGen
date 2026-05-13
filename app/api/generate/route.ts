import { NextRequest, NextResponse } from "next/server";
import {
  DEFAULT_JSON_FORM,
  EqpmnName,
  JsonRoomStateForm,
  createEqpmnRow,
  generateJsonRoomStateOutput,
  validateJsonRoomStateForm,
} from "@/lib/json-room-state";

function toForm(body: Record<string, unknown>): JsonRoomStateForm {
  const eqpmnInput = Array.isArray(body.eqpmn) ? body.eqpmn : DEFAULT_JSON_FORM.eqpmn;

  return {
    broker_ip: String(body.broker_ip ?? DEFAULT_JSON_FORM.broker_ip),
    broker_port: String(body.broker_port ?? DEFAULT_JSON_FORM.broker_port),
    isnd_regist_no: String(body.isnd_regist_no ?? DEFAULT_JSON_FORM.isnd_regist_no),
    measure_ts: String(body.measure_ts ?? DEFAULT_JSON_FORM.measure_ts),
    species: String(body.species ?? DEFAULT_JSON_FORM.species) as JsonRoomStateForm["species"],
    stall_ty_code: String(body.stall_ty_code ?? DEFAULT_JSON_FORM.stall_ty_code),
    stall_no: String(body.stall_no ?? DEFAULT_JSON_FORM.stall_no),
    room_no: String(body.room_no ?? DEFAULT_JSON_FORM.room_no),
    eqpmn: eqpmnInput.map((row, index) => {
      const normalizedRow = row as { name?: string; values?: unknown };
      const values = Array.isArray(normalizedRow.values) ? normalizedRow.values.join(",") : "0";
      return {
        ...createEqpmnRow(
          (normalizedRow.name as EqpmnName) ?? DEFAULT_JSON_FORM.eqpmn[0].name,
          values
        ),
        id: `api-row-${index}`,
      };
    }),
  };
}

export async function POST(request: NextRequest) {
  let body: Record<string, unknown>;

  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "JSON 요청 본문이 올바르지 않습니다." }, { status: 400 });
  }

  const form = toForm(body);
  const validation = validateJsonRoomStateForm(form);

  if (validation.errors.length > 0) {
    return NextResponse.json(
      {
        error: validation.errors[0].message,
        errors: validation.errors,
        warnings: validation.warnings,
      },
      { status: 400 }
    );
  }

  return NextResponse.json(generateJsonRoomStateOutput(form));
}
