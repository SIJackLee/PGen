"use client";

import { useEffect, useMemo, useState } from "react";
import {
  DEFAULT_JSON_FORM,
  EQPMN_NAME_LABELS,
  EQPMN_NAME_OPTIONS,
  JsonRoomStateForm,
  JsonRoomStateOutput,
  SPECIES_OPTIONS,
  STALL_TYPE_OPTIONS,
  createEqpmnRow,
  generateJsonRoomStateOutput,
  nowIsoWithOffset,
  validateJsonRoomStateForm,
} from "@/lib/json-room-state";

const STORAGE_KEY = "protocol-json-room-state-v2";

function CopyButton({ label, value }: { label: string; value: string }) {
  const [copied, setCopied] = useState(false);

  return (
    <button
      type="button"
      onClick={() => {
        navigator.clipboard.writeText(value).then(() => {
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1200);
        });
      }}
      className="rounded-md border border-slate-600 px-3 py-2 text-sm text-slate-200 transition hover:border-cyan-400 hover:text-cyan-200"
    >
      {copied ? "복사됨" : label}
    </button>
  );
}

function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <label className="flex flex-col gap-2">
      <span className="text-sm font-medium text-slate-300">{label}</span>
      {children}
    </label>
  );
}

function OutputCard({
  title,
  value,
  copyLabel,
}: {
  title: string;
  value: string;
  copyLabel: string;
}) {
  return (
    <section className="rounded-2xl border border-slate-800 bg-slate-950/80 p-5 shadow-[0_0_0_1px_rgba(15,23,42,0.3)]">
      <div className="mb-3 flex items-center justify-between gap-3">
        <h3 className="text-sm font-semibold uppercase tracking-[0.2em] text-cyan-300">
          {title}
        </h3>
        <CopyButton label={copyLabel} value={value} />
      </div>
      <pre className="overflow-x-auto whitespace-pre-wrap break-all rounded-xl bg-slate-900 p-4 text-sm leading-6 text-slate-100">
        {value}
      </pre>
    </section>
  );
}

function FlowCard({
  step,
  sendLabel,
  sendValue,
  receiveLabel,
  receiveValue,
}: {
  step: string;
  sendLabel: string;
  sendValue: string;
  receiveLabel: string;
  receiveValue: string;
}) {
  return (
    <section className="rounded-2xl border border-slate-800 bg-slate-950/80 p-5 shadow-[0_0_0_1px_rgba(15,23,42,0.3)]">
      <p className="mb-4 text-sm font-semibold uppercase tracking-[0.2em] text-emerald-300">
        {step}
      </p>
      <div className="space-y-3">
        <div className="rounded-xl border border-cyan-900 bg-cyan-950/40 p-4">
          <p className="mb-2 text-sm font-medium text-cyan-200">보내는 데이터: {sendLabel}</p>
          <pre className="whitespace-pre-wrap break-all text-sm leading-6 text-slate-100">
            {sendValue}
          </pre>
        </div>
        <div className="rounded-xl border border-emerald-900 bg-emerald-950/30 p-4">
          <p className="mb-2 text-sm font-medium text-emerald-200">예상 수신 데이터: {receiveLabel}</p>
          <pre className="whitespace-pre-wrap break-all text-sm leading-6 text-slate-100">
            {receiveValue}
          </pre>
        </div>
      </div>
    </section>
  );
}

function SectionCard({
  title,
  subtitle,
  action,
  children,
}: {
  title: string;
  subtitle?: string;
  action?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-2xl border border-slate-800 bg-slate-950/80 p-5 shadow-[0_0_0_1px_rgba(15,23,42,0.3)]">
      <div className="mb-3 flex items-center justify-between gap-3">
        <h3 className="text-sm font-semibold uppercase tracking-[0.2em] text-cyan-300">
          {title}
        </h3>
        {action}
      </div>
      {subtitle ? (
        <p className="mb-3 text-sm leading-6 text-slate-400">{subtitle}</p>
      ) : null}
      {children}
    </section>
  );
}

export default function JsonProtocolGenerator() {
  const [form, setForm] = useState<JsonRoomStateForm>(DEFAULT_JSON_FORM);
  const [showCompact, setShowCompact] = useState(false);

  useEffect(() => {
    try {
      const saved = window.localStorage.getItem(STORAGE_KEY);
      if (!saved) return;
      const parsed = JSON.parse(saved) as JsonRoomStateForm;
      if (parsed?.eqpmn?.length) {
        setForm(parsed);
      }
    } catch {}
  }, []);

  useEffect(() => {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(form));
  }, [form]);

  const validation = useMemo(() => validateJsonRoomStateForm(form), [form]);

  const output = useMemo<JsonRoomStateOutput | null>(() => {
    if (validation.errors.length > 0) return null;
    try {
      return generateJsonRoomStateOutput(form);
    } catch {
      return null;
    }
  }, [form, validation.errors.length]);

  const payloadText = showCompact
    ? output?.payload_compact ?? ""
    : output?.payload_pretty ?? "";

  const flowSteps = output
    ? [
        {
          step: "1단계",
          sendLabel: "MQTT 연결 요청",
          sendValue: [
            `IP: ${form.broker_ip}`,
            `Port: ${form.broker_port}`,
            "Protocol: MQTT 3.1.1",
            "Clean Session: true",
            "Keep Alive: 60",
            "Client ID: 통신모듈 구현값 사용",
          ].join("\n"),
          receiveLabel: "CONNACK",
          receiveValue: "브로커가 MQTT 연결 수락 응답을 반환합니다.",
        },
        {
          step: "2단계",
          sendLabel: "MQTT 발행 준비",
          sendValue: [
            `Destination IP: ${form.broker_ip}`,
            `Destination Port: ${form.broker_port}`,
            `Publish Topic: ${output.state_topic}`,
            "QoS: 1 권장",
            "Retain: false",
          ].join("\n"),
          receiveLabel: "PUBACK 또는 브로커 응답",
          receiveValue: "QoS 1 사용 시 브로커에서 PUBACK 이 반환될 수 있습니다.",
        },
        {
          step: "3단계",
          sendLabel: "MQTT Payload 전송",
          sendValue: output.payload_compact,
          receiveLabel: "ACK_JSON 또는 수신 서버 응답",
          receiveValue: [
            `수신 확인 토픽: ${output.ack_topic}`,
            "수신 서버 구현에 따라 ACK JSON 메시지가 반환될 수 있습니다.",
          ].join("\n"),
        },
        {
          step: "4단계",
          sendLabel: "ACK 확인",
          sendValue: [
            "ACK 수신 대기",
            `Topic: ${output.ack_topic}`,
            "예시 명령:",
            output.mosquitto_sub_ack_command,
          ].join("\n"),
          receiveLabel: "ACK JSON Payload",
          receiveValue: `예상 ACK 토픽: ${output.ack_topic}`,
        },
      ]
    : [];

  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)]">
      <section className="rounded-[28px] border border-slate-800 bg-slate-950/90 p-6 shadow-[0_24px_80px_rgba(0,0,0,0.35)]">
        <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-2xl font-semibold text-white">JSON 프로토콜 생성기</h2>
            <p className="mt-1 text-sm text-slate-400">
              JSON Room State Protocol v2 페이로드와 MQTT 테스트 명령을 생성합니다.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() => setForm(DEFAULT_JSON_FORM)}
              className="rounded-md border border-slate-700 px-3 py-2 text-sm text-slate-300 transition hover:border-slate-500"
            >
              초기화
            </button>
            <button
              type="button"
              onClick={() =>
                setForm((current) => ({
                  ...current,
                  measure_ts: nowIsoWithOffset(),
                }))
              }
              className="rounded-md border border-cyan-700 px-3 py-2 text-sm text-cyan-200 transition hover:bg-cyan-950"
            >
              현재 시각 입력
            </button>
            <button
              type="button"
              onClick={() =>
                setForm((current) => ({
                  ...current,
                  eqpmn: [...current.eqpmn, createEqpmnRow("es01", "0")],
                }))
              }
              className="rounded-md border border-emerald-700 px-3 py-2 text-sm text-emerald-200 transition hover:bg-emerald-950"
            >
              장비 행 추가
            </button>
          </div>
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <Field label="브로커 IP">
            <input
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.broker_ip}
              onChange={(event) =>
                setForm((current) => ({ ...current, broker_ip: event.target.value }))
              }
            />
          </Field>
          <Field label="브로커 Port">
            <input
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.broker_port}
              onChange={(event) =>
                setForm((current) => ({ ...current, broker_port: event.target.value }))
              }
            />
          </Field>
          <Field label="축산업등록번호">
            <input
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.isnd_regist_no}
              onChange={(event) =>
                setForm((current) => ({ ...current, isnd_regist_no: event.target.value }))
              }
            />
          </Field>
          <Field label="측정 시각">
            <input
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.measure_ts}
              onChange={(event) =>
                setForm((current) => ({ ...current, measure_ts: event.target.value }))
              }
            />
          </Field>
          <Field label="축종">
            <select
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.species}
              onChange={(event) =>
                setForm((current) => ({ ...current, species: event.target.value as JsonRoomStateForm["species"] }))
              }
            >
              {SPECIES_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </Field>
          <Field label="축사 유형 코드">
            <select
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.stall_ty_code}
              onChange={(event) =>
                setForm((current) => ({ ...current, stall_ty_code: event.target.value }))
              }
            >
              {STALL_TYPE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </Field>
          <Field label="축사 번호">
            <input
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.stall_no}
              onChange={(event) =>
                setForm((current) => ({ ...current, stall_no: event.target.value }))
              }
            />
          </Field>
          <Field label="방 번호">
            <input
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.room_no}
              onChange={(event) =>
                setForm((current) => ({ ...current, room_no: event.target.value }))
              }
            />
          </Field>
        </div>

        <div className="mt-8">
          <div className="mb-3 flex items-center justify-between">
            <h3 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-300">
              장비 목록
            </h3>
            <button
              type="button"
              onClick={() =>
                setForm((current) => ({
                  ...current,
                  eqpmn: [...current.eqpmn, createEqpmnRow("es01", "0")],
                }))
              }
              className="rounded-md border border-slate-700 px-3 py-2 text-sm text-slate-200 transition hover:border-cyan-400 hover:text-cyan-200"
            >
              행 추가
            </button>
          </div>
          <div className="space-y-3">
            {form.eqpmn.map((row) => (
              <div
                key={row.id}
                className="grid gap-3 rounded-2xl border border-slate-800 bg-slate-900/70 p-4 md:grid-cols-[160px_minmax(0,1fr)_100px]"
              >
                <Field label="장비명">
                  <select
                    className="rounded-xl border border-slate-700 bg-slate-950 px-3 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
                    value={row.name}
                    onChange={(event) =>
                      setForm((current) => ({
                        ...current,
                        eqpmn: current.eqpmn.map((item) =>
                          item.id === row.id
                            ? { ...item, name: event.target.value as (typeof EQPMN_NAME_OPTIONS)[number] }
                            : item
                        ),
                      }))
                    }
                  >
                    {EQPMN_NAME_OPTIONS.map((name) => (
                      <option key={name} value={name}>
                        {name} - {EQPMN_NAME_LABELS[name]}
                      </option>
                    ))}
                  </select>
                </Field>
                <Field label="측정값 목록">
                  <input
                    className="rounded-xl border border-slate-700 bg-slate-950 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
                    value={row.valuesText}
                    onChange={(event) =>
                      setForm((current) => ({
                        ...current,
                        eqpmn: current.eqpmn.map((item) =>
                          item.id === row.id ? { ...item, valuesText: event.target.value } : item
                        ),
                      }))
                    }
                    placeholder="예: 253,254,252"
                  />
                </Field>
                <div className="flex items-end">
                  <button
                    type="button"
                    onClick={() =>
                      setForm((current) => ({
                        ...current,
                        eqpmn: current.eqpmn.filter((item) => item.id !== row.id),
                      }))
                    }
                    className="w-full rounded-xl border border-rose-800 px-3 py-3 text-sm text-rose-200 transition hover:bg-rose-950"
                  >
                    삭제
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="mt-6 space-y-3">
          {validation.errors.map((issue) => (
            <p
              key={`${issue.field}-${issue.message}`}
              className="rounded-xl border border-rose-900 bg-rose-950/60 px-4 py-3 text-sm text-rose-200"
            >
              {issue.message}
            </p>
          ))}
          {validation.warnings.map((issue) => (
            <p
              key={`${issue.field}-${issue.message}`}
              className="rounded-xl border border-amber-900 bg-amber-950/60 px-4 py-3 text-sm text-amber-200"
            >
              {issue.message}
            </p>
          ))}
        </div>
      </section>

      <section className="space-y-4">
        <div className="rounded-[28px] border border-slate-800 bg-[radial-gradient(circle_at_top,#0f766e_0%,#020617_60%)] p-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="text-sm uppercase tracking-[0.25em] text-cyan-200">생성 결과</p>
              <h2 className="mt-2 text-2xl font-semibold text-white">통신모듈 전송 순서</h2>
            </div>
            <div className="flex gap-2">
              <button
                type="button"
                onClick={() => setShowCompact(false)}
                className={`rounded-md px-3 py-2 text-sm transition ${
                  !showCompact ? "bg-white text-slate-950" : "border border-slate-600 text-slate-200"
                }`}
              >
                JSON 보기 좋게
              </button>
              <button
                type="button"
                onClick={() => setShowCompact(true)}
                className={`rounded-md px-3 py-2 text-sm transition ${
                  showCompact ? "bg-white text-slate-950" : "border border-slate-600 text-slate-200"
                }`}
              >
                JSON 한 줄
              </button>
            </div>
          </div>
        </div>

        {output ? (
          <SectionCard
            title="Raw MQTT CONNECT"
            subtitle="MQTT 연결 메시지"
            action={<CopyButton label="CONNECT HEX 복사" value={output.raw_mqtt_connect_hex} />}
          >
            <div className="mb-3 rounded-xl border border-slate-800 bg-slate-900/80 p-4 text-sm leading-6 text-slate-100">
              <p>Client ID: {output.mqtt_client_id}</p>
              <p>Keep Alive: 60</p>
              <p>Clean Session: true</p>
            </div>
            <div className="space-y-4">
              <div>
                <div className="mb-2 flex items-center justify-between gap-3">
                  <p className="text-sm font-medium text-slate-300">HEX</p>
                  <CopyButton label="CONNECT HEX 복사" value={output.raw_mqtt_connect_hex} />
                </div>
                <pre className="whitespace-pre-wrap break-all text-sm leading-6 text-slate-100">
                  {output.raw_mqtt_connect_hex}
                </pre>
              </div>
              <div>
                <div className="mb-2 flex items-center justify-between gap-3">
                  <p className="text-sm font-medium text-slate-300">ASCII</p>
                  <CopyButton label="CONNECT ASCII 복사" value={output.raw_mqtt_connect_ascii} />
                </div>
                <pre className="whitespace-pre-wrap break-all text-sm leading-6 text-slate-100">
                  {output.raw_mqtt_connect_ascii}
                </pre>
              </div>
            </div>
            <div className="mt-4 rounded-xl border border-emerald-900 bg-emerald-950/30 p-4">
              <div className="mb-2 flex items-center justify-between gap-3">
                <p className="text-sm font-medium text-emerald-200">예상 CONNACK HEX</p>
                <CopyButton label="CONNACK 복사" value="20 02 00 00" />
              </div>
              <pre className="whitespace-pre-wrap break-all text-sm leading-6 text-slate-100">
                20 02 00 00
              </pre>
            </div>
          </SectionCard>
        ) : null}

        {output ? (
          <SectionCard
            title="Raw MQTT PUBLISH"
            subtitle="메시지 전송"
            action={<CopyButton label="PUBLISH HEX 복사" value={output.raw_mqtt_publish_hex} />}
          >
            <div className="mb-3 rounded-xl border border-slate-800 bg-slate-900/80 p-4 text-sm leading-6 text-slate-100">
              <p>Topic: {output.state_topic}</p>
              <p>QoS: 1</p>
              <p>Packet ID: 1</p>
            </div>
            <div className="space-y-4">
              <div>
                <div className="mb-2 flex items-center justify-between gap-3">
                  <p className="text-sm font-medium text-slate-300">HEX</p>
                  <CopyButton label="PUBLISH HEX 복사" value={output.raw_mqtt_publish_hex} />
                </div>
                <pre className="whitespace-pre-wrap break-all text-sm leading-6 text-slate-100">
                  {output.raw_mqtt_publish_hex}
                </pre>
              </div>
              <div>
                <div className="mb-2 flex items-center justify-between gap-3">
                  <p className="text-sm font-medium text-slate-300">ASCII</p>
                  <CopyButton label="PUBLISH ASCII 복사" value={output.raw_mqtt_publish_ascii} />
                </div>
                <pre className="whitespace-pre-wrap break-all text-sm leading-6 text-slate-100">
                  {output.raw_mqtt_publish_ascii}
                </pre>
              </div>
            </div>
            <div className="mt-4 rounded-xl border border-emerald-900 bg-emerald-950/30 p-4">
              <div className="mb-2 flex items-center justify-between gap-3">
                <p className="text-sm font-medium text-emerald-200">예상 PUBACK HEX</p>
                <CopyButton label="PUBACK 복사" value="40 02 00 01" />
              </div>
              <pre className="whitespace-pre-wrap break-all text-sm leading-6 text-slate-100">
                40 02 00 01
              </pre>
            </div>
          </SectionCard>
        ) : null}

        {output ? (
          <details className="rounded-[28px] border border-slate-800 bg-slate-950/80 p-5">
            <summary className="cursor-pointer list-none">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="text-sm uppercase tracking-[0.25em] text-amber-300">
                    통신모듈 송수신 순서
                  </p>
                  <h3 className="mt-2 text-xl font-semibold text-white">
                    무엇을 보내고 어떤 데이터가 오는가
                  </h3>
                </div>
                <span className="text-sm text-slate-400">펼치기</span>
              </div>
            </summary>
            <div className="mt-4 space-y-4">
              {flowSteps.map((item) => (
                <FlowCard
                  key={item.step}
                  step={item.step}
                  sendLabel={item.sendLabel}
                  sendValue={item.sendValue}
                  receiveLabel={item.receiveLabel}
                  receiveValue={item.receiveValue}
                />
              ))}
            </div>
          </details>
        ) : null}

        {output ? (
          <SectionCard title="RAW MQTT PUBLISH Payload">
            <pre className="whitespace-pre-wrap break-all text-sm leading-6 text-slate-100">
              {output.payload_compact}
            </pre>
          </SectionCard>
        ) : null}
      </section>
    </div>
  );
}
