"use client";

import { useEffect, useMemo, useState } from "react";
import {
  DEFAULT_JSON_FORM,
  EQPMN_NAME_OPTIONS,
  JsonRoomStateForm,
  JsonRoomStateOutput,
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
      {copied ? "Copied" : label}
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

  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)]">
      <section className="rounded-[28px] border border-slate-800 bg-slate-950/90 p-6 shadow-[0_24px_80px_rgba(0,0,0,0.35)]">
        <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-2xl font-semibold text-white">JSON Protocol Generator</h2>
            <p className="mt-1 text-sm text-slate-400">
              JSON Room State Protocol v2 payload and MQTT test commands.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() => setForm(DEFAULT_JSON_FORM)}
              className="rounded-md border border-slate-700 px-3 py-2 text-sm text-slate-300 transition hover:border-slate-500"
            >
              Reset
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
              measure_ts Now
            </button>
            <button
              type="button"
              onClick={() =>
                setForm((current) => ({
                  ...current,
                  seq: String((Number(current.seq) || 0) + 1),
                }))
              }
              className="rounded-md border border-emerald-700 px-3 py-2 text-sm text-emerald-200 transition hover:bg-emerald-950"
            >
              seq +1
            </button>
          </div>
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <Field label="isnd_regist_no">
            <input
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.isnd_regist_no}
              onChange={(event) =>
                setForm((current) => ({ ...current, isnd_regist_no: event.target.value }))
              }
            />
          </Field>
          <Field label="seq">
            <input
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.seq}
              onChange={(event) => setForm((current) => ({ ...current, seq: event.target.value }))}
            />
          </Field>
          <Field label="measure_ts">
            <input
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.measure_ts}
              onChange={(event) =>
                setForm((current) => ({ ...current, measure_ts: event.target.value }))
              }
            />
          </Field>
          <Field label="species">
            <input
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.species}
              onChange={(event) =>
                setForm((current) => ({ ...current, species: event.target.value }))
              }
            />
          </Field>
          <Field label="stall_ty_code">
            <input
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.stall_ty_code}
              onChange={(event) =>
                setForm((current) => ({ ...current, stall_ty_code: event.target.value }))
              }
            />
          </Field>
          <Field label="stall_no">
            <input
              className="rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-slate-100 outline-none transition focus:border-cyan-400"
              value={form.stall_no}
              onChange={(event) =>
                setForm((current) => ({ ...current, stall_no: event.target.value }))
              }
            />
          </Field>
          <Field label="room_no">
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
              eqpmn Rows
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
              Add Row
            </button>
          </div>
          <div className="space-y-3">
            {form.eqpmn.map((row) => (
              <div
                key={row.id}
                className="grid gap-3 rounded-2xl border border-slate-800 bg-slate-900/70 p-4 md:grid-cols-[160px_minmax(0,1fr)_100px]"
              >
                <Field label="name">
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
                        {name}
                      </option>
                    ))}
                  </select>
                </Field>
                <Field label="values">
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
                    placeholder="253,254,252"
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
                    Remove
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
              <p className="text-sm uppercase tracking-[0.25em] text-cyan-200">Generated Output</p>
              <h2 className="mt-2 text-2xl font-semibold text-white">MQTT STATE + ACK</h2>
            </div>
            <div className="flex gap-2">
              <button
                type="button"
                onClick={() => setShowCompact(false)}
                className={`rounded-md px-3 py-2 text-sm transition ${
                  !showCompact ? "bg-white text-slate-950" : "border border-slate-600 text-slate-200"
                }`}
              >
                JSON Pretty
              </button>
              <button
                type="button"
                onClick={() => setShowCompact(true)}
                className={`rounded-md px-3 py-2 text-sm transition ${
                  showCompact ? "bg-white text-slate-950" : "border border-slate-600 text-slate-200"
                }`}
              >
                JSON Compact
              </button>
            </div>
          </div>
        </div>

        <OutputCard
          title="STATE Topic"
          value={output?.state_topic ?? ""}
          copyLabel="Copy Topic"
        />
        <OutputCard
          title="ACK Topic"
          value={output?.ack_topic ?? ""}
          copyLabel="Copy Topic"
        />
        <OutputCard
          title="JSON Payload"
          value={payloadText}
          copyLabel="Copy Payload"
        />
        <OutputCard
          title="mosquitto_pub"
          value={output?.mosquitto_pub_command ?? ""}
          copyLabel="Copy Pub"
        />
        <OutputCard
          title="ACK subscribe"
          value={output?.mosquitto_sub_ack_command ?? ""}
          copyLabel="Copy ACK"
        />
      </section>
    </div>
  );
}
