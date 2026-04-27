"use client";

import { useState, useCallback, useEffect, useRef } from "react";

// ============================================================
// 타입
// ============================================================
type Block = { eq_code: string; value: string };

interface FormState {
  keep_alive: string;
  username: string;
  password: string;
  clean_session: boolean;
  regist_no: string;
  stall_ty_code: string;
  stall_no: string;
  room_no: string;
  species: string;
  year: string;
  month: string;
  day: string;
  hour: string;
  minute: string;
  ref_seq: string;
  blocks: Block[];
}

interface OutputState {
  key12: string;
  connect_hex: string;
  expected_connack: string;
  topic: string;
  state_hex: string;
  state_base64: string;
  mqtt_payload: string;
  publish_hex: string;
}

// ============================================================
// 기본값
// ============================================================
const DEFAULT_BLOCKS: Block[] = [
  { eq_code: "ES01", value: "253" },
  { eq_code: "ES02", value: "605" },
  { eq_code: "EC01", value: "1200" },
  { eq_code: "EC02", value: "800" },
  { eq_code: "EC03", value: "131071" },
];

const INITIAL_FORM: FormState = {
  keep_alive: "60",
  username: "",
  password: "",
  clean_session: true,
  regist_no: "FARM01",
  stall_ty_code: "1",
  stall_no: "1",
  room_no: "1",
  species: "P",
  year: "2025",
  month: "4",
  day: "13",
  hour: "14",
  minute: "30",
  ref_seq: "1",
  blocks: DEFAULT_BLOCKS,
};

const SPECIES_OPTIONS = [
  { value: "W", label: "W — 한우" },
  { value: "D", label: "D — 젖소" },
  { value: "P", label: "P — 돼지" },
  { value: "H", label: "H — 말" },
  { value: "T", label: "T — 칠면조" },
  { value: "R", label: "R — 토끼" },
  { value: "I", label: "I — 곤충" },
  { value: "B", label: "B — 육우" },
];

// 장비 코드 → 한글명 매핑
const EQ_KO: Record<string, string> = {
  ES01: "온도 센서",
  ES02: "습도 센서",
  ES03: "환경 센서 #3",
  ES04: "환경 센서 #4",
  EC01: "송풍팬",
  EC02: "입기팬",
  EC03: "배기팬",
  EC04: "환경 제어기 #4",
  PC01: "전력 제어기 #1",
  PC02: "전력 제어기 #2",
  BI01: "생체 장비 #1",
  AF01: "공기 필터 #1",
  SF01: "안전 장비 #1",
  VI01: "영상 장비 #1",
};

const EQ_OPTIONS = Object.keys(EQ_KO);

const NOT_INSTALLED = 131071;
const NO_DATA = 131070;
const STORAGE_KEY = "pgen_form_v1";

// ============================================================
// 공통 스타일
// ============================================================
const inputCls =
  "rounded-md border border-gray-700 bg-gray-800 px-3 py-2 text-lg text-gray-100 " +
  "focus:outline-none focus:ring-1 focus:ring-indigo-500 w-full";

// 블록 행 — w-full 제거, min-w-0 추가
const blockSelectCls =
  "rounded-md border border-gray-700 bg-gray-800 px-2 py-2 text-base text-gray-100 " +
  "focus:outline-none focus:ring-1 focus:ring-indigo-500 w-32 flex-shrink-0";

const blockInputCls =
  "rounded-md border border-gray-700 bg-gray-800 px-3 py-2 text-base text-gray-100 " +
  "focus:outline-none focus:ring-1 focus:ring-indigo-500 flex-1 min-w-0";

const sentinelBtnCls =
  "text-sm px-2 py-1 rounded border border-gray-600 text-gray-400 " +
  "hover:border-orange-500 hover:text-orange-300 transition-colors whitespace-nowrap flex-shrink-0";

// ============================================================
// HEX 해설 — STATE binary를 구간별로 분리
// ============================================================
function parseStateHexSections(hexStr: string): { label: string; bytes: string }[] {
  if (!hexStr) return [];
  const bytes = hexStr.split(" ");
  if (bytes.length < 14) return [{ label: "데이터", bytes: hexStr }];

  const blockHeaderByte = bytes[13];
  const blockHeader = parseInt(blockHeaderByte, 16);
  const blockCount = (blockHeader >> 1) & 0xf;

  const sections: { label: string; bytes: string }[] = [
    { label: "Key9 (9B)", bytes: bytes.slice(0, 9).join(" ") },
    { label: "Time4 (4B)", bytes: bytes.slice(9, 13).join(" ") },
    { label: `BlockHeader (ver=2, count=${blockCount})`, bytes: bytes[13] },
  ];

  let offset = 14;
  for (let i = 0; i < blockCount; i++) {
    if (offset >= bytes.length) break;
    const eqByte = parseInt(bytes[offset], 16);
    const type3 = (eqByte >> 5) & 0x7;
    const noRaw = (eqByte >> 1) & 0xf;
    const prefixes = ["ES","EC","PC","BI","AF","SF","VI"];
    const prefix = prefixes[type3] ?? "??";
    const eqLabel = `${prefix}${String(noRaw + 1).padStart(2,"0")}`;
    const vsl = parseInt(bytes[offset + 1] ?? "1", 16);
    const ic  = parseInt(bytes[offset + 2] ?? "1", 16);
    const blockLen = 3 + 3 * vsl * ic;
    sections.push({
      label: `Block#${i} ${eqLabel} (${blockLen}B)`,
      bytes: bytes.slice(offset, offset + blockLen).join(" "),
    });
    offset += blockLen;
  }
  return sections;
}

// ============================================================
// 컴포넌트
// ============================================================
function Card({ title, children, action }: {
  title: string;
  children: React.ReactNode;
  action?: React.ReactNode;
}) {
  return (
    <div className="rounded-xl border border-gray-700 bg-gray-900 p-6 flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <h2 className="text-base font-semibold text-indigo-400 uppercase tracking-widest">{title}</h2>
        {action}
      </div>
      {children}
    </div>
  );
}

// 한글 강조 + 변수명 라벨
function FL(ko: string, varName?: string) {
  return (
    <span className="flex items-baseline gap-2">
      <span className="text-lg font-semibold text-white">{ko}</span>
      {varName && (
        <span className="text-sm font-mono text-gray-500">{varName}</span>
      )}
    </span>
  );
}

// 시간 필드 한글 매핑
const TIME_KO: Record<string, string> = {
  year: "연도", month: "월", day: "일", hour: "시", minute: "분",
};

function Field({ label, children }: { label: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <label className="leading-snug">{label}</label>
      {children}
    </div>
  );
}

function CopyButton({ text, label = "복사" }: { text: string; label?: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      onClick={() => {
        navigator.clipboard.writeText(text).then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        });
      }}
      className="text-base px-3 py-1.5 rounded border border-gray-600 text-gray-300
                 hover:border-indigo-500 hover:text-indigo-300 transition-colors flex-shrink-0"
    >
      {copied ? "완료" : label}
    </button>
  );
}

// 강조 카드 — TCP로 직접 전송하는 주요 패킷
function PrimaryCard({ label, badge, badgeColor, value }: {
  label: string;
  badge: string;
  badgeColor: "indigo" | "emerald";
  value: string;
}) {
  const borderCls = badgeColor === "indigo"
    ? "border-indigo-500"
    : "border-emerald-500";
  const badgeCls = badgeColor === "indigo"
    ? "bg-indigo-900 text-indigo-300 border border-indigo-600"
    : "bg-emerald-900 text-emerald-300 border border-emerald-600";
  const labelCls = badgeColor === "indigo"
    ? "text-indigo-300"
    : "text-emerald-300";

  return (
    <div className={`rounded-xl border-2 ${borderCls} bg-gray-900 p-5 flex flex-col gap-3`}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <span className={`text-xl font-bold uppercase tracking-widest ${labelCls}`}>{label}</span>
          <span className={`text-sm px-2 py-0.5 rounded-full font-semibold ${badgeCls}`}>{badge}</span>
        </div>
        {value && <CopyButton text={value} />}
      </div>
      {value ? (
        <pre className="text-base break-all whitespace-pre-wrap text-white leading-relaxed font-mono
                        max-h-48 overflow-y-auto">
          {value}
        </pre>
      ) : (
        <span className="text-base text-gray-600">—</span>
      )}
    </div>
  );
}

// 일반 참고 카드 (참고 섹션 내부)
function RefCard({ label, value, annotation }: {
  label: string;
  value: string;
  annotation?: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-gray-700 bg-gray-850 p-4 flex flex-col gap-2">
      <div className="flex items-center justify-between gap-2">
        <span className="text-base font-semibold text-gray-400 uppercase tracking-wider truncate">{label}</span>
        {value && <CopyButton text={value} />}
      </div>
      {value ? (
        <>
          <pre className="text-base break-all whitespace-pre-wrap text-gray-300 leading-relaxed
                          font-mono max-h-32 overflow-y-auto">
            {value}
          </pre>
          {annotation}
        </>
      ) : (
        <span className="text-base text-gray-600">—</span>
      )}
    </div>
  );
}

// STATE HEX 참고 카드 (구간별 해설 포함)
function StateHexRefCard({ value }: { value: string }) {
  const sections = parseStateHexSections(value);
  return (
    <div className="rounded-lg border border-gray-700 bg-gray-850 p-4 flex flex-col gap-2">
      <div className="flex items-center justify-between gap-2">
        <span className="text-base font-semibold text-gray-400 uppercase tracking-wider">
          STATE Binary HEX
        </span>
        {value && <CopyButton text={value} />}
      </div>
      {value ? (
        <>
          <pre className="text-base break-all whitespace-pre-wrap text-gray-300 leading-relaxed font-mono">
            {value}
          </pre>
          <div className="mt-1 space-y-1 border-t border-gray-700 pt-2">
            {sections.map((s, i) => (
              <div key={i} className="flex gap-3 text-sm">
                <span className="text-gray-500 w-56 flex-shrink-0 truncate">{s.label}</span>
                <span className="font-mono text-yellow-600 break-all">{s.bytes}</span>
              </div>
            ))}
          </div>
        </>
      ) : (
        <span className="text-base text-gray-600">—</span>
      )}
    </div>
  );
}

// ============================================================
// 메인 페이지
// ============================================================
export default function Home() {
  const [form, setForm] = useState<FormState>(() => {
    if (typeof window !== "undefined") {
      try {
        const saved = localStorage.getItem(STORAGE_KEY);
        if (saved) return JSON.parse(saved) as FormState;
      } catch {}
    }
    return INITIAL_FORM;
  });

  const [output, setOutput] = useState<OutputState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [autoGenerate, setAutoGenerate] = useState(false);
  const [showJsonPanel, setShowJsonPanel] = useState(false);
  const [jsonImportText, setJsonImportText] = useState("");
  const [jsonImportError, setJsonImportError] = useState<string | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // localStorage 자동 저장
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(form));
    } catch {}
  }, [form]);

  // 자동 생성 (디바운스 300ms)
  useEffect(() => {
    if (!autoGenerate) return;
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => { callGenerate(form); }, 300);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [form, autoGenerate]);

  const setField = useCallback(
    <K extends keyof FormState>(key: K, value: FormState[K]) =>
      setForm((f) => ({ ...f, [key]: value })),
    []
  );

  const setBlock = (idx: number, field: keyof Block, val: string) =>
    setForm((f) => ({
      ...f,
      blocks: f.blocks.map((b, i) => (i === idx ? { ...b, [field]: val } : b)),
    }));

  const moveBlock = (idx: number, dir: -1 | 1) =>
    setForm((f) => {
      const blocks = [...f.blocks];
      const target = idx + dir;
      if (target < 0 || target >= blocks.length) return f;
      [blocks[idx], blocks[target]] = [blocks[target], blocks[idx]];
      return { ...f, blocks };
    });

  const addBlock = () =>
    setForm((f) => ({
      ...f,
      blocks: [...f.blocks, { eq_code: "ES01", value: "0" }],
    }));

  const removeBlock = (idx: number) =>
    setForm((f) => ({ ...f, blocks: f.blocks.filter((_, i) => i !== idx) }));

  const callGenerate = async (currentForm: FormState) => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/generate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          regist_no:     currentForm.regist_no,
          species:       currentForm.species,
          stall_ty_code: Number(currentForm.stall_ty_code),
          stall_no:      Number(currentForm.stall_no),
          room_no:       Number(currentForm.room_no),
          ref_seq:       Number(currentForm.ref_seq),
          year:          Number(currentForm.year),
          month:         Number(currentForm.month),
          day:           Number(currentForm.day),
          hour:          Number(currentForm.hour),
          minute:        Number(currentForm.minute),
          blocks: currentForm.blocks.map((b) => [b.eq_code, Number(b.value)]),
          keep_alive:    Number(currentForm.keep_alive),
          username:      currentForm.username || null,
          password:      currentForm.password || null,
          clean_session: currentForm.clean_session,
        }),
      });
      const data = await res.json();
      if (!res.ok || data.error) {
        setError(data.error ?? "오류가 발생했습니다.");
        setOutput(null);
      } else {
        setOutput(data as OutputState);
      }
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  const generate = () => callGenerate(form);

  const reset = () => {
    setForm(INITIAL_FORM);
    setOutput(null);
    setError(null);
    try { localStorage.removeItem(STORAGE_KEY); } catch {}
  };

  // JSON 내보내기
  const exportJson = () => {
    const json = JSON.stringify(form, null, 2);
    const blob = new Blob([json], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `pgen_${form.regist_no || "config"}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // JSON 가져오기
  const importJson = () => {
    setJsonImportError(null);
    try {
      const parsed = JSON.parse(jsonImportText) as FormState;
      if (!parsed.regist_no || !Array.isArray(parsed.blocks))
        throw new Error("regist_no 또는 blocks 필드가 없습니다.");
      setForm(parsed);
      setShowJsonPanel(false);
      setJsonImportText("");
    } catch (e) {
      setJsonImportError(String(e));
    }
  };

  // 전체 복사 (통신모듈 개발자 전달용)
  const copyAll = () => {
    if (!output) return;
    const text = [
      `## 성일기전 프로토콜 패킷 — ${output.key12}`,
      "",
      `**CONNECT HEX**`,
      "```",
      output.connect_hex,
      "```",
      `Expected CONNACK: \`${output.expected_connack}\``,
      "",
      `**Topic**: \`${output.topic}\``,
      "",
      `**PUBLISH HEX**`,
      "```",
      output.publish_hex,
      "```",
      "",
      `**MQTT Payload**: \`${output.mqtt_payload}\``,
      "",
      `**STATE Binary HEX**`,
      "```",
      output.state_hex,
      "```",
    ].join("\n");
    navigator.clipboard.writeText(text);
  };

  const key12Preview =
    form.regist_no.length === 6
      ? `${form.regist_no}${String(form.stall_ty_code).padStart(2,"0")}${String(form.stall_no).padStart(2,"0")}${String(form.room_no).padStart(2,"0")}`
      : null;

  return (
    <main className="mx-auto max-w-7xl px-4 py-8 space-y-6">
      {/* 헤더 */}
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-4xl font-bold text-white">성일기전 프로토콜 자동생성기</h1>
          <p className="text-lg text-gray-400 mt-1">
            MQTT CONNECT / PUBLISH 데이터 생성 도구 — v2 데이터폼 기준
          </p>
        </div>
        <div className="flex gap-2 flex-wrap">
          {/* 자동 생성 토글 */}
          <button
            onClick={() => setAutoGenerate((v) => !v)}
            className={`text-base px-4 py-2 rounded-lg border transition-colors ${
              autoGenerate
                ? "border-green-500 text-green-300 bg-green-950"
                : "border-gray-600 text-gray-400 hover:border-gray-400"
            }`}
          >
            {autoGenerate ? "자동 생성 ON" : "자동 생성 OFF"}
          </button>
          {/* JSON 패널 토글 */}
          <button
            onClick={() => setShowJsonPanel((v) => !v)}
            className="text-base px-4 py-2 rounded-lg border border-gray-600 text-gray-400 hover:border-indigo-500 hover:text-indigo-300 transition-colors"
          >
            JSON 내보내기/가져오기
          </button>
          {/* 전체 복사 */}
          {output && (
            <button
              onClick={copyAll}
              className="text-base px-4 py-2 rounded-lg border border-indigo-600 text-indigo-300 hover:bg-indigo-900 transition-colors"
            >
              전체 복사 (개발자 전달용)
            </button>
          )}
        </div>
      </header>

      {/* JSON 패널 */}
      {showJsonPanel && (
        <div className="rounded-xl border border-gray-700 bg-gray-900 p-6 space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-base font-semibold text-indigo-400 uppercase tracking-widest">
              JSON 내보내기 / 가져오기
            </h2>
            <button
              onClick={() => setShowJsonPanel(false)}
              className="text-gray-500 hover:text-gray-300 text-xl leading-none"
            >×</button>
          </div>
          <div className="flex gap-3 flex-wrap">
            <button
              onClick={exportJson}
              className="text-base px-4 py-2 rounded-lg border border-indigo-600 text-indigo-300 hover:bg-indigo-900 transition-colors"
            >
              현재 설정 JSON 다운로드
            </button>
            <button
              onClick={() => setJsonImportText(JSON.stringify(form, null, 2))}
              className="text-base px-4 py-2 rounded-lg border border-gray-600 text-gray-300 hover:border-gray-400 transition-colors"
            >
              현재 설정 텍스트로 보기
            </button>
          </div>
          <textarea
            className={`${inputCls} font-mono text-base h-36 resize-y`}
            placeholder="여기에 JSON을 붙여 넣고 아래 '가져오기'를 클릭하세요"
            value={jsonImportText}
            onChange={(e) => setJsonImportText(e.target.value)}
          />
          {jsonImportError && (
            <p className="text-base text-red-400">{jsonImportError}</p>
          )}
          <button
            onClick={importJson}
            disabled={!jsonImportText.trim()}
            className="text-base px-4 py-2 rounded-lg bg-indigo-700 text-white hover:bg-indigo-600 disabled:opacity-40 transition-colors"
          >
            가져오기
          </button>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-[5fr_7fr] gap-6 items-start">
        {/* ──────── 좌측: 입력 ──────── */}
        <div className="space-y-4">

          {/* CONNECT 설정 */}
          <Card title="CONNECT 설정">
            <div className="grid grid-cols-2 gap-3">
              <Field label={FL("유지 시간", "keep_alive")}>
                <input className={inputCls} type="number" min={0}
                  value={form.keep_alive}
                  onChange={(e) => setField("keep_alive", e.target.value)} />
              </Field>
              <Field label={FL("클린 세션", "clean_session")}>
                <select className={inputCls}
                  value={String(form.clean_session)}
                  onChange={(e) => setField("clean_session", e.target.value === "true")}>
                  <option value="true">true</option>
                  <option value="false">false</option>
                </select>
              </Field>
              <Field label={FL("사용자명", "username")}>
                <input className={inputCls} type="text" placeholder="없으면 빈 칸"
                  value={form.username}
                  onChange={(e) => setField("username", e.target.value)} />
              </Field>
              <Field label={FL("비밀번호", "password")}>
                <input className={inputCls} type="password" placeholder="없으면 빈 칸"
                  value={form.password}
                  onChange={(e) => setField("password", e.target.value)} />
              </Field>
            </div>
          </Card>

          {/* Key12 구성 */}
          <Card title="Key12 구성">
            <div className="grid grid-cols-2 gap-3">
              <Field label={FL("등록번호 (6자)", "regist_no")}>
                <input className={inputCls} type="text" maxLength={6}
                  value={form.regist_no}
                  onChange={(e) => setField("regist_no", e.target.value.toUpperCase())} />
              </Field>
              <Field label={FL("축종", "species")}>
                <select className={inputCls} value={form.species}
                  onChange={(e) => setField("species", e.target.value)}>
                  {SPECIES_OPTIONS.map((o) => (
                    <option key={o.value} value={o.value}>{o.label}</option>
                  ))}
                </select>
              </Field>
              <Field label={FL("축사 타입 (1~10)", "stall_ty_code")}>
                <input className={inputCls} type="number" min={1} max={10}
                  value={form.stall_ty_code}
                  onChange={(e) => setField("stall_ty_code", e.target.value)} />
              </Field>
              <Field label={FL("축사 번호", "stall_no")}>
                <input className={inputCls} type="number" min={0} max={99}
                  value={form.stall_no}
                  onChange={(e) => setField("stall_no", e.target.value)} />
              </Field>
              <Field label={FL("방 번호", "room_no")}>
                <input className={inputCls} type="number" min={0} max={99}
                  value={form.room_no}
                  onChange={(e) => setField("room_no", e.target.value)} />
              </Field>
              <Field label={FL("시퀀스 번호", "ref_seq")}>
                <input className={inputCls} type="number" min={1}
                  value={form.ref_seq}
                  onChange={(e) => setField("ref_seq", e.target.value)} />
              </Field>
            </div>
            {key12Preview ? (
              <p className="text-base text-green-400">
                key12 미리보기: <span className="font-bold font-mono">{key12Preview}</span>
              </p>
            ) : (
              <p className="text-base text-red-400">regist_no는 정확히 6자여야 합니다.</p>
            )}
          </Card>

          {/* 시간 구성 */}
          <Card title="시간 구성 (측정 시각)">
            <div className="grid grid-cols-5 gap-2">
              {(["year","month","day","hour","minute"] as const).map((k) => (
                <Field key={k} label={FL(TIME_KO[k], k)}>
                  <input className={inputCls} type="number"
                    value={form[k]}
                    onChange={(e) => setField(k, e.target.value)} />
                </Field>
              ))}
            </div>
          </Card>

          {/* 장비 블록 */}
          <Card title="장비 블록">
            <div className="space-y-2">
              {form.blocks.map((b, idx) => (
                <div key={idx} className="flex gap-2 items-center">
                  {/* 순서 변경 버튼 */}
                  <div className="flex flex-col gap-0.5 flex-shrink-0">
                    <button
                      onClick={() => moveBlock(idx, -1)}
                      disabled={idx === 0}
                      className="text-gray-600 hover:text-gray-300 disabled:opacity-20 leading-none text-xs"
                      title="위로"
                    >▲</button>
                    <button
                      onClick={() => moveBlock(idx, 1)}
                      disabled={idx === form.blocks.length - 1}
                      className="text-gray-600 hover:text-gray-300 disabled:opacity-20 leading-none text-xs"
                      title="아래로"
                    >▼</button>
                  </div>

                  {/* 장비 코드 select + 한글명 */}
                  <div className="flex flex-col gap-0.5">
                    <select
                      className={blockSelectCls}
                      value={b.eq_code}
                      onChange={(e) => setBlock(idx, "eq_code", e.target.value)}
                    >
                      {EQ_OPTIONS.map((eq) => (
                        <option key={eq} value={eq}>{eq} — {EQ_KO[eq]}</option>
                      ))}
                    </select>
                    <span className="text-xs text-indigo-400 pl-1 font-semibold">
                      {EQ_KO[b.eq_code] ?? ""}
                    </span>
                  </div>

                  {/* 값 입력 — blockInputCls 사용 (flex-1 min-w-0) */}
                  <input
                    className={blockInputCls}
                    type="number"
                    min={0}
                    max={131071}
                    placeholder="0~131071"
                    value={b.value}
                    onChange={(e) => setBlock(idx, "value", e.target.value)}
                  />

                  {/* 센티널 빠른 선택 버튼 */}
                  <button
                    className={sentinelBtnCls}
                    onClick={() => setBlock(idx, "value", String(NOT_INSTALLED))}
                    title="NOT_INSTALLED = 131071"
                  >N/A</button>
                  <button
                    className={sentinelBtnCls}
                    onClick={() => setBlock(idx, "value", String(NO_DATA))}
                    title="NO_DATA = 131070"
                  >N/D</button>

                  {/* 삭제 */}
                  <button
                    onClick={() => removeBlock(idx)}
                    className="text-gray-500 hover:text-red-400 text-base leading-none flex-shrink-0 px-0.5"
                    title="삭제"
                  >×</button>
                </div>
              ))}
            </div>

            {/* 센티널 값 범례 */}
            <p className="text-base text-gray-600 pt-1">
              <span className="text-gray-500">N/A</span> = NOT_INSTALLED (131071) &nbsp;
              <span className="text-gray-500">N/D</span> = NO_DATA (131070)
            </p>

            <div className="flex gap-2 items-center">
              <button
                onClick={addBlock}
                className="text-base px-4 py-2 rounded border border-gray-600 text-gray-300 hover:border-indigo-500 hover:text-indigo-300 transition-colors"
              >
                + 블록 추가
              </button>
              <span className="text-base text-gray-600">
                {form.blocks.length}개 / 최대 15개
              </span>
            </div>
          </Card>

          {/* 생성 버튼 */}
          <div className="flex gap-3">
            <button
              onClick={generate}
              disabled={loading}
              className="flex-1 rounded-lg bg-indigo-600 px-4 py-4 text-lg font-semibold
                         text-white hover:bg-indigo-500 disabled:opacity-50 transition-colors"
            >
              {loading ? "생성 중…" : "패킷 생성"}
            </button>
            <button
              onClick={reset}
              className="px-4 py-4 rounded-lg border border-gray-600 text-lg text-gray-300
                         hover:border-gray-400 transition-colors"
            >
              초기화
            </button>
          </div>

          {error && (
            <div className="rounded-lg border border-red-700 bg-red-950 px-4 py-4 text-lg text-red-300">
              <span className="font-semibold">오류: </span>{error}
            </div>
          )}
        </div>

        {/* ──────── 우측: 출력 (sticky) ──────── */}
        <div className="space-y-4 lg:sticky lg:top-4 lg:max-h-[calc(100vh-2rem)] lg:overflow-y-auto">
          {output ? (
            <>
              {/* ── 주요 패킷 (TCP 직접 전송) ── */}
              <PrimaryCard
                label="CONNECT HEX"
                badge="TCP 전송 ①"
                badgeColor="indigo"
                value={output.connect_hex}
              />
              <PrimaryCard
                label="PUBLISH HEX"
                badge="TCP 전송 ②"
                badgeColor="emerald"
                value={output.publish_hex}
              />

              {/* Expected CONNACK */}
              <div className="rounded-xl border border-gray-600 bg-gray-900 px-6 py-4 flex items-center justify-between gap-4">
                <div>
                  <p className="text-base font-semibold text-gray-400 uppercase tracking-widest mb-1">
                    Expected CONNACK
                  </p>
                  <p className="text-lg font-mono text-gray-200">{output.expected_connack}</p>
                </div>
                <CopyButton text={output.expected_connack} />
              </div>

              {/* ── 참고 정보 (접기/펼치기) ── */}
              <details className="group rounded-xl border border-gray-700 bg-gray-900 overflow-hidden">
                <summary className="flex items-center justify-between px-6 py-4 cursor-pointer
                                    text-lg font-semibold text-gray-400 hover:text-gray-200
                                    transition-colors select-none list-none">
                  <span>참고 정보 (Topic / STATE HEX / Base64 / Payload / Key12)</span>
                  <span className="text-gray-600 group-open:rotate-90 transition-transform text-xl">▶</span>
                </summary>
                <div className="px-5 pb-5 space-y-3 border-t border-gray-700 pt-4">
                  <RefCard label="Topic" value={output.topic} />
                  <StateHexRefCard value={output.state_hex} />
                  <RefCard label="STATE Base64 (순수, B64: 접두사 없음)" value={output.state_base64} />
                  <RefCard label='MQTT Payload ("SEQ:N,B64:…")' value={output.mqtt_payload} />
                  <RefCard label="Key12" value={output.key12} />
                </div>
              </details>
            </>
          ) : (
            <div className="rounded-xl border border-dashed border-gray-700 bg-gray-900
                            flex items-center justify-center h-64 text-gray-500 text-lg">
              {loading ? "생성 중…" : '좌측 입력 후 "패킷 생성" 버튼을 누르세요'}
            </div>
          )}
        </div>
      </div>

      {/* 푸터 */}
      <footer className="text-base text-gray-600 border-t border-gray-800 pt-4">
        <p>protocol-spec.md v2 데이터폼 기준 — 성일기전</p>
      </footer>
    </main>
  );
}
