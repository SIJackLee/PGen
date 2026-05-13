export default function LegacyBinaryGenerator() {
  return (
    <section className="rounded-[28px] border border-slate-800 bg-slate-950/90 p-6 shadow-[0_24px_80px_rgba(0,0,0,0.35)]">
      <div className="max-w-3xl space-y-4">
        <p className="text-sm uppercase tracking-[0.25em] text-amber-300">기존 Binary 생성기</p>
        <h2 className="text-2xl font-semibold text-white">Binary/HEX/B64 생성 기능 보존</h2>
        <p className="text-sm leading-6 text-slate-300">
          기존 binary 생성 코드는 삭제하지 않았습니다. 마이그레이션과 비교를 위해 레거시
          백엔드를 유지하고, 기본 작업 흐름만 JSON Room State Protocol v2 생성기로 전환했습니다.
        </p>
        <div className="grid gap-4 md:grid-cols-2">
          <div className="rounded-2xl border border-slate-800 bg-slate-900/70 p-4">
            <h3 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-200">
              보존 경로
            </h3>
            <ul className="mt-3 space-y-2 text-sm text-slate-400">
              <li>`app/api/generate-binary/route.ts`</li>
              <li>`api/generate.py`</li>
            </ul>
          </div>
          <div className="rounded-2xl border border-slate-800 bg-slate-900/70 p-4">
            <h3 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-200">
              용도
            </h3>
            <p className="mt-3 text-sm leading-6 text-slate-400">
              프론트 기본 출력은 JSON MQTT 페이로드이며, 이 경로는 기존 binary 결과 비교용으로
              유지됩니다.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}
