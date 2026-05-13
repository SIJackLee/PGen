export default function LegacyBinaryGenerator() {
  return (
    <section className="rounded-[28px] border border-slate-800 bg-slate-950/90 p-6 shadow-[0_24px_80px_rgba(0,0,0,0.35)]">
      <div className="max-w-3xl space-y-4">
        <p className="text-sm uppercase tracking-[0.25em] text-amber-300">Legacy Binary Generator</p>
        <h2 className="text-2xl font-semibold text-white">Binary/HEX/B64 generation preserved</h2>
        <p className="text-sm leading-6 text-slate-300">
          Existing binary generation code was not removed. The legacy backend remains available for
          migration and comparison, but the default workflow is now the JSON Room State Protocol v2
          generator.
        </p>
        <div className="grid gap-4 md:grid-cols-2">
          <div className="rounded-2xl border border-slate-800 bg-slate-900/70 p-4">
            <h3 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-200">
              Preserved paths
            </h3>
            <ul className="mt-3 space-y-2 text-sm text-slate-400">
              <li>`app/api/generate-binary/route.ts`</li>
              <li>`api/generate.py`</li>
            </ul>
          </div>
          <div className="rounded-2xl border border-slate-800 bg-slate-900/70 p-4">
            <h3 className="text-sm font-semibold uppercase tracking-[0.2em] text-slate-200">
              Purpose
            </h3>
            <p className="mt-3 text-sm leading-6 text-slate-400">
              Use this preserved binary route for backward comparison while front-end default output
              is the JSON MQTT payload.
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}
