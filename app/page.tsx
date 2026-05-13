"use client";

import { useState } from "react";
import JsonProtocolGenerator from "@/app/components/JsonProtocolGenerator";
import LegacyBinaryGenerator from "@/app/components/LegacyBinaryGenerator";

const tabs = [
  { key: "json", label: "JSON 프로토콜 생성기" },
  { key: "legacy", label: "기존 Binary 생성기" },
] as const;

type TabKey = (typeof tabs)[number]["key"];

export default function Home() {
  const [activeTab, setActiveTab] = useState<TabKey>("json");

  return (
    <main className="min-h-screen bg-[linear-gradient(180deg,#020617_0%,#08111f_45%,#020617_100%)]">
      <div className="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
        <header className="mb-8 rounded-[32px] border border-slate-800 bg-[radial-gradient(circle_at_top_left,#164e63_0%,#020617_55%)] p-8 shadow-[0_30px_120px_rgba(0,0,0,0.45)]">
          <p className="text-sm uppercase tracking-[0.35em] text-cyan-200">
            프로토콜 생성기
          </p>
          <h1 className="mt-3 text-4xl font-semibold tracking-tight text-white">
            JSON Room State Protocol v2
          </h1>
          <p className="mt-3 max-w-3xl text-sm leading-7 text-slate-300">
            기본 출력은 MQTT STATE 토픽, JSON 페이로드, mosquitto publish 명령, ACK 구독
            명령 기준입니다. 기존 Binary/HEX/B64 코드는 별도 탭에서 유지됩니다.
          </p>
        </header>

        <div className="mb-6 flex flex-wrap gap-3">
          {tabs.map((tab) => {
            const active = activeTab === tab.key;
            return (
              <button
                key={tab.key}
                type="button"
                onClick={() => setActiveTab(tab.key)}
                className={`rounded-full px-5 py-3 text-sm font-medium transition ${
                  active
                    ? "bg-white text-slate-950"
                    : "border border-slate-700 bg-slate-950/70 text-slate-300 hover:border-cyan-400 hover:text-cyan-200"
                }`}
              >
                {tab.label}
              </button>
            );
          })}
        </div>

        {activeTab === "json" ? <JsonProtocolGenerator /> : <LegacyBinaryGenerator />}
      </div>
    </main>
  );
}
