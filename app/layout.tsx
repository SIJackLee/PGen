import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "성일기전 프로토콜 자동생성기",
  description: "MQTT CONNECT / PUBLISH 데이터 생성 도구 (v2 데이터폼)",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="ko">
      <body className="min-h-screen bg-gray-950 text-gray-100 font-mono">
        {children}
      </body>
    </html>
  );
}
