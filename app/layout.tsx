import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Protocol Generator | JSON Room State Protocol v2",
  description: "Generate MQTT STATE topics, JSON payloads, and mosquitto test commands.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="ko">
      <body>{children}</body>
    </html>
  );
}
