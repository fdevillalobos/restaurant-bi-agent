import React from "react";
import ReactDOM from "react-dom/client";
import { api } from "./api";
import { InviteAccept } from "./components/InviteAccept";
import { Login } from "./components/Login";
import { Workspace } from "./components/Workspace";
import type { Language } from "./i18n";
import { initialLanguage, normalizeLanguage, storeLanguage } from "./i18n";
import type { MeResponse } from "./types";
import "./styles.css";

function App() {
  const [language, setLanguageState] = React.useState<Language>(() => initialLanguage());
  const [me, setMe] = React.useState<MeResponse | null>(null);
  const [loading, setLoading] = React.useState(true);

  function setLanguage(language: Language) {
    setLanguageState(language);
    storeLanguage(language);
  }

  React.useEffect(() => {
    storeLanguage(language);
  }, []);

  React.useEffect(() => {
    api<MeResponse>("/api/me")
      .then((data) => {
        setMe(data);
        setLanguage(normalizeLanguage(data.language));
      })
      .catch(() => undefined)
      .finally(() => setLoading(false));
  }, []);

  const inviteMatch = window.location.pathname.match(/^\/invite\/([^/]+)$/);
  if (inviteMatch) {
    return <InviteAccept token={decodeURIComponent(inviteMatch[1])} language={language} onLanguageChange={setLanguage} />;
  }

  if (loading) {
    return (
      <main className="grid min-h-screen place-items-center bg-background">
        <div className="grid gap-3 text-center">
          <div className="mx-auto h-9 w-9 animate-pulse rounded-md bg-primary" />
          <p className="text-sm text-muted-foreground">{language === "es" ? "Cargando Vera" : "Loading Vera"}</p>
        </div>
      </main>
    );
  }

  if (!me) return <Login language={language} onLanguageChange={setLanguage} onLogin={(nextMe) => { setMe(nextMe); setLanguage(normalizeLanguage(nextMe.language)); }} />;
  return <Workspace initialMe={me} language={language} onLanguageChange={setLanguage} />;
}

ReactDOM.createRoot(document.getElementById("root")!).render(<App />);
