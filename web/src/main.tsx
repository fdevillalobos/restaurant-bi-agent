import React from "react";
import ReactDOM from "react-dom/client";
import { api } from "./api";
import { Login } from "./components/Login";
import { Workspace } from "./components/Workspace";
import type { MeResponse } from "./types";
import "./styles.css";

function App() {
  const [me, setMe] = React.useState<MeResponse | null>(null);
  const [loading, setLoading] = React.useState(true);

  React.useEffect(() => {
    api<MeResponse>("/api/me")
      .then((data) => setMe(data))
      .catch(() => undefined)
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <main className="grid min-h-screen place-items-center bg-background">
        <div className="grid gap-3 text-center">
          <div className="mx-auto h-9 w-9 animate-pulse rounded-md bg-primary" />
          <p className="text-sm text-muted-foreground">Loading Vera</p>
        </div>
      </main>
    );
  }

  if (!me) return <Login onLogin={setMe} />;
  return <Workspace initialMe={me} />;
}

ReactDOM.createRoot(document.getElementById("root")!).render(<App />);
