import React from "react";
import {
  ActionBarPrimitive,
  AssistantRuntimeProvider,
  AuiIf,
  ComposerPrimitive,
  MessagePrimitive,
  ThreadPrimitive,
  useExternalStoreRuntime,
  useMessage,
  type AppendMessage
} from "@assistant-ui/react";
import type { ThreadAssistantMessagePart, ThreadUserMessagePart } from "@assistant-ui/react";
import { ArrowDown, ArrowUp, Bug, Check, Copy, Database, LogOut, Menu, PanelRightOpen, RefreshCw, Settings as SettingsIcon, Square } from "lucide-react";
import { api, toChatMessages } from "../api";
import type { Language } from "../i18n";
import { languageNames, t } from "../i18n";
import { cn } from "../lib/utils";
import type { ChatMessage, ChatResponse, MeResponse, StoredChatMessage, VeraDebug } from "../types";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { ScrollArea } from "./ui/scroll-area";
import { Sheet, SheetContent, SheetTitle } from "./ui/sheet";
import { Skeleton } from "./ui/skeleton";
import { Switch } from "./ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "./ui/tabs";
import { TooltipProvider } from "./ui/tooltip";
import { VeraResponseBlocks } from "./vera/VeraBlocks";
import { Settings } from "./Settings";

function appendMessageText(message: AppendMessage) {
  if (typeof message.content === "string") return message.content;
  return message.content
    .map((part) => {
      if (part.type === "text") return part.text;
      return "";
    })
    .join("")
    .trim();
}

function stateMessageText(content: readonly (ThreadAssistantMessagePart | ThreadUserMessagePart)[]) {
  return content
    .map((part) => {
      if (part.type === "text") return part.text;
      return "";
    })
    .join("");
}

function toAssistantMessage(message: ChatMessage) {
  return {
    id: message.id,
    role: message.role === "status" ? "assistant" as const : message.role,
    content: message.content,
    createdAt: message.created_at ? new Date(message.created_at) : undefined
  };
}

function DebugInspector({
  open,
  onOpenChange,
  latestDebug,
  selected,
  messages,
  includeDebug,
  language
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  latestDebug?: VeraDebug;
  selected: string[];
  messages: ChatMessage[];
  includeDebug: boolean;
  language: Language;
}) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent>
        <SheetTitle className="mb-1 text-xl font-semibold">{t(language, "inspector")}</SheetTitle>
        <p className="mb-4 text-sm text-muted-foreground">{t(language, "debugDescription")}</p>
        <Tabs defaultValue="query">
          <TabsList>
            <TabsTrigger value="query">{t(language, "query")}</TabsTrigger>
            <TabsTrigger value="context">{t(language, "context")}</TabsTrigger>
          </TabsList>
          <TabsContent value="query" className="min-h-0 flex-1">
            {latestDebug ? (
              <div className="grid gap-3">
                {latestDebug.error && <p className="rounded-md border border-destructive/20 bg-destructive/10 p-3 text-sm text-destructive">{latestDebug.error}</p>}
                {latestDebug.failed_query && (
                  <div className="rounded-md border border-border p-3">
                    <p className="mb-2 text-sm font-semibold">{t(language, "failedQuery")}</p>
                    <pre className="debug-pre">{JSON.stringify(latestDebug.failed_query, null, 2)}</pre>
                  </div>
                )}
                {latestDebug.queries?.length ? (
                  latestDebug.queries.map((query, index) => (
                    <div className="rounded-md border border-border p-3" key={`${query.sql}-${index}`}>
                      <div className="mb-2 flex items-center justify-between gap-2">
                        <p className="text-sm font-semibold">{query.purpose || `${t(language, "queryN")} ${index + 1}`}</p>
                        <Badge variant="outline">{query.row_count} {t(language, "rows")}</Badge>
                      </div>
                      <pre className="debug-pre">{query.sql}</pre>
                    </div>
                  ))
                ) : latestDebug.sql ? (
                  <pre className="debug-pre">{latestDebug.sql}</pre>
                ) : null}
              </div>
            ) : (
              <div className="rounded-md border border-dashed border-border p-4 text-sm text-muted-foreground">
                {includeDebug ? t(language, "queryMetadataPrompt") : t(language, "debugOffPrompt")}
              </div>
            )}
          </TabsContent>
          <TabsContent value="context">
            <div className="grid gap-3 text-sm">
              <div className="rounded-md border border-border p-3">
                <p className="mb-2 font-semibold">{t(language, "selectedRestaurants")}</p>
                <div className="flex flex-wrap gap-2">
                  {selected.map((restaurant) => <Badge key={restaurant} variant="outline">{restaurant}</Badge>)}
                </div>
              </div>
              <div className="rounded-md border border-border p-3">
                <p className="mb-2 font-semibold">{t(language, "visibleThread")}</p>
                <p className="text-muted-foreground">{messages.filter((message) => message.role === "assistant").length} {t(language, "assistantResponsesLoaded")}</p>
              </div>
            </div>
          </TabsContent>
        </Tabs>
      </SheetContent>
    </Sheet>
  );
}

function Composer({ language }: { language: Language }) {
  return (
    <ComposerPrimitive.Root className="relative flex w-full flex-col rounded-md border border-border bg-background p-2 shadow-workspace focus-within:ring-2 focus-within:ring-ring">
      <ComposerPrimitive.Input
        submitMode="enter"
        rows={1}
        placeholder={t(language, "askPlaceholder")}
        className="max-h-36 min-h-12 resize-none bg-transparent px-2 py-2 text-sm outline-none"
      />
      <div className="flex items-center justify-between border-t border-border pt-2">
        <p className="px-2 text-xs text-muted-foreground">{t(language, "composerHint")}</p>
        <AuiIf condition={(s) => !s.thread.isRunning}>
          <ComposerPrimitive.Send asChild>
            <Button size="icon" aria-label={t(language, "sendMessage")}><ArrowUp className="h-4 w-4" /></Button>
          </ComposerPrimitive.Send>
        </AuiIf>
        <AuiIf condition={(s) => s.thread.isRunning}>
          <ComposerPrimitive.Cancel asChild>
            <Button size="icon" aria-label={t(language, "stopVera")}><Square className="h-3 w-3 fill-current" /></Button>
          </ComposerPrimitive.Cancel>
        </AuiIf>
      </div>
    </ComposerPrimitive.Root>
  );
}

function MessageActions({ language }: { language: Language }) {
  return (
    <ActionBarPrimitive.Root hideWhenRunning autohide="not-last" className="mt-2 flex gap-1 text-muted-foreground">
      <ActionBarPrimitive.Copy asChild>
        <Button variant="ghost" size="sm">
          <Copy className="h-3.5 w-3.5" />
          {t(language, "copy")}
        </Button>
      </ActionBarPrimitive.Copy>
      <ActionBarPrimitive.Reload asChild>
        <Button variant="ghost" size="sm"><RefreshCw className="h-3.5 w-3.5" /> {t(language, "retry")}</Button>
      </ActionBarPrimitive.Reload>
    </ActionBarPrimitive.Root>
  );
}

function ThreadMessage({ messagesById, onAsk, language }: { messagesById: Map<string, ChatMessage>; onAsk: (text: string) => void; language: Language }) {
  const message = useMessage();
  const source = messagesById.get(message.id);
  const content = source?.content || stateMessageText(message.content);

  if (source?.role === "status") {
    return (
      <MessagePrimitive.Root className="mx-auto w-full max-w-4xl">
        <div className="flex items-center gap-3 rounded-md border border-dashed border-border bg-card p-4 text-sm text-muted-foreground">
          <Skeleton className="h-3 w-3 rounded-full" />
          {content}
        </div>
      </MessagePrimitive.Root>
    );
  }

  if (message.role === "user") {
    return (
      <MessagePrimitive.Root className="mx-auto grid w-full max-w-4xl justify-items-end px-4">
        <div className="max-w-[80%] rounded-md bg-primary px-4 py-3 text-sm leading-6 text-primary-foreground shadow-sm">{content}</div>
        <MessageActions language={language} />
      </MessagePrimitive.Root>
    );
  }

  return (
    <MessagePrimitive.Root className="mx-auto w-full max-w-4xl px-4">
      {source?.response ? (
        <VeraResponseBlocks response={source.response} onAsk={onAsk} language={language} />
      ) : (
        <div className="rounded-md border border-border bg-card p-4 text-sm leading-7">{content}</div>
      )}
      <MessageActions language={language} />
    </MessagePrimitive.Root>
  );
}

function AssistantThread({
  messages,
  isRunning,
  isSendDisabled,
  onAsk,
  language
}: {
  messages: ChatMessage[];
  isRunning: boolean;
  isSendDisabled: boolean;
  onAsk: (text: string) => Promise<void> | void;
  language: Language;
}) {
  const runtime = useExternalStoreRuntime<ChatMessage>({
    messages,
    isRunning,
    isSendDisabled,
    convertMessage: toAssistantMessage,
    onNew: async (message) => {
      const text = appendMessageText(message);
      if (text) await onAsk(text);
    },
    onReload: async () => {
      const lastUser = [...messages].reverse().find((message) => message.role === "user");
      if (lastUser) await onAsk(lastUser.content);
    },
    unstable_capabilities: { copy: true }
  });
  const messagesById = React.useMemo(() => new Map(messages.map((message) => [message.id, message])), [messages]);

  return (
    <AssistantRuntimeProvider runtime={runtime}>
      <ThreadPrimitive.Root className="flex h-full min-h-0 flex-col bg-background">
        <ThreadPrimitive.Viewport className="flex-1 overflow-y-auto scroll-smooth" turnAnchor="top">
          <div className="mx-auto flex min-h-full w-full max-w-5xl flex-col gap-5 px-2 py-6">
            {messages.length === 0 && (
              <div className="mx-auto grid w-full max-w-4xl gap-4 px-4 pt-10">
                <div>
                  <p className="mb-2 text-xs font-semibold uppercase text-emerald-700">{t(language, "emptyEyebrow")}</p>
                  <h2 className="text-3xl font-semibold tracking-normal">{t(language, "emptyTitle")}</h2>
                  <p className="mt-3 max-w-2xl text-muted-foreground">{t(language, "emptySubtitle")}</p>
                </div>
                <div className="grid gap-2 sm:grid-cols-2">
                  {[t(language, "promptSalesChange"), t(language, "promptProducts"), t(language, "promptDelivery"), t(language, "promptNext")].map((prompt) => (
                    <Button key={prompt} variant="outline" className="h-auto justify-start whitespace-normal p-4 text-left" onClick={() => void onAsk(prompt)}>
                      {prompt}
                    </Button>
                  ))}
                </div>
              </div>
            )}
            <ThreadPrimitive.Messages>{() => <ThreadMessage messagesById={messagesById} onAsk={onAsk} language={language} />}</ThreadPrimitive.Messages>
          </div>
          <ThreadPrimitive.ViewportFooter className="sticky bottom-0 bg-gradient-to-t from-background via-background pb-4 pt-8">
            <div className="mx-auto w-full max-w-4xl px-4">
              <ThreadPrimitive.ScrollToBottom asChild>
                <Button variant="outline" size="icon" className="absolute -top-6 left-1/2 h-8 w-8 -translate-x-1/2 rounded-full" aria-label={t(language, "scrollToBottom")}>
                  <ArrowDown className="h-4 w-4" />
                </Button>
              </ThreadPrimitive.ScrollToBottom>
              <Composer language={language} />
            </div>
          </ThreadPrimitive.ViewportFooter>
        </ThreadPrimitive.Viewport>
      </ThreadPrimitive.Root>
    </AssistantRuntimeProvider>
  );
}

function Sidebar({
  me,
  selected,
  messages,
  includeDebug,
  onDebugChange,
  language,
  onLanguageChange,
  onSelect,
  onLogout,
  onOpenInspector,
  onOpenSettings,
  onOpenMenu,
  className,
  mode = "compact"
}: {
  me: MeResponse;
  selected: string[];
  messages: ChatMessage[];
  includeDebug: boolean;
  onDebugChange: (value: boolean) => void;
  language: Language;
  onLanguageChange: (language: Language) => void;
  onSelect: (next: string[]) => void;
  onLogout: () => void;
  onOpenInspector: () => void;
  onOpenSettings: () => void;
  onOpenMenu?: () => void;
  className?: string;
  mode?: "compact" | "menu";
}) {
  const recentQuestions = [...messages].filter((message) => message.role === "user").slice(-5).reverse();
  const showRecent = mode === "compact";
  const showMenuControls = mode === "menu";

  return (
    <aside className={cn("workspace-sidebar flex min-h-0 flex-col border-r border-border bg-card", className)}>
      <div className="sidebar-brand border-b border-border p-5">
        <div className="mb-4 flex items-start justify-between gap-3">
          <div className="brand-icon flex h-10 w-10 items-center justify-center rounded-md bg-primary text-primary-foreground">
            <Database className="h-5 w-5" />
          </div>
          {onOpenMenu && (
            <Button variant="outline" size="icon" onClick={onOpenMenu} aria-label={t(language, "openMenu")}>
              <Menu className="h-4 w-4" />
            </Button>
          )}
        </div>
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase text-emerald-700">Vera BI Analyst</p>
          <h1 className="mt-1 truncate text-xl font-semibold tracking-normal">{me.dsn?.name || t(language, "restaurantGroup")}</h1>
          <p className="mt-1 truncate text-sm text-muted-foreground">{me.user.email}</p>
        </div>
      </div>
      <ScrollArea className="sidebar-scroll min-h-0 flex-1">
        {showMenuControls && (
          <section className="restaurant-section grid gap-3 p-5">
            <div className="flex items-center justify-between">
              <h2 className="text-sm font-semibold">{t(language, "restaurants")}</h2>
              <Badge variant="outline">{selected.length || 0} {t(language, "selected")}</Badge>
            </div>
            <div className="restaurant-list grid gap-2">
              {me.restaurants.map((restaurant) => {
                const active = selected.includes(restaurant);
                return (
                  <button
                    className={cn(
                      "flex min-h-9 items-center justify-between rounded-md border border-border px-3 py-2 text-left text-sm transition-colors hover:bg-accent",
                      active && "border-emerald-500 bg-emerald-50 text-emerald-800"
                    )}
                    key={restaurant}
                    onClick={() => {
                      if (active && selected.length <= 1) return;
                      onSelect(active ? selected.filter((name) => name !== restaurant) : [...selected, restaurant]);
                    }}
                  >
                    <span className="truncate">{restaurant}</span>
                    {active && <Check className="h-4 w-4" />}
                  </button>
                );
              })}
            </div>
          </section>
        )}
        {showRecent && (
          <section className="recent-section grid gap-3 p-5">
            <h2 className="text-sm font-semibold">{t(language, "recentQuestions")}</h2>
            {recentQuestions.length > 0 ? recentQuestions.map((message) => (
              <p className="line-clamp-2 rounded-md bg-muted p-2 text-xs text-muted-foreground" key={message.id}>{message.content}</p>
            )) : <p className="text-sm text-muted-foreground">{t(language, "noQuestionsYet")}</p>}
          </section>
        )}
      </ScrollArea>
      {showMenuControls ? (
        <div className="sidebar-controls grid gap-2 border-t border-border p-5">
          <label className="grid gap-2 rounded-md border border-border px-3 py-2 text-sm">
            <span className="font-medium">{t(language, "language")}</span>
            <select
              className="h-9 rounded-md border border-input bg-background px-2 outline-none focus:ring-2 focus:ring-ring"
              value={language}
              onChange={(event) => onLanguageChange(event.target.value as Language)}
            >
              <option value="en">{languageNames.en}</option>
              <option value="es">{languageNames.es}</option>
            </select>
          </label>
          <label className="flex items-center justify-between gap-3 rounded-md border border-border px-3 py-2 text-sm">
            <span className="flex items-center gap-2"><Bug className="h-4 w-4" /> {t(language, "debug")}</span>
            <Switch checked={includeDebug} onCheckedChange={onDebugChange} />
          </label>
          <Button className="sidebar-inspector" variant="outline" onClick={onOpenInspector}><PanelRightOpen className="h-4 w-4" /> <span className="control-label">{t(language, "inspector")}</span></Button>
          {me.capabilities.settings && <Button className="menu-only-mobile md:hidden" variant="outline" onClick={onOpenSettings}><SettingsIcon className="h-4 w-4" /> <span className="control-label">{t(language, "settings")}</span></Button>}
          <Button className="sidebar-logout" variant="ghost" onClick={onLogout}><LogOut className="h-4 w-4" /> <span className="control-label">{t(language, "logout")}</span></Button>
        </div>
      ) : (
        me.capabilities.settings && (
          <div className="sidebar-footer grid gap-2 border-t border-border p-5">
            <Button className="sidebar-settings" variant="outline" onClick={onOpenSettings}><SettingsIcon className="h-4 w-4" /> <span className="control-label">{t(language, "settings")}</span></Button>
          </div>
        )
      )}
    </aside>
  );
}

export function Workspace({
  initialMe,
  language,
  onLanguageChange
}: {
  initialMe: MeResponse;
  language: Language;
  onLanguageChange: (language: Language) => void;
}) {
  const [me, setMe] = React.useState(initialMe);
  const [selected, setSelected] = React.useState<string[]>(initialMe.selected_restaurants);
  const [messages, setMessages] = React.useState<ChatMessage[]>([]);
  const [debugOpen, setDebugOpen] = React.useState(false);
  const [includeDebug, setIncludeDebug] = React.useState(false);
  const [loadingHistory, setLoadingHistory] = React.useState(true);
  const [running, setRunning] = React.useState(false);
  const [view, setView] = React.useState<"chat" | "settings">("chat");
  const [controlMenuOpen, setControlMenuOpen] = React.useState(false);

  React.useEffect(() => {
    api<{ messages: StoredChatMessage[] }>("/api/chat/history")
      .then((history) => setMessages(toChatMessages(history.messages)))
      .catch(() => undefined)
      .finally(() => setLoadingHistory(false));
  }, []);

  async function saveSelection(next: string[]) {
    const data = await api<MeResponse>("/api/restaurants/select", {
      method: "POST",
      body: JSON.stringify({ restaurant_names: next })
    });
    setMe(data);
    setSelected(data.selected_restaurants);
  }

  async function logout() {
    await api("/api/logout", { method: "POST" });
    window.location.reload();
  }

  async function changeLanguage(next: Language) {
    onLanguageChange(next);
    const data = await api<MeResponse>("/api/language", {
      method: "POST",
      body: JSON.stringify({ language: next })
    });
    setMe(data);
  }

  async function ask(text: string) {
    const question = text.trim();
    if (!question || selected.length === 0 || running) return;
    const userMessage: ChatMessage = { id: crypto.randomUUID(), role: "user", content: question };
    const statusMessage: ChatMessage = { id: crypto.randomUUID(), role: "status", content: t(language, "veraPlanning") };
    setRunning(true);
    setMessages((current) => [...current, userMessage, statusMessage]);
    try {
      const response = await api<ChatResponse>("/api/chat", {
        method: "POST",
        body: JSON.stringify({ message: question, restaurant_names: selected, include_debug: includeDebug, language })
      });
      setMessages((current) => current.filter((message) => message.id !== statusMessage.id).concat({
        id: crypto.randomUUID(),
        role: "assistant",
        content: response.message,
        response
      }));
    } catch (err) {
      const fallback: ChatResponse = {
        action: "answer",
        message: err instanceof Error ? err.message : t(language, "veraCouldNotAnswer"),
        tables: [],
        charts: [],
        recommendations: [],
        suggested_next_questions: [],
        debug: includeDebug ? { error: err instanceof Error ? err.message : t(language, "unknownError") } : undefined
      };
      setMessages((current) => current.filter((message) => message.id !== statusMessage.id).concat({
        id: crypto.randomUUID(),
        role: "assistant",
        content: fallback.message,
        response: fallback
      }));
    } finally {
      setRunning(false);
    }
  }

  const latestDebug = [...messages].reverse().find((message) => message.response?.debug)?.response?.debug;

  if (view === "settings") {
    return (
      <TooltipProvider>
        <Settings me={me} language={language} onBack={() => setView("chat")} />
      </TooltipProvider>
    );
  }

  return (
      <TooltipProvider>
      <main className="workspace-shell grid h-screen min-h-0 grid-cols-[320px_minmax(0,1fr)] bg-background text-foreground max-md:grid-cols-1">
        <Sidebar
          className="desktop-sidebar"
          me={me}
          selected={selected}
          messages={messages}
          includeDebug={includeDebug}
          onDebugChange={setIncludeDebug}
          language={language}
          onLanguageChange={(next) => void changeLanguage(next)}
          onSelect={(next) => void saveSelection(next)}
          onLogout={() => void logout()}
          onOpenInspector={() => setDebugOpen(true)}
          onOpenSettings={() => setView("settings")}
          onOpenMenu={() => setControlMenuOpen(true)}
        />
        <Sheet open={controlMenuOpen} onOpenChange={setControlMenuOpen}>
          <SheetContent className="control-menu-sheet left-0 right-auto max-w-[22rem] border-l-0 border-r p-0">
            <SheetTitle className="sr-only">{t(language, "menu")}</SheetTitle>
            <Sidebar
              className="menu-sidebar"
              mode="menu"
              me={me}
              selected={selected}
              messages={messages}
              includeDebug={includeDebug}
              onDebugChange={setIncludeDebug}
              language={language}
              onLanguageChange={(next) => void changeLanguage(next)}
              onSelect={(next) => void saveSelection(next)}
              onLogout={() => void logout()}
              onOpenInspector={() => {
                setControlMenuOpen(false);
                setDebugOpen(true);
              }}
              onOpenSettings={() => {
                setControlMenuOpen(false);
                setView("settings");
              }}
            />
          </SheetContent>
        </Sheet>
        <section className="chat-pane grid min-h-0 grid-rows-[auto_minmax(0,1fr)]">
          <header className="chat-header flex items-center justify-between gap-3 border-b border-border bg-card px-5 py-3">
            <div className="flex min-w-0 items-center gap-3">
              <Button className="mobile-menu-button md:hidden" variant="outline" size="icon" onClick={() => setControlMenuOpen(true)} aria-label={t(language, "openMenu")}>
                <Menu className="h-4 w-4" />
              </Button>
              <div className="min-w-0">
              <p className="text-sm font-semibold">{t(language, "analystWorkspace")}</p>
                <p className="truncate text-xs text-muted-foreground">{selected.length ? selected.join(", ") : t(language, "selectAtLeastOneRestaurant")}</p>
              </div>
            </div>
            <div />
          </header>
          {loadingHistory ? (
            <div className="mx-auto grid w-full max-w-4xl gap-4 p-8">
              <Skeleton className="h-28" />
              <Skeleton className="h-44" />
            </div>
          ) : (
            <AssistantThread messages={messages} isRunning={running} isSendDisabled={selected.length === 0 || running} onAsk={ask} language={language} />
          )}
        </section>
        <DebugInspector open={debugOpen} onOpenChange={setDebugOpen} latestDebug={latestDebug} selected={selected} messages={messages} includeDebug={includeDebug} language={language} />
      </main>
    </TooltipProvider>
  );
}
