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
import { ArrowDown, ArrowUp, Bug, Check, Copy, Database, LogOut, PanelRightOpen, RefreshCw, Square } from "lucide-react";
import { api, toChatMessages } from "../api";
import { cn } from "../lib/utils";
import type { ChatMessage, ChatResponse, MeResponse, StoredChatMessage, VeraDebug } from "../types";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { ScrollArea } from "./ui/scroll-area";
import { Sheet, SheetContent, SheetTitle } from "./ui/sheet";
import { Skeleton } from "./ui/skeleton";
import { Switch } from "./ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "./ui/tabs";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "./ui/tooltip";
import { VeraResponseBlocks } from "./vera/VeraBlocks";

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
  includeDebug
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  latestDebug?: VeraDebug;
  selected: string[];
  messages: ChatMessage[];
  includeDebug: boolean;
}) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent>
        <SheetTitle className="mb-1 text-xl font-semibold">Inspector</SheetTitle>
        <p className="mb-4 text-sm text-muted-foreground">Debug output is only requested from Vera when Debug is enabled.</p>
        <Tabs defaultValue="query">
          <TabsList>
            <TabsTrigger value="query">Query</TabsTrigger>
            <TabsTrigger value="context">Context</TabsTrigger>
          </TabsList>
          <TabsContent value="query" className="min-h-0 flex-1">
            {latestDebug ? (
              <div className="grid gap-3">
                {latestDebug.error && <p className="rounded-md border border-destructive/20 bg-destructive/10 p-3 text-sm text-destructive">{latestDebug.error}</p>}
                {latestDebug.failed_query && (
                  <div className="rounded-md border border-border p-3">
                    <p className="mb-2 text-sm font-semibold">Failed query</p>
                    <pre className="debug-pre">{JSON.stringify(latestDebug.failed_query, null, 2)}</pre>
                  </div>
                )}
                {latestDebug.queries?.length ? (
                  latestDebug.queries.map((query, index) => (
                    <div className="rounded-md border border-border p-3" key={`${query.sql}-${index}`}>
                      <div className="mb-2 flex items-center justify-between gap-2">
                        <p className="text-sm font-semibold">{query.purpose || `Query ${index + 1}`}</p>
                        <Badge variant="outline">{query.row_count} rows</Badge>
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
                {includeDebug ? "Ask a question to capture SQL/query metadata." : "Turn on Debug, then ask a question to capture SQL/query metadata."}
              </div>
            )}
          </TabsContent>
          <TabsContent value="context">
            <div className="grid gap-3 text-sm">
              <div className="rounded-md border border-border p-3">
                <p className="mb-2 font-semibold">Selected restaurants</p>
                <div className="flex flex-wrap gap-2">
                  {selected.map((restaurant) => <Badge key={restaurant} variant="outline">{restaurant}</Badge>)}
                </div>
              </div>
              <div className="rounded-md border border-border p-3">
                <p className="mb-2 font-semibold">Visible thread</p>
                <p className="text-muted-foreground">{messages.filter((message) => message.role === "assistant").length} assistant responses loaded from the web transcript.</p>
              </div>
            </div>
          </TabsContent>
        </Tabs>
      </SheetContent>
    </Sheet>
  );
}

function Composer() {
  return (
    <ComposerPrimitive.Root className="relative flex w-full flex-col rounded-md border border-border bg-background p-2 shadow-workspace focus-within:ring-2 focus-within:ring-ring">
      <ComposerPrimitive.Input
        submitMode="enter"
        rows={1}
        placeholder="Ask Vera a business question..."
        className="max-h-36 min-h-12 resize-none bg-transparent px-2 py-2 text-sm outline-none"
      />
      <div className="flex items-center justify-between border-t border-border pt-2">
        <p className="px-2 text-xs text-muted-foreground">Enter sends. Shift+Enter adds a new line.</p>
        <AuiIf condition={(s) => !s.thread.isRunning}>
          <ComposerPrimitive.Send asChild>
            <Button size="icon" aria-label="Send message"><ArrowUp className="h-4 w-4" /></Button>
          </ComposerPrimitive.Send>
        </AuiIf>
        <AuiIf condition={(s) => s.thread.isRunning}>
          <ComposerPrimitive.Cancel asChild>
            <Button size="icon" aria-label="Stop Vera"><Square className="h-3 w-3 fill-current" /></Button>
          </ComposerPrimitive.Cancel>
        </AuiIf>
      </div>
    </ComposerPrimitive.Root>
  );
}

function MessageActions() {
  return (
    <ActionBarPrimitive.Root hideWhenRunning autohide="not-last" className="mt-2 flex gap-1 text-muted-foreground">
      <ActionBarPrimitive.Copy asChild>
        <Button variant="ghost" size="sm">
          <Copy className="h-3.5 w-3.5" />
          Copy
        </Button>
      </ActionBarPrimitive.Copy>
      <ActionBarPrimitive.Reload asChild>
        <Button variant="ghost" size="sm"><RefreshCw className="h-3.5 w-3.5" /> Retry</Button>
      </ActionBarPrimitive.Reload>
    </ActionBarPrimitive.Root>
  );
}

function ThreadMessage({ messagesById, onAsk }: { messagesById: Map<string, ChatMessage>; onAsk: (text: string) => void }) {
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
        <MessageActions />
      </MessagePrimitive.Root>
    );
  }

  return (
    <MessagePrimitive.Root className="mx-auto w-full max-w-4xl px-4">
      {source?.response ? (
        <VeraResponseBlocks response={source.response} onAsk={onAsk} />
      ) : (
        <div className="rounded-md border border-border bg-card p-4 text-sm leading-7">{content}</div>
      )}
      <MessageActions />
    </MessagePrimitive.Root>
  );
}

function AssistantThread({
  messages,
  isRunning,
  isSendDisabled,
  onAsk
}: {
  messages: ChatMessage[];
  isRunning: boolean;
  isSendDisabled: boolean;
  onAsk: (text: string) => Promise<void> | void;
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
                  <p className="mb-2 text-xs font-semibold uppercase text-emerald-700">Vera BI Analyst</p>
                  <h2 className="text-3xl font-semibold tracking-normal">Ask about sales, products, channels, covers, or trends.</h2>
                  <p className="mt-3 max-w-2xl text-muted-foreground">Start with an exact metric or a broad diagnostic question. Vera will ask for clarification only when the business intent materially changes the analysis.</p>
                </div>
                <div className="grid gap-2 sm:grid-cols-2">
                  {["What changed in sales last month?", "Which products drove revenue last week?", "Compare delivery vs eat-in this month.", "What should I investigate next?"].map((prompt) => (
                    <Button key={prompt} variant="outline" className="h-auto justify-start whitespace-normal p-4 text-left" onClick={() => void onAsk(prompt)}>
                      {prompt}
                    </Button>
                  ))}
                </div>
              </div>
            )}
            <ThreadPrimitive.Messages>{() => <ThreadMessage messagesById={messagesById} onAsk={onAsk} />}</ThreadPrimitive.Messages>
          </div>
          <ThreadPrimitive.ViewportFooter className="sticky bottom-0 bg-gradient-to-t from-background via-background pb-4 pt-8">
            <div className="mx-auto w-full max-w-4xl px-4">
              <ThreadPrimitive.ScrollToBottom asChild>
                <Button variant="outline" size="icon" className="absolute -top-6 left-1/2 h-8 w-8 -translate-x-1/2 rounded-full" aria-label="Scroll to bottom">
                  <ArrowDown className="h-4 w-4" />
                </Button>
              </ThreadPrimitive.ScrollToBottom>
              <Composer />
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
  onSelect,
  onLogout,
  onOpenInspector
}: {
  me: MeResponse;
  selected: string[];
  messages: ChatMessage[];
  includeDebug: boolean;
  onDebugChange: (value: boolean) => void;
  onSelect: (next: string[]) => void;
  onLogout: () => void;
  onOpenInspector: () => void;
}) {
  const recentQuestions = [...messages].filter((message) => message.role === "user").slice(-5).reverse();

  return (
    <aside className="flex min-h-0 flex-col border-r border-border bg-card">
      <div className="border-b border-border p-5">
        <div className="mb-4 flex h-10 w-10 items-center justify-center rounded-md bg-primary text-primary-foreground">
          <Database className="h-5 w-5" />
        </div>
        <p className="text-xs font-semibold uppercase text-emerald-700">Vera BI Analyst</p>
        <h1 className="mt-1 text-xl font-semibold tracking-normal">{me.dsn?.name || "Restaurant group"}</h1>
        <p className="mt-1 truncate text-sm text-muted-foreground">{me.user.email}</p>
      </div>
      <ScrollArea className="min-h-0 flex-1">
        <section className="grid gap-3 p-5">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold">Restaurants</h2>
            <Badge variant="outline">{selected.length || 0} selected</Badge>
          </div>
          <div className="grid gap-2">
            {me.restaurants.map((restaurant) => {
              const active = selected.includes(restaurant);
              return (
                <button
                  className={cn(
                    "flex min-h-9 items-center justify-between rounded-md border border-border px-3 py-2 text-left text-sm transition-colors hover:bg-accent",
                    active && "border-emerald-500 bg-emerald-50 text-emerald-800"
                  )}
                  key={restaurant}
                  onClick={() => onSelect(active ? selected.filter((name) => name !== restaurant) : [...selected, restaurant])}
                >
                  <span className="truncate">{restaurant}</span>
                  {active && <Check className="h-4 w-4" />}
                </button>
              );
            })}
          </div>
        </section>
        <section className="grid gap-3 border-t border-border p-5">
          <h2 className="text-sm font-semibold">Recent questions</h2>
          {recentQuestions.length > 0 ? recentQuestions.map((message) => (
            <p className="line-clamp-2 rounded-md bg-muted p-2 text-xs text-muted-foreground" key={message.id}>{message.content}</p>
          )) : <p className="text-sm text-muted-foreground">No questions yet.</p>}
        </section>
      </ScrollArea>
      <div className="grid gap-2 border-t border-border p-5">
        <label className="flex items-center justify-between gap-3 rounded-md border border-border px-3 py-2 text-sm">
          <span className="flex items-center gap-2"><Bug className="h-4 w-4" /> Debug</span>
          <Switch checked={includeDebug} onCheckedChange={onDebugChange} />
        </label>
        <Button variant="outline" onClick={onOpenInspector}><PanelRightOpen className="h-4 w-4" /> Inspector</Button>
        <Button variant="ghost" onClick={onLogout}><LogOut className="h-4 w-4" /> Logout</Button>
      </div>
    </aside>
  );
}

export function Workspace({ initialMe }: { initialMe: MeResponse }) {
  const [me, setMe] = React.useState(initialMe);
  const [selected, setSelected] = React.useState<string[]>(initialMe.selected_restaurants);
  const [messages, setMessages] = React.useState<ChatMessage[]>([]);
  const [debugOpen, setDebugOpen] = React.useState(false);
  const [includeDebug, setIncludeDebug] = React.useState(false);
  const [loadingHistory, setLoadingHistory] = React.useState(true);
  const [running, setRunning] = React.useState(false);

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

  async function ask(text: string) {
    const question = text.trim();
    if (!question || selected.length === 0 || running) return;
    const userMessage: ChatMessage = { id: crypto.randomUUID(), role: "user", content: question };
    const statusMessage: ChatMessage = { id: crypto.randomUUID(), role: "status", content: "Vera is planning the analysis, querying the data, and preparing visuals." };
    setRunning(true);
    setMessages((current) => [...current, userMessage, statusMessage]);
    try {
      const response = await api<ChatResponse>("/api/chat", {
        method: "POST",
        body: JSON.stringify({ message: question, restaurant_names: selected, include_debug: includeDebug })
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
        message: err instanceof Error ? err.message : "Vera could not answer this question.",
        tables: [],
        charts: [],
        recommendations: [],
        suggested_next_questions: [],
        debug: includeDebug ? { error: err instanceof Error ? err.message : "Unknown error" } : undefined
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

  return (
    <TooltipProvider>
      <main className="grid h-screen min-h-0 grid-cols-[320px_minmax(0,1fr)] bg-background text-foreground max-md:grid-cols-1 max-md:grid-rows-[auto_minmax(0,1fr)]">
        <Sidebar
          me={me}
          selected={selected}
          messages={messages}
          includeDebug={includeDebug}
          onDebugChange={setIncludeDebug}
          onSelect={(next) => void saveSelection(next)}
          onLogout={() => void logout()}
          onOpenInspector={() => setDebugOpen(true)}
        />
        <section className="grid min-h-0 grid-rows-[auto_minmax(0,1fr)]">
          <header className="flex items-center justify-between gap-3 border-b border-border bg-card px-5 py-3">
            <div>
              <p className="text-sm font-semibold">Analyst workspace</p>
              <p className="text-xs text-muted-foreground">{selected.length ? selected.join(", ") : "Select at least one restaurant"}</p>
            </div>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button variant="outline" size="icon" onClick={() => setDebugOpen(true)} aria-label="Open inspector">
                  <PanelRightOpen className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Open inspector</TooltipContent>
            </Tooltip>
          </header>
          {loadingHistory ? (
            <div className="mx-auto grid w-full max-w-4xl gap-4 p-8">
              <Skeleton className="h-28" />
              <Skeleton className="h-44" />
            </div>
          ) : (
            <AssistantThread messages={messages} isRunning={running} isSendDisabled={selected.length === 0 || running} onAsk={ask} />
          )}
        </section>
        <DebugInspector open={debugOpen} onOpenChange={setDebugOpen} latestDebug={latestDebug} selected={selected} messages={messages} includeDebug={includeDebug} />
      </main>
    </TooltipProvider>
  );
}
