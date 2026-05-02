/**
 * Universal search modal — Cmd-K entry point for finding companies,
 * contacts, industries, or people across the whole SDR workspace.
 *
 * Actions per result kind:
 *   local_account  → open in Review Queue detail pane
 *   local_contact  → open parent account in Review Queue detail pane
 *   company        → +SDR intake via /vendors/companies/intake (same as map)
 *   person         → intake parent company + add contact row
 */
import React from "react";
import { createPortal } from "react-dom";

type SearchMeta = {
  effective: string;
  intent: string;
  corrected: string;
  company_hint: string;
  industry_hints: string[];
  title_filters: string[];
  llm_used: boolean;
};
type LocalAccount = {
  kind: "local_account";
  id: string; name: string; domain: string; industry: string;
  employee_count: number | null; signal_score: number;
  draft_count: number; contact_count: number;
  funding_stage?: string | null;
  country?: string | null;
};
type LocalContact = {
  kind: "local_contact";
  contact_id: string; full_name: string; title: string; email: string;
  account_id: string; account_name: string; account_domain: string;
};
type CompanyHit = {
  kind: "company";
  name: string; domain: string; industry: string;
  employee_count: number | null; funding_stage: string; country: string;
  linkedin_url: string; logo_url: string; short_description: string;
  source: string;
};
type PersonHit = {
  kind: "person";
  full_name: string; title: string; linkedin_url: string;
  company_name: string; company_domain: string; source: string;
};
type SearchItem = LocalAccount | LocalContact | CompanyHit | PersonHit;
type SearchGroup = {
  kind: string;
  label: string;
  items: SearchItem[];
  /** If set, only the first N items are rendered as rich cards; the rest
   *  use the compact one-line layout (industry-bulk only). */
  rich_count?: number;
  /** If true, the UI paginates this group locally (8 rows per page). */
  paginated?: boolean;
  total?: number;
  /** Apollo's universe size for this query (across all server pages).  */
  apollo_total?: number;
  /** How many Apollo-credit-pages (100 rows each) we've fetched so far. */
  apollo_page?: number;
  /** True when ``apollo_total`` > ``apollo_page * 100`` — user can spend
   *  one more credit to grab the next 100-row batch. */
  can_load_more?: boolean;
};
type SearchResponse = {
  query: string;
  normalized: SearchMeta;
  groups: SearchGroup[];
  credits_spent: { apollo: number; pdl: number; hunter: number };
  took_ms: number;
};

const RECENTS_KEY = "sdr_search_recents_v1";
function loadRecents(): string[] {
  try {
    const raw = localStorage.getItem(RECENTS_KEY);
    return raw ? (JSON.parse(raw) as string[]).slice(0, 10) : [];
  } catch { return []; }
}
function saveRecent(q: string) {
  const clean = q.trim();
  if (!clean) return;
  try {
    const prev = loadRecents().filter(x => x.toLowerCase() !== clean.toLowerCase());
    localStorage.setItem(RECENTS_KEY, JSON.stringify([clean, ...prev].slice(0, 10)));
  } catch { /* ignore */ }
}

type IntentChip = "all" | "companies" | "people" | "industries";

type Props = {
  open: boolean;
  onClose: () => void;
  onOpenAccount: (accountId: string) => void;
  onIntakeComplete?: () => void;
  flash: (msg: string) => void;
};

export function UniversalSearch({ open, onClose, onOpenAccount, onIntakeComplete, flash }: Props) {
  const [query, setQuery] = React.useState("");
  const [typesFilter, setTypesFilter] = React.useState<IntentChip>("all");
  const [selectedIndustries, setSelectedIndustries] = React.useState<string[]>([]);
  const [selectedCompanyKeys, setSelectedCompanyKeys] = React.useState<Set<string>>(new Set());
  const [response, setResponse] = React.useState<SearchResponse | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [recents, setRecents] = React.useState<string[]>(() => loadRecents());
  const [activeIdx, setActiveIdx] = React.useState(0);
  // Track intake state as a Set of keys so we can (a) hard-block
  // parallel re-fires for the same row and (b) mark rows that already
  // succeeded in this session so a second click becomes a no-op.
  const [intakeBusy, setIntakeBusy] = React.useState<Set<string>>(new Set());
  const [intakeDone, setIntakeDone] = React.useState<Set<string>>(new Set());
  const intakeInFlight = React.useRef<Set<string>>(new Set());
  const [groupPage, setGroupPage] = React.useState<Record<string, number>>({});
  const [apolloPage, setApolloPage] = React.useState(1);
  const [loadingMore, setLoadingMore] = React.useState(false);
  const inputRef = React.useRef<HTMLInputElement>(null);
  const listRef = React.useRef<HTMLDivElement>(null);

  // Focus input when opened, reset state when closed.
  React.useEffect(() => {
    if (open) {
      setActiveIdx(0);
      requestAnimationFrame(() => inputRef.current?.focus());
    } else {
      setQuery("");
      setSelectedIndustries([]);
      setSelectedCompanyKeys(new Set());
      setResponse(null);
      setLoading(false);
    }
  }, [open]);

  // Any change to query or intent filter resets us back to Apollo page 1.
  React.useEffect(() => {
    setApolloPage(1);
  }, [query, typesFilter, selectedIndustries.join("|")]);

  React.useEffect(() => {
    setSelectedCompanyKeys(new Set());
  }, [query, typesFilter, selectedIndustries.join("|"), apolloPage]);

  // Debounced search (350 ms).  Local-only stage fires on every keystroke;
  // vendor stage only fires after the debounce settles.
  React.useEffect(() => {
    if (!open) return;
    const trimmed = query.trim();
    if (!trimmed) { setResponse(null); return; }
    const t = setTimeout(async () => {
      setLoading(true);
      try {
        const qs = new URLSearchParams({
          q: trimmed,
          types: typesFilter,
          limit: "20",
          apollo_page: String(apolloPage),
          ...(selectedIndustries.length > 0 ? { industries: selectedIndustries.join(",") } : {}),
        });
        const res = await fetch(`/api/worktrigger/search?${qs}`);
        if (res.ok) {
          const data = (await res.json()) as SearchResponse;
          // If we're deep-paging (apolloPage > 1), APPEND the new rows to
          // the existing companies group so the user can keep scrolling
          // through all results they've unlocked.
          if (apolloPage > 1 && response) {
            const merged = { ...data };
            const oldCompaniesGroup = response.groups.find(g => g.kind === "companies");
            const newCompaniesGroup = data.groups.find(g => g.kind === "companies");
            if (oldCompaniesGroup && newCompaniesGroup) {
              const existing = oldCompaniesGroup.items as CompanyHit[];
              const incoming = newCompaniesGroup.items as CompanyHit[];
              const seen = new Set(existing.map(c => (c.domain || c.name).toLowerCase()));
              const deduped = incoming.filter(c => !seen.has((c.domain || c.name).toLowerCase()));
              merged.groups = data.groups.map(g =>
                g.kind === "companies" ? { ...g, items: [...existing, ...deduped] } : g,
              );
            }
            setResponse(merged);
          } else {
            setResponse(data);
            setActiveIdx(0);
            setGroupPage({});
          }
        }
      } catch { /* silent */ }
      finally { setLoading(false); setLoadingMore(false); }
    }, apolloPage > 1 ? 0 : 350);  // deep-page click is instant, debounced only for typing
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query, typesFilter, open, apolloPage, selectedIndustries.join("|")]);

  // On a fresh query, auto-seed active industry filters from LLM hints.
  React.useEffect(() => {
    if (!response || !query.trim()) return;
    if (apolloPage !== 1) return;
    if (selectedIndustries.length > 0) return;
    const hints = (response.normalized?.industry_hints || []).slice(0, 4);
    if (hints.length > 0) setSelectedIndustries(hints);
  }, [response, query, apolloPage, selectedIndustries.length]);

  const PAGE_SIZE = 8;

  // Slice each group to its current page.  Non-paginated groups return
  // all their items; paginated groups return exactly PAGE_SIZE.
  const visiblePerGroup = React.useMemo(() => {
    const map: Record<string, { items: SearchItem[]; page: number; pageCount: number }> = {};
    if (!response) return map;
    for (const g of response.groups) {
      const page = groupPage[g.kind] || 0;
      if (g.paginated) {
        const start = page * PAGE_SIZE;
        const end = start + PAGE_SIZE;
        map[g.kind] = {
          items: g.items.slice(start, end),
          page,
          pageCount: Math.max(1, Math.ceil(g.items.length / PAGE_SIZE)),
        };
      } else {
        map[g.kind] = { items: g.items, page: 0, pageCount: 1 };
      }
    }
    return map;
  }, [response, groupPage]);

  // Flat list of *currently visible* items for keyboard navigation.
  const flatItems: Array<{ group: string; item: SearchItem; idx: number }> = React.useMemo(() => {
    if (!response) return [];
    const out: Array<{ group: string; item: SearchItem; idx: number }> = [];
    let i = 0;
    for (const g of response.groups) {
      const slice = visiblePerGroup[g.kind]?.items || g.items;
      for (const it of slice) {
        out.push({ group: g.kind, item: it, idx: i });
        i += 1;
      }
    }
    return out;
  }, [response, visiblePerGroup]);

  // Scroll the active item into view on arrow keys
  React.useEffect(() => {
    const active = listRef.current?.querySelector<HTMLElement>(`[data-idx="${activeIdx}"]`);
    active?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }, [activeIdx]);

  const intakeCompany = React.useCallback(async (
    item: CompanyHit,
    opts: { openAccount?: boolean; closeModal?: boolean } = {},
  ): Promise<boolean> => {
    const openAccount = opts.openAccount ?? true;
    const closeModal = opts.closeModal ?? true;
    if (!item.domain && !item.name) return false;
    const key = (item.domain || item.name).toLowerCase();
    if (intakeInFlight.current.has(key)) return false;
    if (intakeDone.has(key)) {
      flash(`${item.name} is already in the pipeline`);
      return true;
    }
    intakeInFlight.current.add(key);
    setIntakeBusy(prev => new Set(prev).add(key));
    try {
      const qs = new URLSearchParams({
        domain: item.domain || "",
        company_name: item.name || "",
      });
      const res = await fetch(`/api/worktrigger/vendors/companies/intake?${qs}`, { method: "POST" });
      if (res.ok) {
        const data = await res.json() as { account_id?: string; deduped?: boolean };
        setIntakeDone(prev => new Set(prev).add(key));
        flash(data.deduped ? `${item.name} already in the pipeline` : `Added ${item.name} to the pipeline`);
        onIntakeComplete?.();
        if (openAccount && data.account_id) {
          onOpenAccount(data.account_id);
          if (closeModal) onClose();
        }
        return true;
      }
      flash(`Intake failed: ${await res.text()}`);
      return false;
    } catch (e) {
      flash(`Intake error: ${e instanceof Error ? e.message : "network"}`);
      return false;
    } finally {
      intakeInFlight.current.delete(key);
      setIntakeBusy(prev => { const n = new Set(prev); n.delete(key); return n; });
    }
  }, [flash, intakeDone, onClose, onIntakeComplete, onOpenAccount]);

  const batchAddSelectedCompanies = React.useCallback(async () => {
    if (!response || selectedCompanyKeys.size === 0) return;
    const selectedCompanies: CompanyHit[] = [];
    for (const g of response.groups) {
      if (g.kind !== "companies") continue;
      for (const item of g.items) {
        if (item.kind !== "company") continue;
        const key = (item.domain || item.name).toLowerCase();
        if (selectedCompanyKeys.has(key)) selectedCompanies.push(item);
      }
    }
    if (!selectedCompanies.length) return;
    let added = 0;
    for (const c of selectedCompanies) {
      const ok = await intakeCompany(c, { openAccount: false, closeModal: false });
      if (ok) added += 1;
    }
    flash(`Processed ${added}/${selectedCompanies.length} selected companies`);
    setSelectedCompanyKeys(new Set());
  }, [response, selectedCompanyKeys, intakeCompany, flash]);

  const doAction = React.useCallback(async (item: SearchItem, opts: { secondary?: boolean } = {}) => {
    saveRecent(query);
    setRecents(loadRecents());
    if (item.kind === "local_account") {
      onOpenAccount(item.id);
      onClose();
      return;
    }
    if (item.kind === "local_contact") {
      onOpenAccount(item.account_id);
      onClose();
      return;
    }
    if (item.kind === "company") {
      if (opts.secondary && item.domain) {
        window.open(`https://${item.domain}`, "_blank", "noopener");
        return;
      }
      await intakeCompany(item, { openAccount: true, closeModal: true });
      return;
    }
    if (item.kind === "person") {
      if (opts.secondary && item.linkedin_url) {
        window.open(item.linkedin_url, "_blank", "noopener");
        return;
      }
      if (!item.company_domain && !item.company_name) {
        flash("Need a company to attach this contact to");
        return;
      }
      const key = `p::${(item.company_domain || item.company_name).toLowerCase()}::${item.full_name.toLowerCase()}`;
      if (intakeInFlight.current.has(key)) return;
      if (intakeDone.has(key)) {
        flash(`${item.full_name} already added`);
        return;
      }
      intakeInFlight.current.add(key);
      setIntakeBusy(prev => new Set(prev).add(key));
      try {
        const qs = new URLSearchParams({
          domain: item.company_domain || "",
          company_name: item.company_name || "",
        });
        const res = await fetch(`/api/worktrigger/vendors/companies/intake?${qs}`, { method: "POST" });
        if (!res.ok) {
          flash(`Intake failed: ${await res.text()}`);
          return;
        }
        const data = await res.json() as { account_id?: string };
        if (!data.account_id) { flash("Intake returned no account"); return; }
        const contactQs = new URLSearchParams({
          full_name: item.full_name, title: item.title, email: "",
        });
        await fetch(`/api/worktrigger/accounts/${encodeURIComponent(data.account_id)}/contacts/add?${contactQs}`, { method: "POST" });
        setIntakeDone(prev => new Set(prev).add(key));
        flash(`Added ${item.company_name} + ${item.full_name}`);
        onIntakeComplete?.();
        onOpenAccount(data.account_id);
        onClose();
      } catch (e) {
        flash(`Error: ${e instanceof Error ? e.message : "network"}`);
      } finally {
        intakeInFlight.current.delete(key);
        setIntakeBusy(prev => { const n = new Set(prev); n.delete(key); return n; });
      }
    }
  }, [query, onOpenAccount, onClose, onIntakeComplete, flash, intakeDone, intakeCompany]);

  const onKeyDown = (e: React.KeyboardEvent<HTMLDivElement>) => {
    if (e.key === "Escape") { e.preventDefault(); onClose(); return; }
    if (!flatItems.length) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setActiveIdx(i => (i + 1) % flatItems.length);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIdx(i => (i - 1 + flatItems.length) % flatItems.length);
    } else if (e.key === "Enter") {
      e.preventDefault();
      const chosen = flatItems[activeIdx];
      if (chosen) void doAction(chosen.item, { secondary: e.metaKey || e.ctrlKey });
    }
  };

  if (!open) return null;

  const showingRecents = !query.trim() && recents.length > 0;

  return (
    <div className="us-backdrop" onClick={onClose}>
      <div
        className="us-modal"
        role="dialog"
        aria-modal="true"
        aria-label="Universal search"
        onClick={e => e.stopPropagation()}
        onKeyDown={onKeyDown}
        tabIndex={-1}
      >
        {/* Input row */}
        <div className="us-input-row">
          <svg className="us-icon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
            <circle cx="11" cy="11" r="8" />
            <path d="M21 21l-4.35-4.35" />
          </svg>
          <input
            ref={inputRef}
            className="us-input"
            placeholder="Search companies, industries, or people…"
            value={query}
            onChange={e => setQuery(e.target.value)}
            autoComplete="off"
            spellCheck={false}
          />
          {loading ? <span className="us-spinner" aria-hidden /> : null}
          <kbd className="us-esc">Esc</kbd>
        </div>

        {/* Intent chips */}
        <div className="us-chips">
          {(["all", "companies", "people", "industries"] as const).map(c => (
            <button
              key={c}
              className={`us-chip ${typesFilter === c ? "us-chip-active" : ""}`}
              onClick={() => setTypesFilter(c)}
            >
              {c === "all" ? "All" : c === "companies" ? "Companies" : c === "people" ? "People" : "Industries"}
            </button>
          ))}
          {response?.normalized?.corrected && response.normalized.corrected.toLowerCase() !== query.trim().toLowerCase() ? (
            <button className="us-suggest-fix" onClick={() => setQuery(response.normalized.corrected)}>
              Did you mean <strong>{response.normalized.corrected}</strong>?
            </button>
          ) : null}
          {response ? (
            <span className="us-meta" title={`Intent: ${response.normalized.intent} · LLM: ${response.normalized.llm_used ? "yes" : "no"} · ${response.took_ms}ms`}>
              {response.normalized.intent.replace(/_/g, " ")}
              {response.credits_spent.apollo + response.credits_spent.pdl + response.credits_spent.hunter > 0
                ? ` · ${response.credits_spent.apollo + response.credits_spent.pdl + response.credits_spent.hunter}¢`
                : ""}
            </span>
          ) : null}
        </div>
        {response?.normalized?.industry_hints?.length ? (
          <div className="us-chips" style={{ paddingTop: 0 }}>
            <span className="us-meta" style={{ marginRight: 6 }}>Industry filters:</span>
            {response.normalized.industry_hints.slice(0, 8).map((hint) => {
              const active = selectedIndustries.includes(hint);
              return (
                <button
                  key={hint}
                  className={`us-chip ${active ? "us-chip-active" : ""}`}
                  onClick={() => {
                    setSelectedIndustries((prev) => (
                      prev.includes(hint) ? prev.filter((x) => x !== hint) : [...prev, hint]
                    ));
                    setApolloPage(1);
                    setGroupPage({});
                    setActiveIdx(0);
                  }}
                >
                  {hint}
                </button>
              );
            })}
          </div>
        ) : null}

        {/* Results */}
        <div className="us-list" ref={listRef}>
          {showingRecents ? (
            <div className="us-group">
              <div className="us-group-label">Recent searches</div>
              <div className="us-recents">
                {recents.map(r => (
                  <button key={r} className="us-recent" onClick={() => setQuery(r)}>{r}</button>
                ))}
              </div>
            </div>
          ) : null}

          {!showingRecents && !query.trim() ? (
            <div className="us-empty">
              <div className="us-empty-title">Find anyone, anywhere</div>
              <div className="us-empty-sub">
                Try <code>Stripe</code>, <code>fintech CFOs</code>, <code>stripe.com</code>, or <code>Patrick Collison</code>.
              </div>
              <div className="us-empty-hints">
                <span><kbd>↑↓</kbd> navigate</span>
                <span><kbd>↵</kbd> select</span>
                <span><kbd>⌘↵</kbd> alt action</span>
                <span><kbd>Esc</kbd> close</span>
              </div>
            </div>
          ) : null}

          {response && response.groups.length === 0 && !loading && query.trim() ? (
            <div className="us-empty">
              <div className="us-empty-title">No matches</div>
              <div className="us-empty-sub">Try a different phrasing, or check the intent filter above.</div>
            </div>
          ) : null}

          {response?.groups.map(group => {
            const slice = visiblePerGroup[group.kind] || { items: group.items, page: 0, pageCount: 1 };
            // Compact layout kicks in after the "rich" slot budget is used
            // up on the first page, or for every row on subsequent pages.
            const richBudget = group.rich_count ?? Infinity;
            const isFirstPage = slice.page === 0;
            const companyItemsOnPage = slice.items.filter((it): it is CompanyHit => it.kind === "company");
            const selectedOnPage = companyItemsOnPage.filter((it) => selectedCompanyKeys.has((it.domain || it.name).toLowerCase())).length;
            const allOnPageSelected = companyItemsOnPage.length > 0 && selectedOnPage === companyItemsOnPage.length;
            return (
              <div key={group.kind} className="us-group">
                <div className="us-group-label">
                  {group.label}{" "}
                  <span className="us-group-count">
                    {group.paginated
                      ? `${slice.page * PAGE_SIZE + 1}–${Math.min((slice.page + 1) * PAGE_SIZE, group.items.length)} of ${group.items.length}`
                      : `(${group.items.length})`}
                  </span>
                </div>
                {group.kind === "companies" && companyItemsOnPage.length > 0 ? (
                  <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 8, fontSize: 11 }}>
                    <button
                      className="us-chip"
                      onClick={() => {
                        setSelectedCompanyKeys((prev) => {
                          const next = new Set(prev);
                          if (allOnPageSelected) {
                            for (const c of companyItemsOnPage) next.delete((c.domain || c.name).toLowerCase());
                          } else {
                            for (const c of companyItemsOnPage) next.add((c.domain || c.name).toLowerCase());
                          }
                          return next;
                        });
                      }}
                    >
                      {allOnPageSelected ? "Unselect page" : "Select page"}
                    </button>
                    <span className="us-meta">{selectedCompanyKeys.size} selected</span>
                    <button
                      className="us-chip us-chip-active"
                      disabled={selectedCompanyKeys.size === 0}
                      onClick={() => void batchAddSelectedCompanies()}
                    >
                      + Add selected
                    </button>
                  </div>
                ) : null}
                {slice.items.map((item, groupLocalIdx) => {
                  const globalIndexInGroup = slice.page * PAGE_SIZE + groupLocalIdx;
                  const compact = !isFirstPage || globalIndexInGroup >= richBudget;
                  const flatIdx = flatItems.findIndex(f => f.item === item);
                  const isActive = flatIdx === activeIdx;
                  if (item.kind === "company") {
                    const companyKey = (item.domain || item.name).toLowerCase();
                    const selected = selectedCompanyKeys.has(companyKey);
                    return (
                      <CompanySearchRow
                        key={`${group.kind}-${globalIndexInGroup}`}
                        item={item}
                        compact={compact}
                        flatIdx={flatIdx}
                        isActive={isActive}
                        busy={isBusy(item, intakeBusy)}
                        done={isDone(item, intakeDone)}
                        selected={selected}
                        onMouseEnter={() => setActiveIdx(flatIdx)}
                        onToggleSelect={() => {
                          setSelectedCompanyKeys((prev) => {
                            const next = new Set(prev);
                            if (next.has(companyKey)) next.delete(companyKey); else next.add(companyKey);
                            return next;
                          });
                        }}
                        onAction={() => void doAction(item)}
                      />
                    );
                  }
                  return (
                    <div
                      key={`${group.kind}-${globalIndexInGroup}`}
                      className={`us-item us-item-${item.kind} ${compact ? "us-item-compact" : ""} ${isActive ? "us-item-active" : ""} ${isBusy(item, intakeBusy) ? "us-item-busy" : ""}`}
                      data-idx={flatIdx}
                      onMouseEnter={() => setActiveIdx(flatIdx)}
                      onClick={(e) => {
                        if (isBusy(item, intakeBusy)) { e.preventDefault(); return; }
                        void doAction(item);
                      }}
                    >
                      {compact ? (
                        <>
                          <div className="us-compact-bullet" aria-hidden>{globalIndexInGroup + 1}</div>
                          <div className="us-item-body">
                            <div className="us-compact-line">
                              <span className="us-compact-name">
                                {item.kind === "person" ? item.full_name : item.kind === "local_contact" ? item.full_name : item.name}
                              </span>
                              <span className="us-compact-meta"><CompactMeta item={item} /></span>
                            </div>
                          </div>
                          <ActionLabel item={item} busy={isBusy(item, intakeBusy)} done={isDone(item, intakeDone)} />
                        </>
                      ) : (
                        <>
                          <ResultIcon item={item} />
                          <div className="us-item-body">
                            <div className="us-item-title">
                              {item.kind === "person" ? item.full_name : item.kind === "local_contact" ? item.full_name : item.name}
                              {item.kind === "local_account" && item.draft_count > 0 ? (
                                <span className="us-pill us-pill-info">{item.draft_count} draft{item.draft_count === 1 ? "" : "s"}</span>
                              ) : null}
                              {item.kind === "local_account" && item.signal_score >= 50 ? (
                                <span className="us-pill us-pill-hot">signal {Math.round(item.signal_score)}</span>
                              ) : null}
                            </div>
                            <div className="us-item-sub"><MetaLine item={item} /></div>
                          </div>
                          <ActionLabel item={item} busy={isBusy(item, intakeBusy)} done={isDone(item, intakeDone)} />
                        </>
                      )}
                    </div>
                  );
                })}
                {group.paginated && slice.pageCount > 1 ? (
                  <div className="us-pager">
                    <button
                      className="us-pager-btn"
                      disabled={slice.page <= 0}
                      onClick={(e) => {
                        e.stopPropagation();
                        setGroupPage(prev => ({ ...prev, [group.kind]: Math.max(0, (prev[group.kind] || 0) - 1) }));
                        setActiveIdx(0);
                      }}
                    >← Prev</button>
                    <span className="us-pager-meta">Page {slice.page + 1} of {slice.pageCount}</span>
                    <button
                      className="us-pager-btn"
                      disabled={slice.page >= slice.pageCount - 1}
                      onClick={(e) => {
                        e.stopPropagation();
                        setGroupPage(prev => ({ ...prev, [group.kind]: Math.min(slice.pageCount - 1, (prev[group.kind] || 0) + 1) }));
                        setActiveIdx(0);
                      }}
                    >Next →</button>
                  </div>
                ) : null}
                {group.kind === "companies" && group.paginated && slice.page === slice.pageCount - 1 && group.can_load_more ? (
                  <div className="us-load-more-row">
                    <div className="us-load-more-meta">
                      Showing <strong>{group.items.length.toLocaleString()}</strong> of{" "}
                      <strong>{(group.apollo_total || 0).toLocaleString()}</strong>{" "}
                      matching companies from Apollo.
                    </div>
                    <button
                      className="us-load-more-btn"
                      disabled={loadingMore}
                      onClick={(e) => {
                        e.stopPropagation();
                        if (!confirm("Fetch the next 100 companies? This will use 1 Apollo credit.")) return;
                        setLoadingMore(true);
                        setApolloPage(p => p + 1);
                      }}
                    >
                      {loadingMore ? "Loading…" : "Load next 100  ·  +1 credit"}
                    </button>
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>

        <div className="us-footer">
          <span>
            <kbd>↑↓</kbd> navigate · <kbd>↵</kbd> select · <kbd>⌘↵</kbd> alt · <kbd>Esc</kbd> close
          </span>
          <span>Figwork universal search</span>
        </div>
      </div>
    </div>
  );
}

type ContactBadgeInfo = {
  found: boolean;
  total: number;
  departments: Record<string, number>;
  pattern: string;
} | null;

function EmailIndicator({ domain, employeeFallback }: { domain: string; employeeFallback: number | null }) {
  const [count, setCount] = React.useState<ContactBadgeInfo>(null);
  const [loading, setLoading] = React.useState(false);
  React.useEffect(() => {
    if (!domain) return;
    let cancelled = false;
    setLoading(true);
    fetch(`/api/worktrigger/vendors/companies/contacts-count?domain=${encodeURIComponent(domain)}`)
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => { if (!cancelled) setCount(data as ContactBadgeInfo); })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [domain]);

  if (loading) return <span className="us-pill" style={{ marginLeft: 6 }}>✉ …</span>;
  if (count?.found && count.total > 0) {
    return <span className="us-pill us-pill-info" style={{ marginLeft: 6 }}>✉ {count.total}</span>;
  }
  if ((!count || !count.found) && Number(employeeFallback || 0) > 0) {
    return <span className="us-pill" style={{ marginLeft: 6 }}>~{Number(employeeFallback).toLocaleString()} emp</span>;
  }
  return null;
}

function CompanySearchRow({
  item,
  compact,
  flatIdx,
  isActive,
  busy,
  done,
  selected,
  onMouseEnter,
  onToggleSelect,
  onAction,
}: {
  item: CompanyHit;
  compact: boolean;
  flatIdx: number;
  isActive: boolean;
  busy: boolean;
  done: boolean;
  selected: boolean;
  onMouseEnter: () => void;
  onToggleSelect: () => void;
  onAction: () => void;
}) {
  const TOOLTIP_WIDTH = 280;
  const TOOLTIP_GAP = 10;
  const [hover, setHover] = React.useState(false);
  const [enriched, setEnriched] = React.useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [anchor, setAnchor] = React.useState<{ top: number; left: number } | null>(null);
  const rowRef = React.useRef<HTMLDivElement>(null);

  const computeAnchor = React.useCallback(() => {
    const el = rowRef.current;
    if (!el) return;
    const rect = el.getBoundingClientRect();
    const vw = window.innerWidth;
    const spaceRight = vw - rect.right;
    const placeRight = spaceRight >= TOOLTIP_WIDTH + TOOLTIP_GAP + 12;
    const left = placeRight
      ? Math.min(rect.right + TOOLTIP_GAP, vw - TOOLTIP_WIDTH - 8)
      : Math.max(8, rect.left - TOOLTIP_WIDTH - TOOLTIP_GAP);
    const top = Math.max(8, Math.min(rect.top, window.innerHeight - 120));
    setAnchor({ top, left });
  }, [TOOLTIP_GAP, TOOLTIP_WIDTH]);

  React.useEffect(() => {
    if (!hover || enriched || loading || !item.domain) return;
    let cancelled = false;
    setLoading(true);
    fetch(`/api/worktrigger/vendors/companies/enrich?domain=${encodeURIComponent(item.domain)}`, { method: "POST" })
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => { if (!cancelled && data?.found) setEnriched(data as Record<string, unknown>); })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [hover, enriched, loading, item.domain]);

  React.useEffect(() => {
    if (!hover) return;
    computeAnchor();
    const onMove = () => computeAnchor();
    window.addEventListener("scroll", onMove, true);
    window.addEventListener("resize", onMove);
    return () => {
      window.removeEventListener("scroll", onMove, true);
      window.removeEventListener("resize", onMove);
    };
  }, [hover, computeAnchor]);

  return (
    <div
      ref={rowRef}
      className={`us-item us-item-company ${compact ? "us-item-compact" : ""} ${isActive ? "us-item-active" : ""} ${busy ? "us-item-busy" : ""}`}
      data-idx={flatIdx}
      onMouseEnter={() => { onMouseEnter(); computeAnchor(); setHover(true); }}
      onMouseLeave={() => setHover(false)}
      onClick={(e) => { e.stopPropagation(); if (!busy) onAction(); }}
    >
      <input
        type="checkbox"
        checked={selected}
        onChange={onToggleSelect}
        onClick={(e) => e.stopPropagation()}
        title="Select company"
        style={{ marginRight: 8 }}
      />
      {compact ? (
        <>
          <div className="us-compact-bullet" aria-hidden>{flatIdx + 1}</div>
          <div className="us-item-body">
            <div className="us-compact-line">
              <span className="us-compact-name">
                {item.name}
                <EmailIndicator domain={item.domain} employeeFallback={item.employee_count} />
              </span>
              <span className="us-compact-meta"><CompactMeta item={item} /></span>
            </div>
          </div>
          <ActionLabel item={item} busy={busy} done={done} />
        </>
      ) : (
        <>
          <ResultIcon item={item} />
          <div className="us-item-body">
            <div className="us-item-title">
              {item.name}
              <EmailIndicator domain={item.domain} employeeFallback={item.employee_count} />
            </div>
            <div className="us-item-sub"><MetaLine item={item} /></div>
          </div>
          <ActionLabel item={item} busy={busy} done={done} />
        </>
      )}
      {hover && anchor ? createPortal(
        <div style={{
          position: "fixed",
          top: anchor.top,
          left: anchor.left,
          zIndex: 10050,
          width: TOOLTIP_WIDTH,
          background: "#fff",
          border: "1px solid #dbe3e8",
          borderRadius: 8,
          padding: 10,
          boxShadow: "0 10px 28px rgba(0,0,0,0.14)",
          pointerEvents: "none",
        }}>
          <div style={{ fontWeight: 700, fontSize: 12, marginBottom: 4 }}>{item.name}</div>
          {loading ? <div style={{ fontSize: 11, color: "#6b7280" }}>Loading enrichment…</div> : null}
          {enriched ? (
            <div style={{ display: "grid", gap: 3, fontSize: 11 }}>
              {enriched.industry ? <div>Industry: {String(enriched.industry)}</div> : null}
              {enriched.employee_count ? <div>Employees: {Number(enriched.employee_count).toLocaleString()}</div> : null}
              {enriched.funding_stage ? <div>Funding: {String(enriched.funding_stage)}</div> : null}
              {enriched.short_description ? <div style={{ color: "#4b5563" }}>{String(enriched.short_description).slice(0, 180)}</div> : null}
            </div>
          ) : item.short_description ? (
            <div style={{ fontSize: 11, color: "#4b5563" }}>{item.short_description}</div>
          ) : null}
          {item.domain ? <div style={{ marginTop: 4, fontSize: 10, color: "#2563eb" }}>{item.domain}</div> : null}
        </div>,
        document.body,
      ) : null}
    </div>
  );
}

function ResultIcon({ item }: { item: SearchItem }) {
  if (item.kind === "local_account") {
    const letter = (item.name?.[0] || item.domain?.[0] || "?").toUpperCase();
    return <div className="us-avatar us-avatar-local">{letter}</div>;
  }
  if (item.kind === "local_contact") {
    const initials = (item.full_name || "?").split(/\s+/).map(w => w[0] || "").join("").slice(0, 2).toUpperCase();
    return <div className="us-avatar us-avatar-person">{initials}</div>;
  }
  if (item.kind === "person") {
    const initials = (item.full_name || "?").split(/\s+/).map(w => w[0] || "").join("").slice(0, 2).toUpperCase();
    return <div className="us-avatar us-avatar-person">{initials}</div>;
  }
  // company
  if (item.logo_url) {
    return <img src={item.logo_url} alt="" className="us-logo" />;
  }
  const letter = (item.name?.[0] || "?").toUpperCase();
  return <div className="us-avatar us-avatar-company">{letter}</div>;
}

function MetaLine({ item }: { item: SearchItem }) {
  if (item.kind === "local_account") {
    const bits = [
      item.domain,
      item.industry,
      item.employee_count ? `${item.employee_count} emp` : "",
      item.funding_stage,
      item.country,
    ].filter(Boolean);
    return <>{bits.join(" · ")}</>;
  }
  if (item.kind === "local_contact") {
    return <>{[item.title, item.account_name, item.email].filter(Boolean).join(" · ")}</>;
  }
  if (item.kind === "company") {
    const bits = [
      item.domain,
      item.industry,
      item.employee_count ? `${item.employee_count} emp` : "",
      item.funding_stage,
    ].filter(Boolean);
    return <>{bits.join(" · ")}</>;
  }
  // person
  const bits = [
    item.title,
    item.company_name,
    item.company_domain,
  ].filter(Boolean);
  return <>{bits.join(" · ")}</>;
}

function intakeKeyFor(item: SearchItem): string | null {
  if (item.kind === "company") return (item.domain || item.name || "").toLowerCase();
  if (item.kind === "person") return `p::${(item.company_domain || item.company_name).toLowerCase()}::${item.full_name.toLowerCase()}`;
  return null;
}

function isBusy(item: SearchItem, intakeBusy: Set<string>): boolean {
  const k = intakeKeyFor(item);
  return k != null && intakeBusy.has(k);
}

function isDone(item: SearchItem, intakeDone: Set<string>): boolean {
  const k = intakeKeyFor(item);
  return k != null && intakeDone.has(k);
}

function CompactMeta({ item }: { item: SearchItem }) {
  if (item.kind === "company") {
    const bits = [item.domain, item.industry, item.employee_count ? `${item.employee_count} emp` : ""]
      .filter(Boolean);
    return <>{bits.join(" · ")}</>;
  }
  if (item.kind === "person") {
    return <>{[item.title, item.company_name].filter(Boolean).join(" · ")}</>;
  }
  if (item.kind === "local_account") {
    return <>{[item.domain, item.industry].filter(Boolean).join(" · ")}</>;
  }
  if (item.kind === "local_contact") {
    return <>{[item.title, item.account_name].filter(Boolean).join(" · ")}</>;
  }
  return null;
}

function ActionLabel({ item, busy, done }: { item: SearchItem; busy: boolean; done: boolean }) {
  if (busy) return <span className="us-action us-action-busy">…</span>;
  if (done) return <span className="us-action us-action-done">Added ✓</span>;
  if (item.kind === "local_account") return <span className="us-action">Open</span>;
  if (item.kind === "local_contact") return <span className="us-action">Open account</span>;
  if (item.kind === "company") return <span className="us-action us-action-primary">+ SDR</span>;
  if (item.kind === "person") return <span className="us-action us-action-primary">+ Contact</span>;
  return null;
}
