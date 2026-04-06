import { useEffect, useMemo, useRef, useState } from "react";

import { useI18n } from "@/i18n";
import { cachedFetch } from "@/util/apiCache";

import type { SearchResponse } from "./types";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

interface InlineGenusSearchProps {
  genus: string;
  onSelect: (genus: string) => void;
}

export default function InlineGenusSearch({ genus, onSelect }: InlineGenusSearchProps) {
  const { t } = useI18n();
  const [query, setQuery] = useState(genus);
  const [results, setResults] = useState<string[]>([]);
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setQuery(genus);
  }, [genus]);

  useEffect(() => {
    const handleClick = (event: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  useEffect(() => {
    const normalized = query.trim();
    if (normalized.length < 2 || normalized.toLowerCase() === genus.toLowerCase()) {
      setResults([]);
      return;
    }
    const timer = window.setTimeout(() => {
      cachedFetch<SearchResponse>(`${API_BASE}/api/species-search?q=${encodeURIComponent(normalized)}`)
        .then((payload) => {
          setResults(payload.results ?? []);
          setOpen(true);
        })
        .catch(() => setResults([]));
    }, 220);
    return () => window.clearTimeout(timer);
  }, [genus, query]);

  const visibleResults = useMemo(
    () => results.filter((item) => item.toLowerCase() !== genus.toLowerCase()).slice(0, 8),
    [genus, results],
  );

  const submit = (candidate: string) => {
    const next = candidate.trim();
    if (!next) return;
    setOpen(false);
    setResults([]);
    setQuery(next);
    onSelect(next);
  };

  return (
    <div ref={rootRef}>
      <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
        <input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          onFocus={() => visibleResults.length > 0 && setOpen(true)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              submit(query);
            }
          }}
          placeholder={t("species.inlineSearchPlaceholder")}
          aria-label={t("species.inlineSearchPlaceholder")}
        />
        <button type="button" onClick={() => submit(query)}>
          {t("search.go")}
        </button>
      </div>
      {open && visibleResults.length > 0 ? (
        <ul style={{ listStyle: "none", margin: "10px 0 0", padding: 0 }}>
          {visibleResults.map((item) => (
            <li key={item}>
              <button type="button" onClick={() => submit(item)}>
                <i>{item}</i>
              </button>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
