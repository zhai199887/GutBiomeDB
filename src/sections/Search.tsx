import { useEffect, useRef, useState, useMemo } from "react";
import { capitalize } from "lodash";
import { useDebounce } from "@reactuses/core";
import LoadingIcon from "@/assets/loading.svg?react";
import Placeholder from "@/components/Placeholder";
import Table, { type Col } from "@/components/Table";
import Textbox from "@/components/Textbox";
import Tabs from "@/components/Tabs";
import type { GeoSearch, SampleEntry, SampleSearch } from "@/data";
import { useData, loadSamples } from "@/data";
import { formatNumber } from "@/util/string";
import { thread } from "@/workers";
import classes from "./Search.module.css";

// ── Project columns vs Sample columns ────────────────────────────────────────

const PROJECT_COLS: (keyof SampleEntry)[] = [
  "name",
  "samples",
  "top_disease",
  "disease_types",
  "age_groups",
];

const SAMPLE_COLS: (keyof SampleEntry)[] = [
  "name",
  "age_group",
  "sex",
  "disease",
  "country",
];

const COL_LABELS: Partial<Record<keyof SampleEntry, string>> = {
  name: "Name",
  samples: "Samples",
  top_disease: "Main Disease",
  disease_types: "Disease Types",
  age_groups: "Age Groups",
  age_group: "Age Group",
  sex: "Sex",
  disease: "Disease",
  country: "Country",
};

// ── Main Search section ───────────────────────────────────────────────────────

const Search = () => {
  const geoSearch = useData((s) => s.geoSearch);
  const sampleSearch = useData((s) => s.sampleSearch);
  const [tab, setTab] = useState(0);

  useEffect(() => {
    if (tab === 1 && !sampleSearch) loadSamples();
  }, [tab, sampleSearch]);

  return (
    <section>
      <h2>Search</h2>
      <Tabs
        onChange={setTab}
        tabs={[
          {
            name: "Geography",
            content: (
              <>
                <p>Search for a country or region.</p>
                <GeoSearchList list={geoSearch} />
              </>
            ),
          },
          {
            name: "Sample / Project",
            content: (
              <>
                <p>
                  Search by SRR accession or BioProject ID.
                  <br />
                  <span style={{ color: "var(--light-gray)", fontSize: "0.85rem" }}>
                    Projects show aggregated disease/age summaries across all
                    their samples. Samples show individual annotations.
                  </span>
                </p>
                <SampleProjectList list={sampleSearch} />
              </>
            ),
          },
        ]}
      />
    </section>
  );
};

export default Search;

// ── Geography search list ─────────────────────────────────────────────────────

const GeoSearchList = ({ list }: { list?: GeoSearch }) => {
  const [_search, setSearch] = useState("");
  const search = useDebounce(_search, 300);
  const [type, setType] = useState("All");
  const [exactMatches, setExactMatches] = useState<GeoSearch>([]);
  const [searching, setSearching] = useState(false);
  const ctrl = useRef<AbortController>(null);

  const filtered = useMemo(() => {
    if (!list) return undefined;
    return type === "All"
      ? list
      : list.filter((e) => e.type === type);
  }, [list, type]);

  useEffect(() => {
    ctrl.current = new AbortController();
    if (filtered && search.trim()) {
      setExactMatches([]);
      setSearching(true);
      thread(
        (w) => w.exactSearch(filtered, ["name"], search),
        ctrl.current,
      )
        .then((r) => setExactMatches(r as GeoSearch))
        .catch(console.error)
        .finally(() => setSearching(false));
    }
    return () => {
      ctrl.current?.abort();
      setSearching(false);
    };
  }, [filtered, search]);

  if (!filtered)
    return <Placeholder height={200}>Loading geography data...</Placeholder>;

  const matches = search.trim() ? exactMatches : filtered;

  return (
    <>
      <SearchBar
        search={_search}
        onSearch={setSearch}
        searching={searching}
        count={matches.length}
        typeOptions={["All", "Country", "Region"]}
        type={type}
        onType={setType}
      />
      <Table
        cols={[
          { key: "name", name: "Name" },
          { key: "type", name: "Type" },
          { key: "samples", name: "Samples" },
        ]}
        rows={matches}
        extraRows={
          !searching && !matches.length ? ["", "No results", ""] : undefined
        }
      />
    </>
  );
};

// ── Sample/Project search list ────────────────────────────────────────────────

const SampleProjectList = ({ list }: { list?: SampleSearch }) => {
  const [_search, setSearch] = useState("");
  const search = useDebounce(_search, 300);
  const [type, setType] = useState<"All" | "Project" | "Sample">("All");
  const [exactMatches, setExactMatches] = useState<SampleSearch>([]);
  const [fuzzyMatches, setFuzzyMatches] = useState<SampleSearch>([]);
  const [exactSearching, setExactSearching] = useState(false);
  const [fuzzySearching, setFuzzySearching] = useState(false);
  const exactCtrl = useRef<AbortController>(null);
  const fuzzyCtrl = useRef<AbortController>(null);

  const filtered = useMemo(() => {
    if (!list) return undefined;
    return type === "All" ? list : list.filter((e) => e.type === type);
  }, [list, type]);

  useEffect(() => {
    exactCtrl.current = new AbortController();
    if (filtered && search.trim()) {
      setExactMatches([]);
      setExactSearching(true);
      thread(
        (w) => w.exactSearch(filtered, ["name"], search),
        exactCtrl.current,
      )
        .then((r) => setExactMatches(r as SampleSearch))
        .catch(console.error)
        .finally(() => setExactSearching(false));
    }
    return () => {
      exactCtrl.current?.abort();
      setExactSearching(false);
    };
  }, [filtered, search]);

  useEffect(() => {
    fuzzyCtrl.current = new AbortController();
    if (filtered && search.trim()) {
      setFuzzyMatches([]);
      setFuzzySearching(true);
      thread(
        (w) => w.fuzzySearch(filtered, ["name"], search),
        fuzzyCtrl.current,
      )
        .then((r) => setFuzzyMatches(r as SampleSearch))
        .catch(console.error)
        .finally(() => setFuzzySearching(false));
    }
    return () => {
      fuzzyCtrl.current?.abort();
      setFuzzySearching(false);
    };
  }, [filtered, search]);

  const exactLookup = useMemo(
    () => Object.fromEntries(exactMatches.map((m) => [m.name, ""])),
    [exactMatches],
  );

  if (!filtered)
    return <Placeholder height={300}>Loading sample data...</Placeholder>;

  const matches: SampleSearch = search.trim()
    ? exactMatches.concat(
        fuzzyMatches
          .filter((f) => !(f.name in exactLookup))
          .map((f) => ({ ...f, fuzzy: true })),
      )
    : filtered;

  /** choose columns based on active type filter */
  const showingProjects = type === "Project";
  const showingSamples = type === "Sample";
  const cols: (keyof SampleEntry)[] =
    showingProjects
      ? PROJECT_COLS
      : showingSamples
        ? SAMPLE_COLS
        : ["name", "type", "samples", "top_disease", "age_group", "sex"];

  return (
    <>
      <SearchBar
        search={_search}
        onSearch={setSearch}
        searching={exactSearching || fuzzySearching}
        count={matches.length}
        typeOptions={["All", "Project", "Sample"]}
        type={type}
        onType={(t) => setType(t as typeof type)}
      />
      <Table
        cols={cols.map(
          (col): Col<SampleEntry, keyof SampleEntry> => ({
            key: col,
            name: COL_LABELS[col] ?? capitalize(String(col).replace(/_/g, " ")),
            style: (_, row) => ({ opacity: row?.fuzzy ? 0.5 : 1 }),
          }),
        )}
        rows={matches}
        extraRows={
          !exactSearching && !fuzzySearching && !matches.length
            ? ["", "No results", ""]
            : undefined
        }
      />
    </>
  );
};

// ── Shared search bar ─────────────────────────────────────────────────────────

type SearchBarProps = {
  search: string;
  onSearch: (v: string) => void;
  searching: boolean;
  count: number;
  typeOptions: string[];
  type: string;
  onType: (t: string) => void;
};

const SearchBar = ({
  search,
  onSearch,
  searching,
  count,
  typeOptions,
  type,
  onType,
}: SearchBarProps) => (
  <div className={classes.search}>
    <div className={classes.box}>
      <LoadingIcon style={{ opacity: searching ? 1 : 0 }} />
      <Textbox value={search} onChange={onSearch} placeholder="Search" />
    </div>

    <div style={{ display: "flex", gap: "0.4rem", alignItems: "center" }}>
      <span style={{ fontSize: "0.85rem", color: "var(--light-gray)" }}>
        Type:
      </span>
      {typeOptions.map((t) => (
        <button
          key={t}
          onClick={() => onType(t)}
          style={{
            background: type === t ? "var(--primary)" : "none",
            border: "1px solid var(--gray)",
            color: type === t ? "var(--black)" : "var(--light-gray)",
            borderRadius: "4px",
            padding: "0.2rem 0.6rem",
            cursor: "pointer",
            fontSize: "0.8rem",
          }}
        >
          {t}
        </button>
      ))}
    </div>

    <div style={{ width: 110 }}>{formatNumber(count)} items</div>
  </div>
);
