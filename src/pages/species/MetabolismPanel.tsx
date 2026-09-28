import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import { useI18n } from "@/i18n";

import { buildKeggLink } from "./utils";

interface MappingCategory {
  id: string;
  name_en: string;
  name_zh: string;
  icon?: string;
  taxa?: string[];
  genus_exact_names?: string[];
  key_metabolites?: string[];
  key_metabolites_zh?: string[];
  related_pathways?: string[];
  related_pathways_zh?: string[];
  kegg_pathway_ids?: string[];
  metacyc_pathway_ids?: string[];
}

interface MappingPayload {
  categories: MappingCategory[];
}

interface MetabolismPanelProps {
  genus: string;
}

export default function MetabolismPanel({ genus }: MetabolismPanelProps) {
  const { locale, t } = useI18n();
  const [matches, setMatches] = useState<MappingCategory[]>([]);

  useEffect(() => {
    fetch("/data/metabolism_mapping.json")
      .then((response) => response.json())
      .then((payload: MappingPayload) => {
        const normalized = genus.toLowerCase();
        const filtered = (payload.categories ?? []).filter((category) => {
          const exact = category.genus_exact_names ?? [];
          if (exact.some((item) => item.toLowerCase() === normalized)) return true;
          return (category.taxa ?? []).some((item) => {
            const lower = item.toLowerCase();
            return lower.includes(normalized) || normalized.includes(lower);
          });
        });
        setMatches(filtered);
      })
      .catch(() => setMatches([]));
  }, [genus]);

  if (matches.length === 0) return null;

  return (
    <section className="species-block">
      <div className="species-blockHeader">
        <h2>{t("species.metabolism.title")}</h2>
        <p>{t("species.metabolism.subtitle")}</p>
      </div>
      <div className="species-grid species-gridProfile">
        {matches.map((category) => (
          <article key={category.id} className="species-chartCard">
            <div className="species-chartHeader">
              <div>
                <span>{category.icon ?? "•"}</span>
                <div>
                  <h3>{locale === "zh" ? category.name_zh : category.name_en}</h3>
                  <Link to="/metabolism">{t("species.metabolism.openWorkspace")}</Link>
                </div>
              </div>
              <div className="species-summaryPills">
                {(
                  (locale === "zh" && category.key_metabolites_zh?.length
                    ? category.key_metabolites_zh
                    : category.key_metabolites) ?? []
                )
                  .slice(0, 4)
                  .map((item, index) => (
                    <span key={`${item}-${index}`}>{item}</span>
                  ))}
              </div>
            </div>

            {(category.related_pathways ?? []).length > 0 ? (
              <div>
                <div>{t("species.metabolism.pathways")}</div>
                <div className="species-summaryPills">
                  {(locale === "zh" && category.related_pathways_zh?.length
                    ? category.related_pathways_zh
                    : category.related_pathways ?? []
                  ).map((item, index) => (
                    <span key={`${item}-${index}`}>{item}</span>
                  ))}
                </div>
              </div>
            ) : null}

            {(category.kegg_pathway_ids ?? []).length > 0 ? (
              <div>
                <div>KEGG</div>
                <div className="species-summaryPills">
                  {category.kegg_pathway_ids?.map((pathwayId) => (
                    <a
                      key={pathwayId}
                      href={buildKeggLink(pathwayId)}
                      target="_blank"
                      rel="noreferrer"
                    >
                      {pathwayId}
                    </a>
                  ))}
                </div>
              </div>
            ) : null}
          </article>
        ))}
      </div>
    </section>
  );
}
