/**
 * TransitionPanel.tsx — Age group transition summary
 */
import type { LifecycleTransition } from "../LifecyclePage";
import classes from "../LifecyclePage.module.css";

interface Props {
  transitions: LifecycleTransition[];
  locale: string;
  heading?: string;
}

const AGE_ZH: Record<string, string> = {
  Infant: "婴儿",
  Child: "儿童",
  Adolescent: "青少年",
  Adult: "成人",
  Older_Adult: "老年人",
  Oldest_Old: "高龄老人",
  Centenarian: "百岁老人",
  Unknown: "未知",
};

function sigMark(p: number | null | undefined): string {
  if (p == null || Number.isNaN(p)) return "";
  if (p < 0.001) return "***";
  if (p < 0.01) return "**";
  if (p < 0.05) return "*";
  return "ns";
}

function fmtP(p: number | null | undefined): string {
  if (p == null || Number.isNaN(p)) return "NA";
  if (p < 0.001) return p.toExponential(2);
  return p.toFixed(4);
}

export function TransitionPanel({ transitions, locale, heading }: Props) {
  const ageName = (value: string) => locale === "zh" ? (AGE_ZH[value] ?? value.replace(/_/g, " ")) : value.replace(/_/g, " ");

  return (
    <div className={classes.transitionCard}>
      <div className={classes.cardHeader}>
        <div>
          <h3>{locale === "zh" ? "年龄段过渡变化" : "Age Group Transitions"}</h3>
          {heading ? <p>{heading}</p> : null}
        </div>
      </div>

      {transitions.length === 0 ? (
        <div className={classes.emptyHint}>
          {locale === "zh" ? "当前筛选下没有达到阈值的年龄转折变化。" : "No transition exceeds the reporting threshold under the current filters."}
        </div>
      ) : (
        transitions.map((transition) => (
          <div key={`${transition.from}-${transition.to}`} className={classes.transitionSegment}>
            <div className={classes.transitionHeader}>
              <strong>{ageName(transition.from)}</strong>
              <span className={classes.arrow}>→</span>
              <strong>{ageName(transition.to)}</strong>
            </div>
            <ul className={classes.transitionList}>
              {transition.top_changes.map((change, index) => (
                <li key={`${transition.from}-${transition.to}-${change.genus}`} className={classes.transitionItem}>
                  <span className={classes.transitionRank}>{index + 1}.</span>
                  <i>{change.genus}</i>
                  <span className={change.direction === "increase" ? classes.increase : classes.decrease}>
                    {change.direction === "increase" ? "↑" : "↓"} {change.change.toFixed(2)}%
                  </span>
                  {change.adjusted_p != null ? (
                    <span
                      className={classes.transitionP}
                      title={`adj.p = ${fmtP(change.adjusted_p)}`}
                    >
                      {sigMark(change.adjusted_p)}
                    </span>
                  ) : null}
                </li>
              ))}
            </ul>
          </div>
        ))
      )}
    </div>
  );
}
