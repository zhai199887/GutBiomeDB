import classes from "../DiseasePage.module.css";
import type { DemoEntry } from "./types";

interface Props {
  data: DemoEntry[];
  locale: string;
  orientation?: "horizontal" | "vertical";
  formatter?: (name: string) => string;
}

const DemoBarChart = ({
  data,
  locale,
  orientation = "horizontal",
  formatter,
}: Props) => {
  if (data.length === 0) {
    return <div className={classes.emptyPlot}>{locale === "zh" ? "暂无数据" : "No data"}</div>;
  }

  const formatted = data.map((entry) => ({
    ...entry,
    label: formatter ? formatter(entry.name) : entry.name,
  }));
  const maxCount = Math.max(...formatted.map((entry) => entry.count), 1);

  if (orientation === "vertical") {
    return (
      <div className={classes.verticalBars}>
        {formatted.map((entry) => (
          <div key={entry.name} className={classes.verticalBarItem}>
            <div className={classes.verticalBarValue}>{entry.count.toLocaleString("en")}</div>
            <div
              className={classes.verticalBarFill}
              style={{ height: `${Math.max((entry.count / maxCount) * 100, 6)}%` }}
              title={`${entry.label}: ${entry.count.toLocaleString("en")}`}
            />
            <div className={classes.verticalBarLabel}>{entry.label}</div>
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className={classes.demoBars}>
      {formatted.map((entry) => (
        <div key={entry.name} className={classes.demoBarRow}>
          <div className={classes.demoBarLabel}>{entry.label}</div>
          <div className={classes.demoBarTrack}>
            <div
              className={classes.demoBarFill}
              style={{ width: `${Math.max((entry.count / maxCount) * 100, 4)}%` }}
            />
          </div>
          <div className={classes.demoBarValue}>{entry.count.toLocaleString("en")}</div>
        </div>
      ))}
    </div>
  );
};

export default DemoBarChart;
