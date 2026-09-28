import { Link } from "react-router-dom";
import LightbulbIcon from "@/assets/lightbulb.svg?react";
import classes from "./Recipes.module.css";

const features = [
  "Filter samples by age group, sex, or disease",
  "Explore geographic distribution on an interactive map",
  "Compare microbiome composition across phenotype groups",
  "Search by SRR accession or BioProject ID",
  "View age × disease co-occurrence heatmap",
  "Analyze top-30 genus abundance by group",
];

const Recipes = () => (
  <section>
    <h2>What you can do</h2>

    <div className={classes.buttons}>
      {features.map((text, i) => (
        <div key={i} className={classes.item}>
          <LightbulbIcon className="inline-svg" />
          <span>{text}</span>
        </div>
      ))}
    </div>

    <p style={{ marginTop: "1.5rem" }}>
      <Link to="/phenotype" style={{ color: "var(--primary)" }}>
        → Open Phenotype Analysis
      </Link>{" "}
      to compare microbiome composition between any two groups.
    </p>
  </section>
);

export default Recipes;
