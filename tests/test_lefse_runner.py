"""Integration checks for the real microeco-backed LEfSe runner."""

import os
import sys
import unittest
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api"))
from compare_utils import run_lefse_analysis  # noqa: E402


class LefseRunnerTests(unittest.TestCase):
    def test_microeco_lefse_returns_kw_lda_and_group_direction(self):
        rng = np.random.default_rng(7)
        group_a = np.tile(np.array([20.0, 2.0, 3.0]), (8, 1))
        group_b = np.tile(np.array([2.0, 20.0, 3.0]), (8, 1))
        group_a += rng.normal(0, 0.1, group_a.shape)
        group_b += rng.normal(0, 0.1, group_b.shape)
        group_a[group_a < 0] = 0
        group_b[group_b < 0] = 0

        old = {key: os.environ.get(key) for key in ("LEFSE_BOOTS", "LEFSE_ALPHA", "LEFSE_P_ADJUST", "LEFSE_SEED")}
        try:
            os.environ["LEFSE_BOOTS"] = "2"
            os.environ["LEFSE_ALPHA"] = "0.05"
            os.environ["LEFSE_P_ADJUST"] = "none"
            os.environ["LEFSE_SEED"] = "1982"
            result = run_lefse_analysis(
                group_a,
                group_b,
                ["GenusA", "GenusB", "GenusC"],
                "genus",
            )
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertEqual(result["method"], "microeco::trans_diff(method='lefse')")
        self.assertEqual(result["hierarchical_wilcoxon"], "not_run_without_lefse_subgroup")
        self.assertGreaterEqual(result["n_output_rows"], 2)
        by_taxon = {row["taxon"]: row for row in result["results"]}
        self.assertEqual(by_taxon["GenusA"]["enriched_group"], "A")
        self.assertEqual(by_taxon["GenusB"]["enriched_group"], "B")
        self.assertGreater(by_taxon["GenusA"]["lda_score"], 0)
        self.assertGreater(by_taxon["GenusA"]["p_value"], 0)


if __name__ == "__main__":
    unittest.main()
