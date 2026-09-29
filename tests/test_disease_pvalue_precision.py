"""Keep computed p-values, and distinguish floating-point underflow from zero."""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api"))
from disease_utils import compute_genus_statistics


class DiseasePValuePrecisionTests(unittest.TestCase):
    @staticmethod
    def result(group_size):
        disease_keys = [f"d{i}" for i in range(group_size)]
        control_keys = [f"c{i}" for i in range(group_size)]
        counts = pd.DataFrame({
            "Bacteria.Bacteroidota.Class.Order.Family.Bacteroides": [90] * group_size + [10] * group_size,
            "Bacteria.Bacillota.Class.Order.Family.Blautia": [10] * group_size + [90] * group_size,
        }, index=disease_keys + control_keys)
        return compute_genus_statistics(counts, disease_keys, control_keys)

    def test_small_nonzero_p_and_fdr_are_not_rounded_to_zero(self):
        for row in self.result(40):
            self.assertGreater(row["p_value"], 0)
            self.assertLess(row["p_value"], 1e-8)
            self.assertGreater(row["adjusted_p"], 0)
            self.assertLess(row["adjusted_p"], 1e-8)
            self.assertFalse(row["p_value_underflow"])
            self.assertFalse(row["adjusted_p_underflow"])

    def test_true_numeric_underflow_is_flagged_without_inventing_a_pvalue(self):
        for row in self.result(2000):
            self.assertEqual(row["p_value"], 0.0)
            self.assertEqual(row["adjusted_p"], 0.0)
            self.assertIn("p_value_underflow", row)
            self.assertIn("adjusted_p_underflow", row)
            self.assertTrue(row["p_value_underflow"])
            self.assertTrue(row["adjusted_p_underflow"])


if __name__ == "__main__":
    unittest.main()
