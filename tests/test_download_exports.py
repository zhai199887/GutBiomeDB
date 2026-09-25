"""Regression tests for download serialization, without loading the full database."""
import ast
import asyncio
import copy
import csv
import io
import json
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from fastapi import HTTPException
from fastapi.responses import JSONResponse, StreamingResponse


def load_download_functions(profile):
    source = Path(__file__).resolve().parents[1] / "api" / "main.py"
    names = {
        "_validate_download_format", "_slugify_download_part", "_download_filename",
        "_download_headers", "_normalize_download_value", "_download_response",
        "download_species_profile_data", "download_disease_profile_data",
    }
    functions = []
    for node in ast.parse(source.read_text(encoding="utf-8-sig")).body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            node.decorator_list = []
            functions.append(node)
    namespace = {
        "Request": object, "HTTPException": HTTPException,
        "JSONResponse": JSONResponse, "StreamingResponse": StreamingResponse,
        "io": io, "csv_mod": csv, "json": json, "datetime": datetime,
        "app": SimpleNamespace(version="test"),
        "DOWNLOAD_FORMATS": {"csv": ",", "tsv": "\t", "json": None},
        "DOWNLOAD_CITATION_NOTE": "Test citation",
        # The full profile is expensive; the serializer and endpoint remain real.
        "species_profile": lambda request, genus: copy.deepcopy(profile),
        "disease_profile": lambda request, disease, top_n: copy.deepcopy(profile),
        "get_genus_list": lambda: ["Bacteroides"],
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), "exec"), namespace)
    return namespace


async def response_text(response):
    if hasattr(response, "body"):
        return response.body.decode("utf-8")
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk)
    return "".join(chunks)


class GenusDownloadTests(unittest.TestCase):
    def setUp(self):
        self.profile = {
            "genus": "Bacteroides", "phylum": "Bacteroidota",
            "total_samples": 10, "present_samples": 8, "prevalence": 0.8,
            "mean_abundance": 12.5, "median_abundance": 9.0,
            "nc_mean": 4.0, "nc_prevalence": 0.5,
            "by_disease": [{
                "name": "adenoma", "mean_abundance": 12.3456789,
                "median_abundance": 8.0, "std_abundance": 2.0, "p25": 4.0,
                "p75": 14.0, "prevalence": 0.75, "sample_count": 4,
                "log2fc": 1.625, "p_value": 0.01, "adjusted_p": 0.02,
                "effect_size": -0.4, "significant": True,
            }],
            "by_country": [], "by_age_group": [], "by_sex": [],
        }
        self.endpoint = load_download_functions(self.profile)["download_species_profile_data"]

    def test_csv_contains_actual_mean_abundance_and_profile_phylum(self):
        response = self.endpoint(None, "Bacteroides", "csv")
        rows = list(csv.DictReader(io.StringIO(asyncio.run(response_text(response)))))
        self.assertEqual(len(rows), 1)
        self.assertNotEqual(rows[0]["abundance"], "", "The CSV must not discard mean_abundance")
        self.assertEqual(float(rows[0]["abundance"]), 12.3456789)
        self.assertEqual(rows[0]["phylum"], "Bacteroidota")
        self.assertEqual(float(rows[0]["prevalence"]), 0.75)
        self.assertEqual(int(rows[0]["sample_count"]), 4)

    def test_tsv_preserves_zero_abundance_instead_of_blank(self):
        self.profile["by_disease"][0]["mean_abundance"] = 0.0
        response = self.endpoint(None, "Bacteroides", "tsv")
        rows = list(csv.DictReader(io.StringIO(asyncio.run(response_text(response))), delimiter="\t"))
        self.assertEqual(rows[0]["abundance"], "0.0")
        self.assertEqual(rows[0]["phylum"], "Bacteroidota")

    def test_invalid_numeric_taxonomy_label_is_not_exported_as_a_genus(self):
        with self.assertRaises(HTTPException) as raised:
            self.endpoint(None, "00", "csv")
        self.assertEqual(raised.exception.status_code, 400)

    def test_genus_validation_preserves_case_insensitive_access(self):
        response = self.endpoint(None, " bacteroides ", "json")
        self.assertEqual(json.loads(asyncio.run(response_text(response))), self.profile)

    def test_empty_disease_rows_keep_csv_header(self):
        self.profile["by_disease"] = []
        response = self.endpoint(None, "Bacteroides", "csv")
        reader = csv.DictReader(io.StringIO(asyncio.run(response_text(response))))
        self.assertEqual(reader.fieldnames, ["name", "abundance", "prevalence", "sample_count", "phylum"])
        self.assertEqual(list(reader), [])


class DiseaseDownloadTests(unittest.TestCase):
    def test_csv_retains_small_pvalues_and_explicit_underflow_flags(self):
        profile = {"top_genera": [
            {"genus": "Bacteroides", "p_value": 8.14e-86, "adjusted_p": 4.65e-84,
             "p_value_underflow": False, "adjusted_p_underflow": False},
            {"genus": "Alistipes", "p_value": 0.0, "adjusted_p": 0.0,
             "p_value_underflow": True, "adjusted_p_underflow": True},
        ]}
        endpoint = load_download_functions(profile)["download_disease_profile_data"]
        response = endpoint(None, "adenoma", "csv")
        rows = list(csv.DictReader(io.StringIO(asyncio.run(response_text(response)))))
        self.assertEqual(float(rows[0]["p_value"]), 8.14e-86)
        self.assertEqual(rows[0].get("p_value_underflow"), "false")
        self.assertEqual(rows[1].get("p_value_underflow"), "true")
        self.assertEqual(rows[1].get("adjusted_p_underflow"), "true")


if __name__ == "__main__":
    unittest.main()
