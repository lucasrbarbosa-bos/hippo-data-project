# main.py
import argparse, os, json
import pandas as pd
from pathlib import Path

from utils.utils import read_json_records, read_csv_concat
from src.validate import coerce_claim, coerce_revert
from src.transform import make_pharmacies, claims_df, reverts_df
from src.aggregate import metrics_by_npi_ndc, top2_chains_by_ndc, most_common_quantities_by_ndc


def ensure_dir(path: str | Path):
    Path(path).mkdir(parents=True, exist_ok=True)


def main():
    ap = argparse.ArgumentParser(description="Hippo DE pipeline (pandas, functions)")
    ap.add_argument("--pharmacies-dir", required=True, help="Directory of pharmacy CSV files")
    ap.add_argument("--claims-dir", required=True, help="Directory of claim JSON/JSONL files")
    ap.add_argument("--reverts-dir", required=True, help="Directory of revert JSON/JSONL files")
    ap.add_argument("--outdir", default="outputs", help="Directory to write outputs")
    ap.add_argument("--staging-dir", default="staging", help="Directory to write staged artifacts when --no-stage=false")
    # String boolean so you can pass true/false explicitly (PowerShell-friendly)
    ap.add_argument("--no-stage", default="true", choices=["true", "false"],
                    help="true = run end-to-end without staging (default); false = write staged artifacts first, then continue")
    args = ap.parse_args()

    no_stage = (args.no_stage.lower() == "true")

    ensure_dir(args.outdir)

    # 1) Read inputs
    df_pharm_raw = read_csv_concat(args.pharmacies_dir)
    pharmacies = make_pharmacies(df_pharm_raw)

    claims_raw = read_json_records(args.claims_dir)
    reverts_raw = read_json_records(args.reverts_dir)

    # 2) Validate/coerce
    claims_valid = [c for c in (coerce_claim(r) for r in claims_raw) if c is not None]
    reverts_valid = [r for r in (coerce_revert(x) for x in reverts_raw) if r is not None]

    # 3) Build DataFrames & enforce "events from Pharmacy dataset"
    claims = claims_df(claims_valid, pharmacies)
    kept_claim_ids = set(claims["claim_id"].tolist()) if not claims.empty else set()
    reverts = reverts_df(reverts_valid, kept_claim_ids, claims)

    # 3.5) Optional staging of the filtered artifacts (when --no-stage=false)
    if not no_stage:
        staging_dir = Path(args.staging_dir)
        ensure_dir(staging_dir)

        # Keep staged files simple & human-inspectable (CSV/JSON)
        # Pharmacies dimension (normalized)
        pharmacies.to_csv(staging_dir / "pharmacies.csv", index=False)

        # Claims/Reverts that survived validation + "from pharmacy dataset"
        claims.to_json(staging_dir / "claims.json", orient="records", indent=2)
        reverts.to_json(staging_dir / "reverts.json", orient="records", indent=2)

        # You still continue the pipeline below exactly the same way.

    # 4) Metrics (Goal 2)
    metrics = metrics_by_npi_ndc(claims, reverts)
    metrics_path = os.path.join(args.outdir, "metrics_by_npi_ndc.json")
    metrics.to_json(metrics_path, orient="records", indent=2)

    # 5) Downstream (Goals 3–4) on non-reverted claims only
    if not claims.empty:
        reverted_ids = set(reverts["claim_id"].tolist()) if not reverts.empty else set()
        non_rev_claims = claims[~claims["claim_id"].isin(reverted_ids)].copy()
    else:
        non_rev_claims = claims

    top2 = top2_chains_by_ndc(non_rev_claims)
    top2_path = os.path.join(args.outdir, "top2_chains_by_ndc.json")
    top2.to_json(top2_path, orient="records", indent=2)

    common_q = most_common_quantities_by_ndc(non_rev_claims, k=5)
    common_q_path = os.path.join(args.outdir, "most_common_quantities_by_ndc.json")
    common_q.to_json(common_q_path, orient="records", indent=2)

    print("Wrote:")
    print(f" - {metrics_path}")
    print(f" - {top2_path}")
    print(f" - {common_q_path}")
    if not no_stage:
        print(f"Staged filtered artifacts under: {Path(args.staging_dir).resolve()}")


if __name__ == "__main__":
    main()
