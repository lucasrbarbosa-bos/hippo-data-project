import pandas as pd

def metrics_by_npi_ndc(claims: pd.DataFrame, reverts: pd.DataFrame) -> pd.DataFrame:
    """
    For each (npi, ndc):
      - fills: count of all valid claims
      - reverted: count of those later reverted
      - avg_price: average unit_price over non-reverted claims
      - total_price: sum of price over non-reverted claims
    """
    if claims.empty:
        return pd.DataFrame(columns=["npi","ndc","fills","reverted","avg_price","total_price"])

    reverted_ids = set(reverts["claim_id"].tolist()) if not reverts.empty else set()
    claims = claims.copy()
    claims["is_reverted"] = claims["claim_id"].isin(reverted_ids)

    # fills & reverted
    grp = claims.groupby(["npi","ndc"], as_index=False).agg(
        fills=("claim_id","count"),
        reverted=("is_reverted", "sum")
    )

    # prices over non-reverted
    non_rev = claims[~claims["is_reverted"]].copy()
    if non_rev.empty:
        grp["avg_price"] = None
        grp["total_price"] = 0.0
        return grp[["npi","ndc","fills","reverted","avg_price","total_price"]]

    price_agg = non_rev.groupby(["npi","ndc"], as_index=False).agg(
        avg_price=("unit_price","mean"),
        total_price=("price","sum")
    )
    out = grp.merge(price_agg, on=["npi","ndc"], how="left").fillna({"total_price":0.0})
    # Sort for determinism
    out = out.sort_values(["npi","ndc"]).reset_index(drop=True)
    return out[["npi","ndc","fills","reverted","avg_price","total_price"]]

def top2_chains_by_ndc(non_reverted_claims: pd.DataFrame) -> pd.DataFrame:
    """
    For each ndc, pick the two chains with the lowest average unit price.
    Output format matches spec:
    [
      {
        "ndc": "...",
        "chain": [
          {"name": "health", "avg_price": 377.56},
          {"name": "saint", "avg_price": 413.40}
        ]
      },
      ...
    ]
    """
    if non_reverted_claims.empty:
        return pd.DataFrame(columns=["ndc", "chain"])

    avg_chain = (
        non_reverted_claims
        .groupby(["ndc", "chain"], as_index=False)["unit_price"]
        .mean()
        .rename(columns={"unit_price": "avg_price"})
    )
    # sort by ndc, then avg_price ascending
    avg_chain = avg_chain.sort_values(["ndc", "avg_price"], ascending=[True, True])

    # take top-2 per ndc and format as list of dicts
    def format_top2(group: pd.DataFrame) -> pd.Series:
        top = group.head(2)
        chain_list = [{"name": row["chain"], "avg_price": round(row["avg_price"], 2)} for _, row in top.iterrows()]
        return pd.Series({"chain": chain_list})

    out = avg_chain.groupby("ndc").apply(format_top2).reset_index()
    return out[["ndc", "chain"]]

def most_common_quantities_by_ndc(non_reverted_claims: pd.DataFrame, k: int = 5) -> pd.DataFrame:
    """
    For each ndc, return the top-k most frequently prescribed quantities
    (based on non-reverted claims). Tie-break by quantity ascending for stability.

    Output:
    [
      {"ndc":"00002323401", "most_prescribed_quantity":[8.5, 15.0, 45.0, 180.0, 2.0]},
      ...
    ]
    """
    if non_reverted_claims.empty:
        return pd.DataFrame(columns=["ndc", "most_prescribed_quantity"])

    # Count occurrences of each quantity per ndc
    counts = (
        non_reverted_claims
        .groupby(["ndc", "quantity"], as_index=False)
        .size()
        .rename(columns={"size": "cnt"})
    )

    # Sort: most frequent first; tie-break by smaller quantity first
    counts = counts.sort_values(["ndc", "cnt", "quantity"],
                                ascending=[True, False, True])

    def topk(group: pd.DataFrame) -> pd.Series:
        # Keep numeric fidelity (floats) as-is
        return pd.Series({
            "most_prescribed_quantity": group["quantity"].head(k).tolist()
        })

    out = counts.groupby("ndc").apply(topk).reset_index()
    return out[["ndc", "most_prescribed_quantity"]]