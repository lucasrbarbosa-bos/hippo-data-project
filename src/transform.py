import pandas as pd

def make_pharmacies(df_pharm: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize pharmacies columns and keep only npi + chain.
    Assumes 'id' is the pharmacy's NPI identifier.
    """
    if df_pharm.empty:
        return pd.DataFrame(columns=["npi", "chain"])
    cols = {c.lower(): c for c in df_pharm.columns}
    id_col = cols.get("id", "id")
    chain_col = cols.get("chain", "chain")
    df = df_pharm.rename(columns={id_col: "npi", chain_col: "chain"})
    df["npi"] = df["npi"].astype(str)
    return df[["npi", "chain"]].drop_duplicates().reset_index(drop=True)

def claims_df(valid_claims: list[dict], pharmacies: pd.DataFrame) -> pd.DataFrame:
    """
    Build a claims DataFrame, joining known pharmacies to enforce
    'only events from Pharmacy dataset'.
    """
    if not valid_claims:
        return pd.DataFrame(columns=["claim_id","ndc","npi","quantity","price","unit_price","timestamp","chain"])
    df = pd.DataFrame(valid_claims)
    df["npi"] = df["npi"].astype(str)
    # inner join ensures only claims whose NPI exists in pharmacy dataset
    df = df.merge(pharmacies, on="npi", how="inner")
    return df

def reverts_df(valid_reverts: list[dict], kept_claim_ids: set[str], claims_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a reverts DataFrame, keeping only reverts that reference a kept claim.
    Also attach NPI (and chain) via claim_id for alignment.
    """
    if not valid_reverts or claims_df.empty:
        return pd.DataFrame(columns=["id","claim_id","timestamp","npi","chain"])
    df = pd.DataFrame(valid_reverts)
    # only reverts pointing to claims we kept
    df = df[df["claim_id"].isin(kept_claim_ids)].copy()
    if df.empty:
        return df.assign(npi=pd.Series(dtype=str), chain=pd.Series(dtype=str))
    # attach npi + chain from claims
    claim_keys = claims_df[["claim_id","npi","chain"]].drop_duplicates()
    df = df.merge(claim_keys, on="claim_id", how="inner")
    return df
