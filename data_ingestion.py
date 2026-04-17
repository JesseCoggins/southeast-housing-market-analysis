"""Reusable data pipeline for Southeast U.S. metro housing market data.

This module handles everything up to a clean, analysis-ready base table:
  - API pulls from Census ACS, FHFA HPI, and BLS LAUS
  - Source-specific type conversion and sentinel replacement
  - Metro geography standardization (CBSA keys, state FIPS, clean names)
  - Caching so repeated runs don't re-hit the APIs
  - A merged base snapshot with one row per metro

What this module does NOT do:
  - Percentile scoring or feature ranking
  - Pillar weights or composite scores
  - Research-question-specific derived features
  - Visualization or interpretation

Those belong in the analysis notebook so the ingestion layer can be reused
across different analyses, dashboards, or scoring frameworks without modification.

Typical usage:
    from data_ingestion import load_all_source_tables, build_latest_market_snapshot

    tables = load_all_source_tables()
    # (optional: apply analysis-specific filters to the raw source tables here)
    market_snapshot_df = build_latest_market_snapshot(
        tables["census_profile_df"],
        tables["fhfa_df"],
        tables["bls_df"],
        tables["growth_metrics_df"],
        tables["employment_growth_df"],
    )
"""

from __future__ import annotations

import io
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests


REQUEST_CONNECT_TIMEOUT = 15
REQUEST_READ_TIMEOUT = 180
REQUEST_RETRY_COUNT = 3
REQUEST_RETRY_BACKOFF_SECONDS = 2
CENSUS_CHUNK_PAUSE_SECONDS = 1
CENSUS_METRO_GEO_CLAUSE = "metropolitan statistical area/micropolitan statistical area:*"
CENSUS_METRO_GEO_KEY = "metropolitan statistical area/micropolitan statistical area"
CACHE_DIR = Path(__file__).resolve().parent / "cache"

# I use a stricter Southeast state list so the metro universe stays aligned with the
# project framing rather than including metros that only partially overlap the region.
SOUTHEAST_STATES = {
    "AL": "01",
    "AR": "05",
    "FL": "12",
    "GA": "13",
    "KY": "21",
    "LA": "22",
    "MS": "28",
    "NC": "37",
    "SC": "45",
    "TN": "47",
    "VA": "51",
}


def utc_now_iso() -> str:
    """Return a consistent UTC timestamp for when I pulled the data."""
    return datetime.now(timezone.utc).isoformat()


def fetch_json(url: str, *, method: str = "GET", **kwargs) -> object:
    """Use one shared request helper for JSON APIs with retry/backoff on slow responses."""
    headers = kwargs.pop("headers", {}) or {}
    headers.setdefault("User-Agent", "Mozilla/5.0 (compatible; housing-data-pipeline/1.0)")

    last_error: Exception | None = None
    for attempt in range(1, REQUEST_RETRY_COUNT + 1):
        try:
            response = requests.request(
                method,
                url,
                timeout=(REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT),
                headers=headers,
                **kwargs,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as exc:
            last_error = exc
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code not in {429, 500, 502, 503, 504} or attempt == REQUEST_RETRY_COUNT:
                raise
            time.sleep(REQUEST_RETRY_BACKOFF_SECONDS * attempt)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_error = exc
            if attempt == REQUEST_RETRY_COUNT:
                break
            time.sleep(REQUEST_RETRY_BACKOFF_SECONDS * attempt)

    raise requests.exceptions.RequestException(
        f"Request to {url} failed after {REQUEST_RETRY_COUNT} attempts."
    ) from last_error


def metro_state_abbrevs(metro_name: str) -> list[str]:
    """Extract state abbreviations from a Census metro name."""
    suffix = metro_name.rsplit(",", 1)[-1].strip()
    suffix = re.sub(
        r"\s+(Metro Area|Micro Area|Metropolitan Statistical Area|Micropolitan Statistical Area)$",
        "",
        suffix,
    )
    return [part.strip() for part in suffix.split("-") if part.strip()]


def is_southeast_metro(metro_name: str) -> bool:
    """Keep metros only when every listed state stays inside the Southeast region."""
    states = metro_state_abbrevs(metro_name)
    return bool(states) and all(state in SOUTHEAST_STATES for state in states)


def primary_state_fips(metro_name: str) -> str | None:
    """Use the first listed state as the metro's primary state for BLS IDs."""
    states = metro_state_abbrevs(metro_name)
    if not states:
        return None
    return SOUTHEAST_STATES.get(states[0])


def clean_metro_name(metro_name: str) -> str:
    """Remove geography suffixes to make metro names easier to read."""
    cleaned = re.sub(
        r"\s+(Metro Area|Micro Area|Metropolitan Statistical Area|Micropolitan Statistical Area)$",
        "",
        metro_name,
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def chunked(values: list[str], chunk_size: int) -> list[list[str]]:
    """Split a list into fixed-size chunks."""
    return [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]


def fetch_census_acs_dataframe(
    *,
    year: int,
    variables: list[str],
    chunk_size: int = 5,
) -> pd.DataFrame:
    """Fetch ACS metro data in smaller chunks and merge the results on NAME + CBSA.

    The Census API is reliable with short variable lists but tends to throw 503s on
    very wide requests. Chunking keeps each request smaller while still building the
    same final table.
    """
    census_api_key = os.getenv("CENSUS_API_KEY")
    url = f"https://api.census.gov/data/{year}/acs/acs1"
    merged_df: pd.DataFrame | None = None

    for variable_chunk in chunked(variables, chunk_size):
        params = {
            "get": ",".join(["NAME"] + variable_chunk),
            "for": CENSUS_METRO_GEO_CLAUSE,
        }
        if census_api_key:
            params["key"] = census_api_key

        rows = fetch_json(url, params=params)
        columns = rows[0]
        data_rows = rows[1:]
        chunk_df = pd.DataFrame(data_rows, columns=columns)

        if merged_df is None:
            merged_df = chunk_df
        else:
            merged_df = merged_df.merge(
                chunk_df,
                on=["NAME", CENSUS_METRO_GEO_KEY],
                how="inner",
            )
        time.sleep(CENSUS_CHUNK_PAUSE_SECONDS)

    if merged_df is None:
        raise ValueError(f"Census ACS {year} request returned no data.")

    return merged_df


def fetch_census_metros() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch metro-level ACS 2023 data and return profile + metro dimension tables.

    Pulls a wide set of housing, demographic, and economic variables so the profile
    table serves as a reusable base for any housing market analysis without needing
    to re-fetch from the API for each new project.
    """
    CACHE_DIR.mkdir(exist_ok=True)
    profile_cache = CACHE_DIR / "census_acs_profile_2023.csv"
    metro_dim_cache = CACHE_DIR / "census_metro_dim_2023.csv"

    if profile_cache.exists() and metro_dim_cache.exists():
        profile_df = pd.read_csv(
            profile_cache,
            dtype={"cbsa_code": str, "primary_state_fips": str, "state_abbrevs": str},
        )
        metro_dim_df = pd.read_csv(
            metro_dim_cache,
            dtype={"cbsa_code": str, "primary_state_fips": str, "state_abbrevs": str},
        )
        return profile_df, metro_dim_df

    census_variables = [
        # Value and rent
        "B25077_001E",  # Median home value
        "B25064_001E",  # Median gross rent
        # Income
        "B19013_001E",  # Median household income
        # Population and demographics
        "B01003_001E",  # Total population
        "B01002_001E",  # Median age
        # Housing stock
        "B25001_001E",  # Total housing units
        "B25002_003E",  # Vacant housing units
        "B25035_001E",  # Median year structure built
        # Tenure (owner vs. renter)
        "B25003_002E",  # Owner-occupied units
        "B25003_003E",  # Renter-occupied units
        # Educational attainment (population 25+)
        "B15003_001E",  # Population 25+ (denominator)
        "B15003_022E",  # Bachelor's degree
        "B15003_023E",  # Master's degree
        "B15003_024E",  # Professional school degree
        "B15003_025E",  # Doctorate degree
        # Gross rent as % of income — cost burden
        "B25070_001E",  # Total renters with rent data (GRAPI denominator)
        "B25070_007E",  # Rent 30–34.9% of income
        "B25070_008E",  # Rent 35–39.9% of income
        "B25070_009E",  # Rent 40–49.9% of income
        "B25070_010E",  # Rent 50%+ of income (severely cost burdened)
    ]
    df = fetch_census_acs_dataframe(year=2023, variables=census_variables, chunk_size=5)

    df = df.rename(
        columns={
            "NAME": "metro_name_full",
            "B25077_001E": "median_home_value",
            "B25064_001E": "median_gross_rent",
            "B19013_001E": "median_household_income",
            "B01003_001E": "population",
            "B01002_001E": "median_age",
            "B25001_001E": "housing_units",
            "B25002_003E": "vacant_housing_units",
            "B25035_001E": "median_year_built",
            "B25003_002E": "owner_occupied_units",
            "B25003_003E": "renter_occupied_units",
            "B15003_001E": "pop_25_plus",
            "B15003_022E": "bachelors_degree",
            "B15003_023E": "masters_degree",
            "B15003_024E": "professional_degree",
            "B15003_025E": "doctorate_degree",
            "B25070_001E": "total_renters_grapi",
            "B25070_007E": "renters_30_to_35_pct",
            "B25070_008E": "renters_35_to_40_pct",
            "B25070_009E": "renters_40_to_50_pct",
            "B25070_010E": "renters_50_plus_pct",
            "metropolitan statistical area/micropolitan statistical area": "cbsa_code",
        }
    )

    # Keep only Southeast metros so every later join stays in the same geography universe.
    df = df[df["metro_name_full"].apply(is_southeast_metro)].copy()
    df["metro_name"] = df["metro_name_full"].apply(clean_metro_name)
    df["primary_state_fips"] = df["metro_name_full"].apply(primary_state_fips)
    df["state_abbrevs"] = df["metro_name_full"].apply(lambda name: ",".join(metro_state_abbrevs(name)))
    df["source"] = "census_acs1_2023"
    df["pulled_at_utc"] = utc_now_iso()

    numeric_columns = [
        "median_home_value",
        "median_gross_rent",
        "median_household_income",
        "population",
        "median_age",
        "housing_units",
        "vacant_housing_units",
        "median_year_built",
        "owner_occupied_units",
        "renter_occupied_units",
        "pop_25_plus",
        "bachelors_degree",
        "masters_degree",
        "professional_degree",
        "doctorate_degree",
        "total_renters_grapi",
        "renters_30_to_35_pct",
        "renters_35_to_40_pct",
        "renters_40_to_50_pct",
        "renters_50_plus_pct",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    # Census uses -666666666 as a sentinel for suppressed or unavailable estimates.
    # Replace here so downstream code never needs to know about this source-specific sentinel.
    df[numeric_columns] = df[numeric_columns].replace(-666666666, pd.NA)

    # Fill any missing primary_state_fips from the parsed state abbreviations as a fallback.
    df["primary_state_fips"] = df["primary_state_fips"].fillna(
        df["state_abbrevs"].str.split(",").str[0].map(SOUTHEAST_STATES)
    )

    # Derived columns — computed here so they are always available in the base profile
    # regardless of which analysis or notebook consumes this table.
    df["owner_occupancy_rate"] = df["owner_occupied_units"] / df["housing_units"]
    df["renter_occupancy_rate"] = df["renter_occupied_units"] / df["housing_units"]
    df["college_attainment_rate"] = (
        df[["bachelors_degree", "masters_degree", "professional_degree", "doctorate_degree"]].sum(axis=1)
        / df["pop_25_plus"]
    )
    # Share of renters paying 30%+ of income on rent — standard HUD cost-burden threshold.
    df["cost_burden_rate"] = (
        df[["renters_30_to_35_pct", "renters_35_to_40_pct", "renters_40_to_50_pct", "renters_50_plus_pct"]].sum(axis=1)
        / df["total_renters_grapi"]
    )

    profile_df = df[
        [
            "cbsa_code",
            "metro_name",
            "metro_name_full",
            "primary_state_fips",
            "state_abbrevs",
            # Value, rent, income
            "median_home_value",
            "median_gross_rent",
            "median_household_income",
            # Population and demographics
            "population",
            "median_age",
            # Housing stock
            "housing_units",
            "vacant_housing_units",
            "median_year_built",
            # Tenure
            "owner_occupied_units",
            "renter_occupied_units",
            "owner_occupancy_rate",
            "renter_occupancy_rate",
            # Education
            "pop_25_plus",
            "college_attainment_rate",
            # Cost burden
            "total_renters_grapi",
            "cost_burden_rate",
            "source",
            "pulled_at_utc",
        ]
    ].sort_values("metro_name")

    # Small metro dimension table — one clean geography lookup reusable across joins.
    metro_dim_df = df[
        [
            "cbsa_code",
            "metro_name",
            "metro_name_full",
            "primary_state_fips",
            "state_abbrevs",
        ]
    ].drop_duplicates().sort_values("metro_name")
    metro_dim_df["region"] = "Southeast"
    metro_dim_df["pulled_at_utc"] = utc_now_iso()

    profile_df = profile_df.reset_index(drop=True)
    metro_dim_df = metro_dim_df.reset_index(drop=True)
    profile_df.to_csv(profile_cache, index=False)
    metro_dim_df.to_csv(metro_dim_cache, index=False)
    return profile_df, metro_dim_df


def fetch_fhfa_metro_hpi(valid_cbsa_codes: set[str]) -> pd.DataFrame:
    """Fetch FHFA all-transactions quarterly metro HPI history."""
    url = "https://www.fhfa.gov/hpi/download/quarterly_datasets/hpi_at_metro.csv"
    df = pd.read_csv(
        url,
        header=None,
        names=["metro_name", "cbsa_code", "year", "quarter", "hpi_index_nsa", "hpi_index_sa"],
    )

    # I use FHFA's CBSA codes so I can join this history back to the Census metros cleanly.
    df["cbsa_code"] = df["cbsa_code"].astype(str).str.zfill(5)
    df = df[df["cbsa_code"].isin(valid_cbsa_codes)].copy()
    df["metro_name"] = df["metro_name"].str.replace('"', "", regex=False).apply(clean_metro_name)

    numeric_columns = ["year", "quarter", "hpi_index_nsa", "hpi_index_sa"]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    # Fall back to the non-seasonally-adjusted series when the SA series is missing.
    # This is a source-level data quality fix, not an analytical choice.
    df["hpi_index_sa"] = df["hpi_index_sa"].fillna(df["hpi_index_nsa"])

    # I add a few notebook-friendly time columns so charting and filtering are easier later.
    df["quarter_label"] = "Q" + df["quarter"].astype("Int64").astype(str)
    df["period_end_date"] = pd.PeriodIndex.from_fields(
        year=df["year"].astype("Int64"),
        quarter=df["quarter"].astype("Int64"),
        freq="Q",
    ).end_time.date.astype(str)
    df["source"] = "fhfa_hpi_all_transactions"
    df["pulled_at_utc"] = utc_now_iso()

    return df[
        [
            "cbsa_code",
            "metro_name",
            "year",
            "quarter",
            "quarter_label",
            "period_end_date",
            "hpi_index_nsa",
            "hpi_index_sa",
            "source",
            "pulled_at_utc",
        ]
    ].sort_values(["cbsa_code", "year", "quarter"]).reset_index(drop=True)


def fetch_acs_growth_metrics(
    valid_cbsa_codes: set[str],
    profile_2023_df: pd.DataFrame,
) -> pd.DataFrame:
    """Pull 2019 ACS population and income, then compute growth rates to 2023.

    I use 2019 as the pre-pandemic baseline because it captures the full
    pandemic-era migration and wage shifts that are most relevant to current
    market conditions.
    """
    CACHE_DIR.mkdir(exist_ok=True)
    cache_file = CACHE_DIR / "acs_growth_metrics_2019_2023.csv"

    if cache_file.exists():
        return pd.read_csv(cache_file, dtype={"cbsa_code": str})

    df_2019 = fetch_census_acs_dataframe(
        year=2019,
        variables=[
            "B01003_001E",  # Population
            "B19013_001E",  # Median household income
            "B25003_003E",  # Renter-occupied units
        ],
        chunk_size=5,
    )
    df_2019 = df_2019.rename(
        columns={
            "NAME": "metro_name_full",
            "B01003_001E": "population_2019",
            "B19013_001E": "median_household_income_2019",
            "B25003_003E": "renter_occupied_2019",
            "metropolitan statistical area/micropolitan statistical area": "cbsa_code",
        }
    )
    df_2019["cbsa_code"] = df_2019["cbsa_code"].astype(str).str.zfill(5)
    df_2019 = df_2019[df_2019["cbsa_code"].isin(valid_cbsa_codes)].copy()

    base_cols_2019 = ["population_2019", "median_household_income_2019", "renter_occupied_2019"]
    for col in base_cols_2019:
        df_2019[col] = pd.to_numeric(df_2019[col], errors="coerce")
    df_2019[base_cols_2019] = df_2019[base_cols_2019].replace(-666666666, pd.NA)

    merged = df_2019[["cbsa_code"] + base_cols_2019].merge(
        profile_2023_df[
            ["cbsa_code", "population", "median_household_income", "renter_occupied_units", "housing_units"]
        ],
        on="cbsa_code",
        how="inner",
    )

    merged["pop_growth_pct"] = (
        (merged["population"] - merged["population_2019"]) / merged["population_2019"] * 100
    )
    merged["income_growth_pct"] = (
        (merged["median_household_income"] - merged["median_household_income_2019"])
        / merged["median_household_income_2019"] * 100
    )
    # Renter share change: how much the renter percentage of total housing stock shifted
    # between 2019 and 2023. Rising renter share signals increasing rental demand.
    merged["renter_share_2019"] = merged["renter_occupied_2019"] / merged["housing_units"]
    merged["renter_share_2023"] = merged["renter_occupied_units"] / merged["housing_units"]
    merged["renter_share_change_pct"] = (merged["renter_share_2023"] - merged["renter_share_2019"]) * 100

    merged["source"] = "acs_growth_2019_2023"
    merged["pulled_at_utc"] = utc_now_iso()

    growth_df = (
        merged[
            [
                "cbsa_code",
                "pop_growth_pct",
                "income_growth_pct",
                "renter_share_change_pct",
                "source",
                "pulled_at_utc",
            ]
        ]
        .sort_values("cbsa_code")
        .reset_index(drop=True)
    )
    growth_df.to_csv(cache_file, index=False)
    return growth_df


def fetch_bls_metro_employment_growth(
    metro_dim_df: pd.DataFrame,
    start_year: int = 2020,
    end_year: int = 2026,
) -> pd.DataFrame:
    """Fetch metro employment levels from BLS LAUS and compute year-over-year growth.

    I pull the employment level series (00000005) separately from the unemployment
    rate series so the job growth signal stays cleanly isolated.
    """
    bls_api_key = os.getenv("BLS_API_KEY", "").strip() or None
    cbsa_to_name = dict(zip(metro_dim_df["cbsa_code"], metro_dim_df["metro_name"]))

    # I build employment-level series IDs using the LAUS 00000005 measure code.
    series_lookup: dict[str, str] = {}
    for row in metro_dim_df.itertuples(index=False):
        if not row.primary_state_fips:
            continue
        series_lookup[row.cbsa_code] = f"LAUMT{row.primary_state_fips}{row.cbsa_code}00000005"

    if not series_lookup:
        raise ValueError("No BLS employment series IDs were created from the current metro universe.")

    CACHE_DIR.mkdir(exist_ok=True)
    cache_file = CACHE_DIR / f"bls_metro_employment_{start_year}_{end_year}.csv"

    if cache_file.exists():
        cached = pd.read_csv(cache_file, dtype={"cbsa_code": str})
        if "employment_level" in cached.columns:
            return cached
        cache_file.unlink()

    series_to_cbsa = {series_id: cbsa_code for cbsa_code, series_id in series_lookup.items()}
    records: list[dict[str, object]] = []
    max_series_per_request = 50 if bls_api_key else 25
    series_ids = list(series_lookup.values())

    for batch_start in range(0, len(series_ids), max_series_per_request):
        batch_series_ids = series_ids[batch_start : batch_start + max_series_per_request]
        payload = {
            "seriesid": batch_series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
        }
        if bls_api_key:
            payload["registrationkey"] = bls_api_key

        response = fetch_json(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            method="POST",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        results = response.get("Results", {})
        response_series = results.get("series")
        if response_series is None:
            message_text = " | ".join(response.get("message", [])) or "No BLS message returned."
            raise ValueError(
                "BLS metro employment request failed. "
                f"status={response.get('status', 'UNKNOWN')}; message={message_text}"
            )

        for series in response_series:
            cbsa_code = series_to_cbsa.get(series["seriesID"])
            if not cbsa_code:
                continue
            for row in series["data"]:
                period = row["period"]
                if not period.startswith("M"):
                    continue
                records.append(
                    {
                        "cbsa_code": cbsa_code,
                        "metro_name": cbsa_to_name[cbsa_code],
                        "year": int(row["year"]),
                        "month": int(period[1:]),
                        "employment_level": pd.to_numeric(row["value"], errors="coerce"),
                        "source": "bls_laus_employment",
                        "pulled_at_utc": utc_now_iso(),
                    }
                )

    df = pd.DataFrame(records)
    if df.empty:
        raise ValueError("BLS metro employment request returned no rows.")

    df = df.sort_values(["cbsa_code", "year", "month"]).reset_index(drop=True)

    # I compute YoY employment growth as a 12-month pct change on the monthly employment level.
    df["employment_yoy_growth_pct"] = (
        df.groupby("cbsa_code")["employment_level"].pct_change(12, fill_method=None) * 100
    )

    # I keep only the most recent valid YoY reading per metro for the snapshot join.
    growth_df = (
        df.dropna(subset=["employment_yoy_growth_pct"])
        .groupby("cbsa_code", as_index=False)
        .tail(1)[
            [
                "cbsa_code",
                "metro_name",
                "year",
                "month",
                "employment_level",
                "employment_yoy_growth_pct",
                "source",
                "pulled_at_utc",
            ]
        ]
        .sort_values(["cbsa_code"])
        .reset_index(drop=True)
    )

    growth_df.to_csv(cache_file, index=False)
    return growth_df


def build_bls_metro_series_ids(metro_dim_df: pd.DataFrame) -> dict[str, str]:
    """Build BLS metro unemployment-rate series IDs from state FIPS + CBSA."""
    series_lookup = {}
    for row in metro_dim_df.itertuples(index=False):
        if not row.primary_state_fips:
            continue
        # I build each LAUS series ID from the metro's primary state FIPS plus its CBSA code.
        series_lookup[row.cbsa_code] = f"LAUMT{row.primary_state_fips}{row.cbsa_code}00000003"
    return series_lookup


def fetch_bls_metro_unemployment(
    metro_dim_df: pd.DataFrame,
    start_year: int = 2020,
    end_year: int = 2026,
) -> pd.DataFrame:
    """Fetch metro unemployment-rate history from BLS LAUS."""
    bls_api_key = os.getenv("BLS_API_KEY", "").strip() or None
    series_lookup = build_bls_metro_series_ids(metro_dim_df)
    cbsa_to_name = dict(zip(metro_dim_df["cbsa_code"], metro_dim_df["metro_name"]))
    if not series_lookup:
        raise ValueError("No BLS metro series IDs were created from the current metro universe.")
    CACHE_DIR.mkdir(exist_ok=True)
    cache_file = CACHE_DIR / f"bls_metro_unemployment_{start_year}_{end_year}.csv"
    if cache_file.exists():
        return pd.read_csv(cache_file, dtype={"cbsa_code": str})
    series_to_cbsa = {series_id: cbsa_code for cbsa_code, series_id in series_lookup.items()}
    records: list[dict[str, object]] = []
    max_series_per_request = 50 if bls_api_key else 25
    series_ids = list(series_lookup.values())

    # I batch BLS requests so the notebook stays within the public API limits.
    for batch_start in range(0, len(series_ids), max_series_per_request):
        batch_series_ids = series_ids[batch_start : batch_start + max_series_per_request]
        payload = {
            "seriesid": batch_series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
        }
        if bls_api_key:
            # If I have a BLS API key, I attach it here so repeated notebook runs are less likely
            # to run into public-rate-limit friction.
            payload["registrationkey"] = bls_api_key

        response = fetch_json(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            method="POST",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        results = response.get("Results", {})
        response_series = results.get("series")
        if response_series is None:
            message_text = " | ".join(response.get("message", [])) or "No BLS message returned."
            raise ValueError(
                "BLS metro unemployment request failed. "
                f"status={response.get('status', 'UNKNOWN')}; message={message_text}"
            )

        for series in response_series:
            cbsa_code = series_to_cbsa.get(series["seriesID"])
            if not cbsa_code:
                continue

            for row in series["data"]:
                period = row["period"]
                if not period.startswith("M"):
                    continue

                # I keep monthly observations only because they are the most useful for this analysis.
                records.append(
                    {
                        "cbsa_code": cbsa_code,
                        "metro_name": cbsa_to_name[cbsa_code],
                        "series_id": series["seriesID"],
                        "year": int(row["year"]),
                        "month": int(period[1:]),
                        "period_name": row["periodName"],
                        "unemployment_rate": pd.to_numeric(row["value"], errors="coerce"),
                        "source": "bls_laus",
                        "pulled_at_utc": utc_now_iso(),
                    }
                )

    df = pd.DataFrame(records)
    if df.empty:
        raise ValueError("BLS metro unemployment request returned no rows.")

    df = df.sort_values(["cbsa_code", "year", "month"]).reset_index(drop=True)
    df.to_csv(cache_file, index=False)
    return df



def fetch_census_building_permits(
    valid_cbsa_codes: set[str],
    year: int = 2023,
) -> pd.DataFrame:
    """Fetch annual metro building permit totals from the Census Bureau BPS.

    Returns one row per metro with total units, single-family units, and
    multifamily (5+) units authorized. Supply pipeline data useful for
    construction activity, affordability pressure, and investor analyses.

    File format: Census publishes XLS files at
    https://www.census.gov/construction/bps/xls/msaannual_{year}99.xls
    Row 5 (0-indexed) is the header; CBSA code is in column 1.
    Requires xlrd in the environment (listed in requirements.txt).
    """
    CACHE_DIR.mkdir(exist_ok=True)
    cache_file = CACHE_DIR / f"census_building_permits_{year}.csv"
    if cache_file.exists():
        return pd.read_csv(cache_file, dtype={"cbsa_code": str})

    url = f"https://www.census.gov/construction/bps/xls/msaannual_{year}99.xls"
    try:
        resp = requests.get(
            url,
            timeout=(REQUEST_CONNECT_TIMEOUT, REQUEST_READ_TIMEOUT),
            headers={"User-Agent": "Mozilla/5.0 (compatible; housing-data-pipeline/1.0)"},
        )
        resp.raise_for_status()
        # Row 5 (0-indexed) is the header row in the Census BPS annual MSA XLS.
        # Columns: CSA | CBSA | Name | Total | 1 Unit | 2 Units | 3 and 4 Units | 5+ Units | ...
        raw = pd.read_excel(io.BytesIO(resp.content), engine="xlrd", header=5)
    except Exception as exc:
        raise ValueError(
            f"Could not fetch Census building permits for {year} from {url}: {exc}"
        ) from exc

    # Normalize column names.
    raw.columns = [
        str(c).strip().lower().replace(" ", "_").replace("-", "_") for c in raw.columns
    ]

    # CBSA code is the second column; the workbook often stores it as a float
    # like 10500.0, so normalize through numeric first before zero-padding.
    raw = raw.rename(columns={raw.columns[1]: "cbsa_code"})
    raw["cbsa_code"] = (
        pd.to_numeric(raw["cbsa_code"], errors="coerce")
        .astype("Int64")
        .astype(str)
        .replace("<NA>", pd.NA)
        .str.zfill(5)
    )
    df = raw[raw["cbsa_code"].isin(valid_cbsa_codes)].copy()

    # Rename the known permit columns to stable names.
    col_map = {
        raw.columns[3]: "permits_total_units",
        raw.columns[4]: "permits_single_family",
        raw.columns[7]: "permits_multifamily_5plus",
    }
    df = df.rename(columns=col_map)

    keep = ["cbsa_code", "permits_total_units", "permits_single_family", "permits_multifamily_5plus"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].copy()

    for col in keep[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["permits_year"] = year
    df["source"] = "census_bps_annual"
    df["pulled_at_utc"] = utc_now_iso()

    df = df.sort_values("cbsa_code").reset_index(drop=True)
    df.to_csv(cache_file, index=False)
    return df


# BLS SAE supersectors to pull for the industry employment breakdown.
# Series ID format: SMU{state_fips_2}{cbsa_5}{industry_8}1
# industry_8 = 2-digit supersector code + 6 zeros for the supersector-total level.
_SAE_SECTORS: dict[str, str] = {
    "emp_total_nonfarm":        "000000001",
    "emp_manufacturing":        "300000001",
    "emp_financial":            "550000001",
    "emp_professional_business":"600000001",
    "emp_education_health":     "650000001",
    "emp_leisure_hospitality":  "700000001",
    "emp_government":           "900000001",
}


def fetch_bls_industry_employment(
    metro_dim_df: pd.DataFrame,
    year: int = 2023,
) -> pd.DataFrame:
    """Fetch metro employment by industry sector from BLS State and Area Employment (SAE).

    Returns one row per metro with employment (thousands) for seven supersectors:
    total nonfarm, manufacturing, financial, professional/business, education/health,
    leisure/hospitality, and government. Useful for economic composition analysis,
    concentration risk, and sector-specific growth studies.

    Sectors with no BLS data for a given metro are returned as NaN — not all metros
    publish every sector, so missing values are expected for smaller markets.
    """
    bls_api_key = os.getenv("BLS_API_KEY", "").strip() or None
    cbsa_to_name = dict(zip(metro_dim_df["cbsa_code"], metro_dim_df["metro_name"]))

    CACHE_DIR.mkdir(exist_ok=True)
    cache_file = CACHE_DIR / f"bls_industry_employment_{year}.csv"
    if cache_file.exists():
        return pd.read_csv(cache_file, dtype={"cbsa_code": str})

    # Build one series ID per metro × sector combination.
    series_index: dict[str, tuple[str, str]] = {}
    for row in metro_dim_df.itertuples(index=False):
        if not row.primary_state_fips:
            continue
        for sector_name, industry_suffix in _SAE_SECTORS.items():
            series_id = f"SMU{row.primary_state_fips}{row.cbsa_code}{industry_suffix}"
            series_index[series_id] = (row.cbsa_code, sector_name)

    if not series_index:
        raise ValueError("No BLS SAE series IDs could be built from the metro dimension table.")

    series_ids = list(series_index.keys())
    max_per_request = 50 if bls_api_key else 25
    records: list[dict] = []

    for batch_start in range(0, len(series_ids), max_per_request):
        batch = series_ids[batch_start : batch_start + max_per_request]
        payload: dict = {"seriesid": batch, "startyear": str(year), "endyear": str(year)}
        if bls_api_key:
            payload["registrationkey"] = bls_api_key

        response = fetch_json(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            method="POST",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        results = response.get("Results", {})
        response_series = results.get("series")
        if response_series is None:
            message_text = " | ".join(response.get("message", [])) or "No message."
            raise ValueError(
                "BLS industry employment request failed. "
                f"status={response.get('status', 'UNKNOWN')}; message={message_text}"
            )

        for series in response_series:
            cbsa_code, sector_name = series_index.get(series["seriesID"], (None, None))
            if not cbsa_code or not series["data"]:
                continue
            # Prefer the annual average (period M13); fall back to the most recent month.
            annual = [r for r in series["data"] if r.get("period") == "M13"]
            row_data = annual[0] if annual else series["data"][0]
            records.append(
                {
                    "cbsa_code": cbsa_code,
                    "metro_name": cbsa_to_name.get(cbsa_code, ""),
                    "sector": sector_name,
                    "employment_thousands": pd.to_numeric(row_data["value"], errors="coerce"),
                }
            )

    if not records:
        # Return an empty but schema-correct frame so callers do not need to handle None.
        cols = ["cbsa_code", "metro_name"] + list(_SAE_SECTORS.keys()) + ["year", "source", "pulled_at_utc"]
        return pd.DataFrame(columns=cols)

    long_df = pd.DataFrame(records)
    wide_df = long_df.pivot_table(
        index=["cbsa_code", "metro_name"],
        columns="sector",
        values="employment_thousands",
        aggfunc="first",
    ).reset_index()
    wide_df.columns.name = None

    # Guarantee all sector columns exist even when some metros returned no data.
    for col in _SAE_SECTORS:
        if col not in wide_df.columns:
            wide_df[col] = pd.NA

    wide_df["year"] = year
    wide_df["source"] = "bls_sae_industry"
    wide_df["pulled_at_utc"] = utc_now_iso()

    wide_df = wide_df.sort_values("cbsa_code").reset_index(drop=True)
    wide_df.to_csv(cache_file, index=False)
    return wide_df


def build_latest_market_snapshot(
    census_profile_df: pd.DataFrame,
    fhfa_df: pd.DataFrame,
    bls_df: pd.DataFrame,
    growth_metrics_df: pd.DataFrame | None = None,
    employment_growth_df: pd.DataFrame | None = None,
    building_permits_df: pd.DataFrame | None = None,
    industry_employment_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Join the latest available metrics into one analysis-ready base table.

    All parameters after bls_df are optional — pass only what you have fetched.
    The snapshot always contains Census, FHFA, and BLS LAUS fields; the remaining
    columns are added when the corresponding DataFrame is provided.
    """
    fhfa_work = fhfa_df.sort_values(["cbsa_code", "year", "quarter"]).copy()

    # I calculate quarter-over-quarter and year-over-year HPI change as quick growth signals.
    fhfa_work["hpi_qoq_pct"] = (
        fhfa_work.groupby("cbsa_code")["hpi_index_sa"].pct_change(fill_method=None) * 100
    )
    fhfa_work["hpi_yoy_pct"] = (
        fhfa_work.groupby("cbsa_code")["hpi_index_sa"].pct_change(4, fill_method=None) * 100
    )
    latest_fhfa = fhfa_work.groupby("cbsa_code", as_index=False).tail(1)
    latest_fhfa = latest_fhfa[
        [
            "cbsa_code",
            "period_end_date",
            "hpi_index_sa",
            "hpi_qoq_pct",
            "hpi_yoy_pct",
        ]
    ].rename(columns={"period_end_date": "latest_hpi_period_end"})

    bls_work = bls_df.sort_values(["cbsa_code", "year", "month"]).copy()
    latest_bls = bls_work.groupby("cbsa_code", as_index=False).tail(1).copy()
    latest_bls["latest_unemployment_period"] = (
        latest_bls["year"].astype(str) + "-" + latest_bls["month"].astype(str).str.zfill(2)
    )
    latest_bls = latest_bls[
        [
            "cbsa_code",
            "latest_unemployment_period",
            "unemployment_rate",
        ]
    ]

    snapshot_df = census_profile_df.merge(latest_fhfa, on="cbsa_code", how="left").merge(
        latest_bls, on="cbsa_code", how="left"
    )

    if growth_metrics_df is not None:
        snapshot_df = snapshot_df.merge(
            growth_metrics_df[
                ["cbsa_code", "pop_growth_pct", "income_growth_pct", "renter_share_change_pct"]
            ],
            on="cbsa_code",
            how="left",
        )

    if employment_growth_df is not None:
        snapshot_df = snapshot_df.merge(
            employment_growth_df[["cbsa_code", "employment_level", "employment_yoy_growth_pct"]],
            on="cbsa_code",
            how="left",
        )
        # Employment-to-population ratio: share of the total population that is employed.
        # BLS LAUS metro employment level (series 00000005) is returned in persons,
        # so dividing directly by Census population yields a 0–1 fraction.
        # This signal cannot be gamed by labor force exit — if people leave the metro,
        # E/Pop falls even if the unemployment rate stays low.
        snapshot_df["employment_pop_ratio"] = (
            snapshot_df["employment_level"] / snapshot_df["population"]
        )

    # I create a few starter ratios here so I can begin exploring affordability right away.
    snapshot_df["price_to_income_ratio"] = (
        snapshot_df["median_home_value"] / snapshot_df["median_household_income"]
    )
    snapshot_df["annual_rent_to_income_ratio"] = (
        snapshot_df["median_gross_rent"] * 12 / snapshot_df["median_household_income"]
    )

    # Vacancy rate signals supply tightness — low vacancy means demand is outpacing supply.
    snapshot_df["vacancy_rate"] = (
        snapshot_df["vacant_housing_units"] / snapshot_df["housing_units"] * 100
    )

    # Gross yield measures rental income relative to purchase price — a key investor signal.
    snapshot_df["gross_yield"] = (
        snapshot_df["median_gross_rent"] * 12 / snapshot_df["median_home_value"] * 100
    )

    if building_permits_df is not None:
        permit_cols = [c for c in building_permits_df.columns if c.startswith("permits_")]
        snapshot_df = snapshot_df.merge(
            building_permits_df[["cbsa_code"] + permit_cols],
            on="cbsa_code",
            how="left",
        )

    if industry_employment_df is not None:
        sector_cols = [c for c in industry_employment_df.columns if c.startswith("emp_")]
        snapshot_df = snapshot_df.merge(
            industry_employment_df[["cbsa_code"] + sector_cols],
            on="cbsa_code",
            how="left",
        )

    return snapshot_df.sort_values("metro_name").reset_index(drop=True)


def load_all_source_tables(
    *,
    bls_start_year: int = 2020,
    bls_end_year: int = 2026,
    permits_year: int = 2023,
    industry_year: int = 2023,
    include_building_permits: bool = True,
    include_industry_employment: bool = False,
) -> dict[str, pd.DataFrame]:
    """Load all source tables in one call and return them in a dictionary.

    Census defines the metro universe; all other sources are filtered to the same
    set of CBSA codes so every table joins cleanly on cbsa_code.

    Returned keys:
        metro_dim_df          — geography lookup (one row per metro)
        census_profile_df     — ACS 2023 housing, income, demographics, tenure, education, cost burden
        fhfa_df               — FHFA quarterly HPI history
        bls_df                — BLS LAUS monthly unemployment rate history
        growth_metrics_df     — ACS 2019→2023 population, income, renter share growth
        employment_growth_df  — BLS LAUS employment level + YoY growth
        building_permits_df   — Census BPS annual permit totals by unit type
        industry_employment_df — BLS SAE employment by supersector (wide format)
    """
    census_profile_df, metro_dim_df = fetch_census_metros()
    valid_cbsa_codes = set(metro_dim_df["cbsa_code"])

    fhfa_df = fetch_fhfa_metro_hpi(valid_cbsa_codes)
    bls_df = fetch_bls_metro_unemployment(
        metro_dim_df,
        start_year=bls_start_year,
        end_year=bls_end_year,
    )
    growth_metrics_df = fetch_acs_growth_metrics(valid_cbsa_codes, census_profile_df)
    employment_growth_df = fetch_bls_metro_employment_growth(
        metro_dim_df,
        start_year=bls_start_year,
        end_year=bls_end_year,
    )
    if include_building_permits:
        building_permits_df = fetch_census_building_permits(valid_cbsa_codes, year=permits_year)
    else:
        building_permits_df = pd.DataFrame(
            columns=[
                "cbsa_code",
                "permits_total_units",
                "permits_single_family",
                "permits_multifamily_5plus",
                "permits_year",
                "source",
                "pulled_at_utc",
            ]
        )

    if include_industry_employment:
        industry_employment_df = fetch_bls_industry_employment(metro_dim_df, year=industry_year)
    else:
        industry_employment_df = pd.DataFrame(
            columns=[
                "cbsa_code",
                "metro_name",
                *list(_SAE_SECTORS.keys()),
                "year",
                "source",
                "pulled_at_utc",
            ]
        )

    # build_latest_market_snapshot() is intentionally not called here.
    # The notebook applies analysis-specific filters before building the snapshot.
    return {
        "metro_dim_df": metro_dim_df,
        "census_profile_df": census_profile_df,
        "fhfa_df": fhfa_df,
        "bls_df": bls_df,
        "growth_metrics_df": growth_metrics_df,
        "employment_growth_df": employment_growth_df,
        "building_permits_df": building_permits_df,
        "industry_employment_df": industry_employment_df,
    }


def main() -> None:
    """Run a quick local smoke test for the helper functions."""
    tables = load_all_source_tables()
    for name, df in tables.items():
        print(f"\n{name}:")
        print(df.head())
        print(f"shape={df.shape}")


if __name__ == "__main__":
    main()
