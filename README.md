# Southeast Housing Market Analysis

A three-pillar data pipeline and scoring framework that ranks Southeast U.S. metros on affordability, growth, and labor-market strength, then identifies which markets best balance upside with economic stability.

## Start Here

- **Project summary page**: [Live project summary](https://jessecoggins.github.io/southeast-housing-market-analysis/)
- **Analysis notebook**: [southeast_housing_analysis.ipynb](./southeast_housing_analysis.ipynb)

---

## Research Question

Which Southeast metros rank highest when affordability, growth, and labor-market strength are evaluated together, and which best balance growth potential with labor-market stability?

---

## Who This Is For

This framework is built for anyone who needs to screen Southeast housing markets before committing to deeper due diligence:

- **Homebuyers** relocating within or into the Southeast who want to understand which markets offer the best combination of price-entry point, appreciation potential, and economic stability, not just the cheapest or the fastest-growing
- **Real estate investors** underwriting new positions who need a data-driven way to distinguish markets where price growth is backed by genuine employment and population demand from markets where it is speculative
- **Lenders and risk analysts** setting market-level underwriting parameters who want a transparent, reproducible signal of labor-market depth beyond the headline unemployment rate
- **Regional planners and economic development officials** benchmarking local market performance against comparable metros in the region

The scoring framework is intentionally transparent and reproducible. All scoring inputs come from free public Census, FHFA, and BLS source data, all weights are explicit, and the full methodology is documented in the notebook so practitioners can adapt the framework to different geographies or weight assumptions.

---

## Key Findings

- **Huntsville, AL** is the top-ranked metro on both the overall score and the balance score, making it the clearest example of a market that combines reasonable affordability, real population growth, and unusual labor-market depth.
- **Fayetteville-Springdale-Rogers, AR** and **Nashville, TN** round out the balance top three behind Huntsville, with high scores on both growth potential and labor-market stability.
- **Florence-Muscle Shoals, AL** and **Decatur, AL** rank highest on raw affordability but carry weaker growth signals.
- Larger, higher-cost metros are present in the dataset but generally do not surface near the top because affordability deteriorated faster than local income growth.
- The geometric mean balance score reveals that several high-overall-score markets are strong on affordability but weak on growth, a split the simple average would obscure.

---

## Data Sources

| Source | What It Provides | Vintage |
|---|---|---|
| U.S. Census ACS 1-Year | Median home value, gross rent, household income, population, housing units, vacancy, tenure, education, cost burden | 2023 (current), 2019 (growth baseline) |
| FHFA House Price Index | Quarterly metro-level all-transactions HPI used to derive quarter-over-quarter and year-over-year price momentum | Latest available quarter in the project snapshot |
| BLS LAUS (series 00000003) | Metro unemployment rate | Latest monthly observation in the project snapshot |
| BLS LAUS (series 00000005) | Metro employment level, used to compute Employment-to-Population ratio and YoY employment growth | Latest monthly observation in the project snapshot |
| U.S. Census Building Permits Survey | Metro permit totals by structure type | 2023 |
| BLS State and Area Employment | Metro supersector employment mix | 2023, optional |

Population growth and income growth are computed as percent change from 2019 to 2023 using paired ACS pulls. Employment growth is the year-over-year percent change between the latest project-snapshot month and the same month one year earlier in the BLS employment-level series.

---

## Project Structure

```
SE Housing Market Analysis/
├── data_ingestion.py                 # Full data pipeline: public Census, FHFA, and BLS pulls + caching
├── southeast_housing_analysis.ipynb  # Analysis notebook: cleaning, EDA, scoring, ranking
├── requirements.txt                  # Python dependencies
├── README.md                         # This file
└── cache/                            # Auto-generated on first run — not committed to version control
    ├── census_acs_profile_2023.csv
    ├── census_metro_dim_2023.csv
    ├── acs_growth_metrics_2019_2023.csv
    ├── bls_metro_unemployment_YYYY_YYYY.csv
    ├── bls_metro_employment_YYYY_YYYY.csv
    └── census_building_permits_2023.csv
```

### Pipeline Flow

```
data_ingestion.py
  └── load_all_source_tables()
        ├── fetch_census_metros()               → Census ACS 2023 profile + metro dimension
        ├── fetch_fhfa_metro_hpi()              → FHFA HPI quarterly
        ├── fetch_bls_metro_unemployment()      → BLS LAUS unemployment rate (series 00000003)
        ├── fetch_acs_growth_metrics()          → Census ACS 2019 + growth vs. 2023
        ├── fetch_bls_metro_employment_growth() → BLS employment level + YoY growth (series 00000005)
        ├── fetch_census_building_permits()     → Census BPS permit activity [fetched; not used in current scoring model]
        └── fetch_bls_industry_employment()     → BLS supersector employment [optional; not used in current scoring model]

southeast_housing_analysis.ipynb
  ├── 0.  Imports and Setup
  ├── 1.  ETL and Data Sourcing
  │     └── build_latest_market_snapshot()     → merged, analysis-ready table
  ├── 2.  Raw Data Inspection
  ├── 3.  Data Quality Review
  ├── 4.  Data Cleaning
  ├── 5.  Exploratory Data Analysis
  ├── 6.  Feature Engineering
  ├── 7.  Methodology and Scoring
  ├── 7a. Sensitivity Check
  ├── 8.  Market Ranking
  ├── 9.  Findings and Recommendations
  └── 10. Limitations and Next Steps
```

---

## Methodology

### Scoring Pillars

Three approximately equal-weight pillars combine into an **Overall Score**:

| Pillar | Weight | Components |
|---|---|---|
| Affordability | 33% | Price-to-income percentile (50%), Rent-to-income percentile (50%) |
| Growth | 34% | HPI quarter-over-quarter (20%), HPI year-over-year (20%), Population growth (40%), Vacancy rate tightness (20%) |
| Labor Market | 33% | Employment-to-Population ratio (30%), Unemployment rate (20%), Employment growth (20%), Income level (15%), Income growth (15%) |

All features are converted to percentile ranks (0–1) before scoring. Higher is always better within each feature. For affordability, the percentile direction is intentionally reversed, so lower price-to-income and rent-to-income ratios receive higher affordability scores. In the notebook outputs, market tiers such as `Top Tier` are defined with `pd.qcut(q=3)`, so they represent relative thirds of the final score distribution rather than fixed score cutoffs.

### Balance Score

The **Balance Score** is the geometric mean of the Growth and Labor Market pillar scores:

```
balance_score = sqrt(growth_score × labor_market_score)
```

The geometric mean penalizes lopsided markets. A metro with growth=0.9 and labor_market=0.3 scores 0.52 on balance. A metro with growth=0.7 and labor_market=0.7 scores 0.70. This surfaces markets that are genuinely strong on both dimensions rather than excellent on one. Affordability is intentionally excluded from the balance score so this second ranking measures resilience and momentum, not simple cheapness.

### Why Employment-to-Population Ratio?

The unemployment rate can stay low when people leave a metro. If the labor force shrinks, unemployed workers may not be counted. The Employment-to-Population (E/Pop) ratio avoids this by measuring employment relative to the metro's total population, so labor-force exit shows up as a weaker ratio rather than a deceptively stable unemployment rate. This was a deliberate design choice to avoid over-ranking metros where low unemployment reflects population loss rather than genuine economic strength.

---

## Setup and Reproduction

### Requirements

All notebook and pipeline dependencies are listed in `requirements.txt`.

Install with:
```bash
python3 -m pip install -r requirements.txt
```

### API Keys

A free BLS API key is recommended for the BLS pulls and required if you choose to enable the optional industry-employment fetch. Register at [bls.gov/developers](https://www.bls.gov/developers/) and set the environment variable before running the notebook:

```python
import os
os.environ["BLS_API_KEY"] = "your_key_here"
```

An early setup cell in the notebook checks whether `BLS_API_KEY` is already available in your environment.

### Running

1. Open `southeast_housing_analysis.ipynb` in Jupyter
2. Set your BLS API key in the setup cell near the top if it is not already present in your environment
3. Run all cells top to bottom. The pipeline fetches and caches the core analysis tables automatically on first run.

By default, `load_all_source_tables()` loads the core tables used in the scoring model plus building permits. The optional BLS industry-employment table is skipped unless you explicitly call `load_all_source_tables(include_industry_employment=True)`.

Cache files are written to the working directory. Subsequent runs read from cache and are significantly faster. If underlying data changes (new FHFA quarter, new ACS vintage), delete the relevant cache CSV to force a re-pull.

---

## Recommended Actions

The rankings are a screening tool. Here is the concrete next step for each stakeholder type:

| Stakeholder | Action |
|---|---|
| **Homebuyer** | Shortlist Huntsville first if you want the strongest all-around market. Add Florence-Muscle Shoals or Decatur if lowest entry price matters more than upside. Use the balance ranking when you want to distinguish genuinely resilient metros from places that are simply inexpensive. |
| **Investor** | Start with the balance list, not just the overall list. Huntsville and Fayetteville-Springdale-Rogers are the clearest cases where appreciation appears supported by labor-market depth. Validate with rent comps, cap rates, vacancy, and current supply before committing. |
| **Lender / Risk Analyst** | Use the labor-market score, especially E/Pop ratio and income growth, as the primary credit-quality signal. High affordability paired with a weak balance score often signals fragile demand rather than value and should push underwriting more conservative. |
| **Regional Planner** | Treat the balance top five as benchmark comps for what sustained, two-dimensional growth looks like. For underperforming metros, identify whether the binding constraint is growth, labor depth, or affordability pressure, then treat that gap as the policy lever. |

This analysis does not replace local due diligence. Rankings are point-in-time macro signals. Validate with active listings, days on market, employer news, and current local market conditions before any capital decision.

---

## Limitations

- **Complete-case analysis**: metros missing any of the core required metrics — HPI index, HPI QoQ, HPI YoY, unemployment rate, population growth, income growth, employment growth, employment-to-population ratio, vacancy rate, or gross yield — are excluded from the final ranking. This makes the sample smaller and adjacent ranks less precise.
- **ACS CBSA coverage and source overlap**: the Census universe begins with Southeast metropolitan and micropolitan CBSAs. ACS 1-year coverage is broader for metropolitan areas than for micropolitan areas, but the final scored sample is constrained more by FHFA/BLS overlap and complete-case requirements than by ACS alone.
- **Loaded data vs. scored data**: the pipeline can also pull building permits by default and industry employment optionally, but those datasets are not part of the current scoring model. They are included for exploration and future model expansion rather than the ranked output shown here.
- **Percentile scoring and simple weighting**: percentile ranks make unlike variables easy to compare and keep the framework transparent, but they also compress real magnitude differences between metros. The notebook includes a lightweight sensitivity check across alternative pillar weights, yet the weights remain transparent choices rather than empirically optimized ones.
- **No historical validation/backtesting**: this version is designed as a screening tool, but it is not backtested to show whether earlier vintages would have predicted later appreciation or labor-market outperformance.
- **Nominal growth measures**: income growth is measured in nominal terms rather than inflation-adjusted terms. That is acceptable for cross-metro comparison within the same window, but it can overstate real household purchasing-power gains over 2019–2023.
- **Point-in-time snapshot**: the scoring reflects the most recent data vintage included in this project snapshot. It does not forecast future conditions or account for local policy changes, new employer announcements, or macro interest rate shifts.

## Next Steps

- **Backtest earlier vintages**: apply the framework to earlier snapshots and compare those rankings with later housing and labor-market outcomes.
- **Test alternative standardization and weighting schemes**: evaluate z-scores or winsorized z-scores alongside percentile ranks, and compare transparent weights with empirically tuned alternatives.
- **Compare against external benchmarks**: check whether the ranking broadly agrees with migration, housing-demand, and published market-strength measures.
- **Extend supply-side and affordability context**: add deeper inputs such as building permits, interest-rate-adjusted affordability, sub-market rental vacancy, multiple ACS vintages, and wage growth by sector.
