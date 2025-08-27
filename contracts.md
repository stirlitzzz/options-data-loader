# Vol Warehouse Contracts

Minimal, unambiguous specs for the curated Parquet outputs. If a column is not here, it’s not relied on by downstream code.

## Directory Layout

- Raw pulls (from loader):
  - `${DATA_DIR}/raw/*.parquet`
- Curated tables (these contracts):
  - `${DATA_DIR}/curated/pairs.parquet`
  - `${DATA_DIR}/curated/atm.parquet`
  - `${DATA_DIR}/curated/smile_slope.parquet`
  - `${DATA_DIR}/curated/curve_header.parquet`
- Optional signal panels:
  - `${DATA_DIR}/signals/*.parquet`

> Tip: store per-year partitions if desired, e.g. `curated/2020/pairs.parquet`. Contracts below are identical per file.

## Conventions (apply everywhere)

- Dates: `DATE` type (`YYYY-MM-DD`).
- `tau` = `dte / 365.0` (years). Clamp with `GREATEST(tau, 1e-6)` when dividing.
- `S` = `underlying_price` (pair-averaged for C and P).
- `K` = `price_strike` (double).
- Moneyness:
  - `x = ln(K / S)` (dimensionless).
- Delta-weighted mid IV (per strike):
  - Let `delta_c01 = clamp(call_delta, 0, 1)`.
  - `ivol_mid = iv_call * (1 - delta_c01) + iv_put * delta_c01`.
- Quote tightness (dimensionless):
  - `vega_avg = (vega_c + vega_p) / 2`.
  - `half_spread_norm = min(Ask_p - Bid_p, Ask_c - Bid_c) / (vega_avg * 2)`.

---

## Table: pairs.parquet

**Grain (primary key):** `(stocks_id, c_date, expiration_date, K)`

| column              | type    | notes                                                        |
|---------------------|---------|--------------------------------------------------------------|
| c_date              | DATE    | trade date                                                  |
| stocks_id           | BIGINT  | IVolatility internal ID (stable across renames)            |
| expiration_date     | DATE    | option expiry                                               |
| K                   | DOUBLE  | strike                                                      |
| S                   | DOUBLE  | spot; average of call/put `underlying_price`               |
| dte                 | BIGINT  | days to expiry (from source)                                |
| tau                 | DOUBLE  | dte / 365.0                                                 |
| delta_c01           | DOUBLE  | clamped call delta in [0, 1]                                |
| iv_c                | DOUBLE  | call implied vol                                            |
| iv_p                | DOUBLE  | put implied vol                                             |
| vega_c              | DOUBLE  | call vega                                                   |
| vega_p              | DOUBLE  | put vega                                                    |
| ivol_mid            | DOUBLE  | delta-weighted IV (see Conventions)                         |
| half_spread_norm    | DOUBLE  | normalized half-offer spread                                |
| x                   | DOUBLE  | ln(K / S)                                                   |

**Required filters applied at build time (quality gates):**
- `ivol_mid BETWEEN 0.01 AND 5.0`
- `vega_c > 0 AND vega_p > 0`
- `ABS(x) < 1.0`
- `dte BETWEEN 1 AND 730`

---

## Table: atm.parquet

**Grain (primary key):** `(stocks_id, c_date, expiration_date)`

| column          | type   | notes                                                                 |
|-----------------|--------|-----------------------------------------------------------------------|
| stocks_id       | BIGINT |                                                                       |
| c_date          | DATE   |                                                                       |
| expiration_date | DATE   |                                                                       |
| S               | DOUBLE | carried from nearest-below row in `pairs` (same group)                |
| tau             | DOUBLE | carried from pairs                                                    |
| iv_atm          | DOUBLE | linear interpolation at `K = S` using the two closest strikes         |

**ATM interpolation:**
- Find `K_below <= S` and `K_above >= S` with smallest distance to `S`.
- If `K_above == K_below`, `iv_atm = (iv_above + iv_below)/2`.
- Else:
- `iv_atm = (iv_below * (K_above - S) + iv_above * (S - K_below)) / (K_above - K_below)`

---

## Table: smile_slope.parquet

**Grain (primary key):** `(stocks_id, c_date, expiration_date)`

| column          | type   | notes                                                                 |
|-----------------|--------|-----------------------------------------------------------------------|
| stocks_id       | BIGINT |                                                                       |
| c_date          | DATE   |                                                                       |
| expiration_date | DATE   |                                                                       |
| slope           | DOUBLE | weighted linear slope near ATM                                        |

**Model and weighting:**
- Define `X = (10.0 / sqrt(tau)) * x`.
- Define `Y = ivol_mid - iv_atm`.
- Weight per strike: `w = 1 / (half_spread_norm^2 + 1e-6)`; if missing, `w = 1`.
- Fit slope via weighted normal equations over strikes with `ABS(x) <= 0.3`:
`slope = (sxy - sxsy/sw) / (sxx - (sxsx)/sw)`
`where`
`sw = sum(w), sx = sum(wX), sy = sum(wY),`
`sxx = sum(wXX), sxy = sum(wXY)`

---

## Table: curve_header.parquet

**Grain (primary key):** `(stocks_id, c_date, expiration_date)`

| column          | type   | notes                                  |
|-----------------|--------|----------------------------------------|
| stocks_id       | BIGINT |                                        |
| c_date          | DATE   |                                        |
| expiration_date | DATE   |                                        |
| S               | DOUBLE |                                        |
| tau             | DOUBLE |                                        |
| iv_atm          | DOUBLE | from `atm.parquet`                     |
| slope           | DOUBLE | from `smile_slope.parquet`             |

**Reconstructing the near-ATM smile:**

`Given strike K:
x  = ln(K / S)
iv = iv_atm + slope * (10.0 / sqrt(tau)) * x`

---

## Optional: signals/atm_panel.parquet (starter)

**Grain:** `(stocks_id, c_date, expiration_date)`

| column     | type    | notes                                        |
|------------|---------|----------------------------------------------|
| stocks_id  | BIGINT  |                                              |
| c_date     | DATE    |                                              |
| expiration_date | DATE |                                           |
| tau        | DOUBLE  |                                              |
| S          | DOUBLE  |                                              |
| iv_atm     | DOUBLE  |                                              |
| slope      | DOUBLE  |                                              |
| is_tight   | BOOLEAN | quality flag, e.g. median half_spread_norm <= 0.05 |

---

## Regeneration Commands (examples)

Rebuild all tables for a window:
```bash
scripts/build_curves.sh 2005-01-01 2006-12-31

Direct one-offs with envsubst:

DATA_DIR="$IVOL_DATA_DIR" START_DATE="2006-01-01" END_DATE="2006-12-31" \
envsubst < sql/01_pairs.sql | duckdb


