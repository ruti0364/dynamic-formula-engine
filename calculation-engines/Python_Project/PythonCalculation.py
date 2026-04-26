# -*- coding: utf-8 -*-
"""
Payment Calculation System — Python Implementation
===================================================

Architecture overview:
  The system loads dynamic formulas from SQL Server and evaluates each one
  across 1 million data rows using vectorized NumPy operations.
  Results are streamed to disk as Parquet (Snappy-compressed) to minimize
  memory pressure, then loaded into SQL Server via batched insertion.

Design decisions:
  1. Vectorized evaluation (pandas.eval + NumPy)
     Each formula runs across all 1M rows simultaneously at C-level speed.
     No Python-level loops over rows — orders of magnitude faster than eval().

  2. Per-formula Parquet files (Snappy compression)
     Each formula's results are written to an individual Parquet file
     immediately after calculation, then the DataFrame is released from memory.
     Peak RAM stays constant at ~1 formula (~80MB) regardless of formula count,
     compared to ~400MB+ if all results were accumulated in memory.
     Disk usage: ~8MB per formula vs ~180MB per formula with CSV (~95% saving).

  3. fast_executemany insertion
     SQLAlchemy's fast_executemany mode batches INSERT statements efficiently.
     Trade-off vs BULK INSERT: slightly slower, but avoids the 1.5GB CSV file
     required by SQL Server's BULK INSERT mechanism.

  4. Single shared connection for all inserts
     Opening one connection per formula adds ~50ms overhead per formula.
     A single connection reused across all formulas eliminates this cost.

  Scalability note:
     For datasets beyond ~100M rows, consider replacing pandas with Polars
     or Dask, which support out-of-core computation with lower memory overhead.
"""

import re
import time
import os
import shutil
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SERVER_NAME = r'.\SQLEXPRESS'
DB_NAME     = 'PaymentSystemDB'
TEMP_DIR    = r'C:\temp\parquet_batches'
METHOD      = 'Python'

_conn_str = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={SERVER_NAME};DATABASE={DB_NAME};Trusted_Connection=yes;'
)

# fast_executemany=True — batches INSERT statements instead of row-by-row
engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={_conn_str}",
    fast_executemany=True
)

# Compiled once — avoids recompiling the regex on every formula iteration
_IF_PATTERN = re.compile(r'\bif\s*\(', re.IGNORECASE)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_local_dict(data_df: pd.DataFrame) -> dict:
    """
    Build the evaluation context for pandas.eval.
    Maps formula variable names (a, b, c, d) to NumPy arrays,
    and formula function names to their NumPy equivalents.
    Using .values (ndarray) instead of Series avoids pandas overhead in eval.
    """
    ctx = {
        # Trigonometric
        'sin'   : np.sin,
        'cos'   : np.cos,
        'tan'   : np.tan,
        'asin'  : np.arcsin,
        'acos'  : np.arccos,
        'atan'  : np.arctan,
        # Exponential / logarithmic
        'exp'   : np.exp,
        'log'   : np.log,        # natural log (base e)
        'log10' : np.log10,
        'log2'  : np.log2,
        'sqrt'  : np.sqrt,
        # Rounding
        'floor' : np.floor,
        'ceil'  : np.ceil,
        'round' : np.round,
        # Misc
        'abs'   : np.abs,
        'sign'  : np.sign,
        'power' : np.power,
        # Conditional — replaces if(cond, a, b) after regex rewrite
        'where' : np.where,
    }
    for col in ('a', 'b', 'c', 'd'):
        ctx[col] = data_df[col].values
    return ctx


def _rewrite_formula(raw: str) -> str:
    """
    Rewrite if(...) → where(...) for NumPy vectorization.
    Uses a word-boundary regex to avoid matching substrings like 'notify('.
    """
    return _IF_PATTERN.sub('where(', raw)


def _parquet_path(formula_id: int) -> str:
    return os.path.join(TEMP_DIR, f'formula_{formula_id:03d}.parquet')


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run() -> None:
    t_start = time.time()

    # --- Prepare temp directory ---
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)   # remove any leftover files from previous run
    os.makedirs(TEMP_DIR)

    try:
        # --- Clean previous run in DB ---
        # Deletes only this method's rows — other methods remain for comparison
        print('Cleaning previous run data...')
        with engine.begin() as conn:
            conn.execute(text('DELETE FROM t_results WHERE method = :m'), {'m': METHOD})
            conn.execute(text('DELETE FROM t_log     WHERE method = :m'), {'m': METHOD})
        print('  Done.\n')

        # --- Step 1: Load data ---
        print('Step 1: Reading data from SQL Server...')
        with engine.connect() as conn:
            data_df     = pd.read_sql('SELECT data_id, a, b, c, d FROM t_data',    conn)
            formulas_df = pd.read_sql('SELECT targil_id, targil FROM t_targil',     conn)

        data_ids   = data_df['data_id'].values          # ndarray — reused every formula
        local_dict = _build_local_dict(data_df)
        n_formulas = len(formulas_df)
        print(f'  Loaded {len(data_df):,} data rows and {n_formulas} formulas.\n')

        # --- Step 2: Evaluate each formula and write to its own Parquet file ---
        # Each DataFrame is released immediately after writing (del df).
        # Peak RAM stays at ~1 formula (~80MB) regardless of formula count.
        print('Step 2: Calculating formulas...\n')
        formula_timings: list[tuple[int, float]] = []
        total_invalid   = 0
        total_rows      = 0

        for _, f_row in formulas_df.iterrows():
            f_id    = int(f_row['targil_id'])
            formula = _rewrite_formula(f_row['targil'])

            t0 = time.time()
            try:
                # engine='python' required — numexpr does not support np.where
                res = pd.eval(formula, engine='python', local_dict=local_dict)
                calc_duration = time.time() - t0

                # Sanitize: replace NaN / ±Infinity with 0.0
                arr           = np.asarray(res, dtype=np.float64)
                invalid_mask  = ~np.isfinite(arr)
                invalid_count = int(invalid_mask.sum())
                if invalid_count:
                    arr[invalid_mask] = 0.0
                    total_invalid    += invalid_count
                    print(f'  [WARN] Formula {f_id}: {invalid_count:,} '
                          f'NaN/Inf values replaced with 0.0')

                # Write this formula's results to Parquet and release memory
                df = pd.DataFrame({
                    'data_id'  : data_ids,
                    'targil_id': f_id,
                    'method'   : METHOD,
                    'result'   : arr,
                })
                df.to_parquet(_parquet_path(f_id), engine='pyarrow',
                              compression='snappy', index=False)
                total_rows += len(df)
                del df, arr   # explicit release — key to constant RAM usage

                formula_timings.append((f_id, calc_duration))
                print(f'  Formula {f_id:>2}: {calc_duration:.4f}s  '
                      f'→  formula_{f_id:03d}.parquet')

            except Exception as exc:
                print(f'  [ERROR] Formula {f_id} failed: {exc}')

        if not formula_timings:
            print('No formulas were processed successfully.')
            return

        # Report Parquet disk usage
        parquet_mb = sum(
            os.path.getsize(_parquet_path(fid))
            for fid, _ in formula_timings
        ) / (1024 * 1024)
        print(f'\n  Parquet total: {parquet_mb:.1f} MB  '
              f'(vs ~{total_rows * 28 / 1024 / 1024:.0f} MB for CSV)\n')

        # --- Step 3: Load Parquet files into SQL Server ---
        # Reads one file at a time — RAM stays at ~1 formula during insert.
        # Single shared connection — avoids open/close overhead per formula.
        print('Step 3: Loading results into SQL Server...')
        t_insert = time.time()

        with engine.begin() as conn:
            for f_id, _ in formula_timings:
                chunk = pd.read_parquet(_parquet_path(f_id))
                chunk.to_sql('t_results', con=conn,
                             if_exists='append', index=False,
                             chunksize=100_000)
                del chunk
                print(f'  Inserted formula_{f_id:03d}.parquet')

        insert_duration = time.time() - t_insert
        print(f'\n  Insert done in {insert_duration:.2f}s')

        # --- Step 4: Write execution log ---
        log_rows = [
            {'targil_id': fid, 'method': METHOD,
             'run_time' : dur + insert_duration / len(formula_timings)}
            for fid, dur in formula_timings
        ]
        pd.DataFrame(log_rows).to_sql(
            name='t_log', con=engine, if_exists='append', index=False)

        # --- Step 5: Cleanup temp files ---
        shutil.rmtree(TEMP_DIR)

        # --- Final report ---
        total_time = time.time() - t_start
        calc_total = sum(d for _, d in formula_timings)

        print('\n' + '=' * 55)
        print('FINAL PERFORMANCE REPORT (PYTHON)')
        print('=' * 55)
        print(f'  Formulas evaluated : {len(formula_timings)} / {n_formulas}')
        print(f'  Total rows inserted: {total_rows:,}')
        print(f'  Calculation time   : {calc_total:.2f}s')
        print(f'  SQL insert time    : {insert_duration:.2f}s')
        print(f'  Total time         : {total_time:.2f}s')
        print(f'  Parquet disk usage : {parquet_mb:.1f} MB  (Snappy compressed)')

        if total_invalid:
            print(f'\n  [NOTE] {total_invalid:,} NaN/Inf values replaced with 0.0 '
                  f'— may affect cross-method comparison accuracy.')
        else:
            print(f'\n  All {total_rows:,} values are finite — '
                  f'results fully comparable across methods.')
        print('=' * 55)
        print('\nSUCCESS — t_results and t_log updated.')

    finally:
        # Always clean up temp files, even if an exception occurred
        if os.path.exists(TEMP_DIR):
            shutil.rmtree(TEMP_DIR)


if __name__ == '__main__':
    run()
