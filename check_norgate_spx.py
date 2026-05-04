import sys; sys.path.insert(0,'.')
import duckdb, os
from dotenv import load_dotenv; load_dotenv()

# Check all possible DB files for Norgate data
possible_dbs = [
    os.environ.get('OBQ_EODHD_MIRROR_DB', r'D:/OBQ_AI/obq_eodhd_mirror.duckdb'),
    r'D:/OBQ_AI/obq_ai.duckdb',
    r'D:/OBQ_AI/backtest.duckdb',
    r'D:/Master Data Backup 2025/norgate_data.duckdb',
    r'D:/OBQ_AI/norgate.duckdb',
    r'D:/OBQ_AI/market_data.duckdb',
]

import glob
# Also search for any duckdb file on D drive
extra = glob.glob(r'D:/OBQ_AI/*.duckdb') + glob.glob(r'D:/OBQ_AI/**/*.duckdb', recursive=True)
all_dbs = list(set(possible_dbs + extra))

print("Searching for SPX data across all DuckDB files...")
for db_path in all_dbs:
    if not os.path.exists(db_path):
        continue
    try:
        con = duckdb.connect(db_path, read_only=True)
        tables = [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()]
        
        for tbl in tables:
            cols = [r[0].lower() for r in con.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name='{tbl}'"
            ).fetchall()]
            sym_col = next((c for c in cols if c == 'symbol' or c == 'ticker'), None)
            if not sym_col: continue
            
            r = con.execute(f"""
                SELECT COUNT(*), MIN(date)::VARCHAR, MAX(date)::VARCHAR 
                FROM {tbl} WHERE {sym_col} IN ('$SPX','SPX','GSPC','^SPX','^GSPC','SP500')
            """).fetchone()
            if r[0] > 0:
                print(f"  FOUND: {db_path}")
                print(f"    Table: {tbl}  Symbol found | {r[1]} -> {r[2]}  ({r[0]:,} rows)")
        con.close()
    except Exception as e:
        pass  # skip inaccessible DBs

# Also check the JCN dashboard DB path
print("\nChecking JCNDashboardApp paths...")
jcn_paths = glob.glob(r'C:/Users/admin/Desktop/JCNDashboardApp/**/*.duckdb', recursive=True)
jcn_paths += glob.glob(r'D:/**/*norgate*.duckdb', recursive=True)
jcn_paths += glob.glob(r'D:/**/*index*.duckdb', recursive=True)
for p in jcn_paths[:10]:
    print(f"  Found DB: {p}")
