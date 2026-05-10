import sys; sys.path.insert(0,'.')
import duckdb

# Use PWB_indices as primary (purpose-built index table)
con = duckdb.connect(r'D:/OBQ_AI/pwb_data.duckdb', read_only=True)

print("=== PWB_indices columns ===")
cols = con.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='PWB_indices' ORDER BY ordinal_position").fetchall()
for c in cols:
    print(f"  {c[0]:<25} {c[1]}")

print("\n=== Sample SPX data ===")
sample = con.execute("""
    SELECT * FROM PWB_indices WHERE symbol='$SPX' 
    ORDER BY date LIMIT 5
""").fetchdf()
print(sample.to_string())

print("\n=== SPX coverage ===")
r = con.execute("""
    SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR, COUNT(*) 
    FROM PWB_indices WHERE symbol='$SPX'
""").fetchone()
print(f"  Range: {r[0]} to {r[1]}  ({r[2]:,} trading days)")

# Check what columns are available — price? total return? adjusted?
print("\n=== Sample around 1990 (our backtest start) ===")
s2 = con.execute("""
    SELECT * FROM PWB_indices 
    WHERE symbol='$SPX' AND date BETWEEN '1990-01-01' AND '1990-12-31'
    ORDER BY date LIMIT 5
""").fetchdf()
print(s2.to_string())

con.close()
