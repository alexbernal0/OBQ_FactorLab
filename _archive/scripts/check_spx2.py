import duckdb

# Check PWB_indices
con = duckdb.connect(r'D:/OBQ_AI/pwb_data.duckdb', read_only=True)
syms = [s[0] for s in con.execute('SELECT DISTINCT symbol FROM PWB_indices ORDER BY symbol LIMIT 30').fetchall()]
print('PWB_indices symbols:', syms)

r = con.execute('SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR, COUNT(*) FROM PWB_indices WHERE symbol IN (\'SPX\',\'$SPX\',\'GSPC\',\'SP500\')').fetchone()
print('SPX variants in PWB:', r)

# Try case-insensitive
r2 = con.execute("SELECT symbol, COUNT(*), MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM PWB_indices GROUP BY symbol ORDER BY MIN(date) LIMIT 10").fetchall()
print('All PWB symbols with dates:')
for row in r2:
    print(f"  {row[0]:<15} {row[2]} -> {row[3]}  ({row[1]:,} rows)")
con.close()

# Check EH_OHLCV
print()
con2 = duckdb.connect(r'D:/OBQ_AI/eh_research.duckdb', read_only=True)
syms2 = [s[0] for s in con2.execute('SELECT DISTINCT symbol FROM EH_OHLCV ORDER BY symbol LIMIT 30').fetchall()]
print('EH_OHLCV symbols:', syms2)

r3 = con2.execute("SELECT symbol, COUNT(*), MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM EH_OHLCV GROUP BY symbol ORDER BY MIN(date) LIMIT 10").fetchall()
print('All EH symbols with dates:')
for row in r3:
    print(f"  {row[0]:<15} {row[2]} -> {row[3]}  ({row[1]:,} rows)")
con2.close()
