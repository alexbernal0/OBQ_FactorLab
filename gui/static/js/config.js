// Global config state
window.FL = {
  model: "quintile",
  factor: "value",
  index: "OBQ Investable Universe (Top 3000)",
  sector_exclusions: [],
  min_market_cap_B: null,
  min_price: 2.0,
  liquidity_adv_M: null,
  na_handling: "Exclude",
  winsorize: true,
  sector_neutral: false,
  n_quintiles: 5,
  top_n: 30,
  position_sizing: "Equal",
  direction: "Long Only",
  start_date: "1990-07-31",
  end_date: null,
  rebalance: "Monthly",
  commission_bps: 5.0,
  slippage_bps: 10.0,
  initial_capital: 1000000,
  rf_annual: 0.04,
  model_type: "quintile",
  // active run
  run_id: null,
  sse: null,
};

// Helpers
function pct(v, digits=1) {
  if (v == null || isNaN(v)) return "—";
  return (v*100).toFixed(digits) + "%";
}
function num(v, digits=2) {
  if (v == null || isNaN(v)) return "—";
  return parseFloat(v).toFixed(digits);
}
function pos_neg_cls(v) {
  if (v == null) return "";
  return v >= 0 ? "pos" : "neg";
}
