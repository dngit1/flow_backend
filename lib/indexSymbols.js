// Index tickers (S&P 500 etc.) - one shared list so the option-flow hub,
// the live price hub, the bars endpoint and the server's routing can't
// drift apart.
//
// Why indexes need their own handling: the stock price feed (Alpaca) has
// no index data at all, and for "SPX" it instead reports an unrelated
// security trading around $0.10. That wrong price fed the chart AND the
// strike selection for option flow (which put the SPX watch on strikes
// 1000-3500 while the real index sat near 7,700). For these symbols, the
// price comes from Tradier's own index quote instead.
//
// Extend as needed.
const INDEX_SYMBOLS = new Set(['SPX', 'NDX', 'RUT', 'VIX', 'DJX', 'XSP', 'OEX']);

function isIndexSymbol(symbol) {
  return INDEX_SYMBOLS.has(String(symbol || '').toUpperCase());
}

module.exports = { INDEX_SYMBOLS, isIndexSymbol };
