/**
 * app.js — LSP frontend logic
 *
 * Responsibilities:
 *   1. Load S&P 500 universe → populate searchable dropdown
 *   2. Load Google Trends → render trends panel
 *   3. On stock selection → fetch metrics + signal → update stock panel
 *
 * All API calls use fetch() with error handling and loading states.
 */

'use strict';

// ── DOM refs ──────────────────────────────────────────────────────────────────
const stockInput    = document.getElementById('stock-input');
const dropdown      = document.getElementById('dropdown');
const inputSpinner  = document.getElementById('input-spinner');
const emptyState    = document.getElementById('empty-state');
const stockData     = document.getElementById('stock-data');
const stockLoading  = document.getElementById('stock-loading');
const trendsList    = document.getElementById('trends-list');
const trendsSkeleton = document.getElementById('trends-skeleton');

// ── State ─────────────────────────────────────────────────────────────────────
let allStocks = [];        // [{ticker, company, sector}]
let focusedIndex = -1;     // keyboard nav in dropdown
let selectedTicker = null;

// ── Number formatters ─────────────────────────────────────────────────────────

/**
 * Format a value based on its type.
 * Returns '—' for null/undefined/NaN.
 */
function fmt(value, type) {
  if (value === null || value === undefined || value === '' || Number.isNaN(value)) return '—';
  const n = parseFloat(value);
  if (!isFinite(n)) return '—';

  switch (type) {
    case 'price':
      return '$' + n.toFixed(2);
    case 'pct':
      // Value is already a decimal (e.g. 0.432) → multiply by 100
      return (n * 100).toFixed(1) + '%';
    case 'pct_raw':
      // Value is already a percentage (e.g. 43.2)
      return n.toFixed(1) + '%';
    case 'large':
      if (n >= 1e12) return '$' + (n / 1e12).toFixed(2) + 'T';
      if (n >= 1e9)  return '$' + (n / 1e9).toFixed(2) + 'B';
      if (n >= 1e6)  return '$' + (n / 1e6).toFixed(2) + 'M';
      return '$' + n.toFixed(0);
    case 'ratio':
      return n.toFixed(2) + 'x';
    case 'multiple':
      return n.toFixed(1) + 'x';
    case 'plain':
      return n.toFixed(2);
    default:
      return String(value);
  }
}

function setText(id, text, cssClass = '') {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  el.className = el.className.replace(/\b(positive|negative|na)\b/g, '').trim();
  if (cssClass) el.classList.add(cssClass);
}

// ── Universe loading ──────────────────────────────────────────────────────────

async function loadUniverse() {
  try {
    inputSpinner.classList.remove('hidden');
    const resp = await fetch('/api/universe');
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    allStocks = await resp.json();
    stockInput.placeholder = `Search ${allStocks.length} S&P 500 stocks by ticker or name…`;
  } catch (err) {
    console.warn('Universe load failed:', err);
    stockInput.placeholder = 'Search S&P 500 stocks…';
  } finally {
    inputSpinner.classList.add('hidden');
  }
}

// ── Dropdown logic ────────────────────────────────────────────────────────────

function filterStocks(query) {
  const q = query.trim().toLowerCase();
  if (!q) return [];
  return allStocks
    .filter(s =>
      s.ticker.toLowerCase().startsWith(q) ||
      s.company.toLowerCase().includes(q)
    )
    .slice(0, 25); // cap at 25 for performance
}

function renderDropdown(matches) {
  focusedIndex = -1;

  if (!matches.length) {
    dropdown.innerHTML = '<div class="dropdown-no-results">No results</div>';
    return;
  }

  dropdown.innerHTML = matches.map((s, i) => `
    <div class="dropdown-item" data-ticker="${s.ticker}" data-index="${i}">
      <span class="dropdown-ticker">${s.ticker}</span>
      <span class="dropdown-company">${s.company}</span>
      <span class="dropdown-sector">${s.sector || ''}</span>
    </div>
  `).join('');

  dropdown.querySelectorAll('.dropdown-item').forEach(item => {
    item.addEventListener('mousedown', e => {
      e.preventDefault();
      selectStock(item.dataset.ticker);
    });
  });
}

function closeDropdown() {
  dropdown.innerHTML = '';
  focusedIndex = -1;
}

stockInput.addEventListener('input', () => {
  const q = stockInput.value;
  if (!q.trim()) { closeDropdown(); return; }
  renderDropdown(filterStocks(q));
});

stockInput.addEventListener('keydown', e => {
  const items = dropdown.querySelectorAll('.dropdown-item');
  if (!items.length) return;

  if (e.key === 'ArrowDown') {
    e.preventDefault();
    focusedIndex = Math.min(focusedIndex + 1, items.length - 1);
    items.forEach((el, i) => el.classList.toggle('focused', i === focusedIndex));
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    focusedIndex = Math.max(focusedIndex - 1, 0);
    items.forEach((el, i) => el.classList.toggle('focused', i === focusedIndex));
  } else if (e.key === 'Enter' && focusedIndex >= 0) {
    e.preventDefault();
    const focused = items[focusedIndex];
    if (focused) selectStock(focused.dataset.ticker);
  } else if (e.key === 'Escape') {
    closeDropdown();
    stockInput.blur();
  }
});

stockInput.addEventListener('blur', () => {
  // Small delay so mousedown on dropdown item fires first
  setTimeout(closeDropdown, 150);
});

// ── Stock selection ───────────────────────────────────────────────────────────

async function selectStock(ticker) {
  selectedTicker = ticker;
  const stock = allStocks.find(s => s.ticker === ticker);

  // Update input to show selected ticker
  stockInput.value = stock ? `${stock.ticker} — ${stock.company}` : ticker;
  closeDropdown();

  // Show loading state
  emptyState.classList.add('hidden');
  stockData.classList.add('hidden');
  stockLoading.classList.remove('hidden');

  try {
    const resp = await fetch(`/api/stock/${encodeURIComponent(ticker)}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    renderStockPanel(data);
  } catch (err) {
    console.error('Stock fetch failed:', err);
    stockLoading.classList.add('hidden');
    emptyState.classList.remove('hidden');
    document.querySelector('.empty-title').textContent = 'Failed to load data';
    document.querySelector('.empty-sub').textContent = 'Please try again in a moment';
  }
}

// ── Stock panel rendering ─────────────────────────────────────────────────────

function renderStockPanel(data) {
  // Company header
  document.getElementById('company-name').textContent =
    data.company_name || data.ticker;
  document.getElementById('ticker-badge').textContent = data.ticker;
  document.getElementById('sector-tag').textContent   = data.sector || '';

  // Price
  const price = data.current_price;
  document.getElementById('current-price').textContent =
    price != null ? fmt(price, 'price') : '—';

  // Signal badge
  const badge = document.getElementById('signal-badge');
  const signal = (data.signal || 'HOLD').toUpperCase();
  badge.textContent = signal;
  badge.className = 'signal-badge';
  badge.classList.add(
    signal === 'BUY'  ? 'signal-buy'  :
    signal === 'SELL' ? 'signal-sell' :
    signal === 'HOLD' ? 'signal-hold' : 'signal-none'
  );

  // Score bar (range -5 to +5, normalised to 0-100%)
  const score = data.score ?? 0;
  const pct   = Math.round(((score + 5) / 10) * 100);
  const fill  = document.getElementById('score-fill');
  fill.style.width = pct + '%';
  fill.style.background =
    signal === 'BUY'  ? '#00C853' :
    signal === 'SELL' ? '#FF1744' : '#FFB300';
  document.getElementById('score-value').textContent = `${score > 0 ? '+' : ''}${score} / 5`;

  // Financial metrics
  setText('m-pe',          fmt(data.pe_ratio,       'plain'));
  setText('m-eps',         data.eps != null ? fmt(data.eps, 'price') : '—');
  setText('m-mktcap',      fmt(data.market_cap,     'large'));
  setText('m-revenue',     fmt(data.revenue,        'large'));
  setText('m-grossmargin', fmt(data.gross_margin,   'pct'));
  setText('m-de',          fmt(data.debt_to_equity, 'ratio'));
  setText('m-roe',         fmt(data.roe,            'pct'));
  setText('m-pb',          fmt(data.pb_ratio,       'multiple'));

  const hi = data.week_52_high, lo = data.week_52_low;
  document.getElementById('m-52w').textContent =
    (hi != null && lo != null) ? `$${parseFloat(lo).toFixed(2)} – $${parseFloat(hi).toFixed(2)}` : '—';

  const dy = data.dividend_yield;
  document.getElementById('m-divyield').textContent =
    dy != null ? (dy * 100).toFixed(2) + '%' : '—';

  // Apply colour hints to key metrics
  applyMetricColors(data);

  // Indicator votes
  renderVotes(data.votes || {});

  // Show panel
  stockLoading.classList.add('hidden');
  stockData.classList.remove('hidden');
}

function applyMetricColors(data) {
  // ROE: green if > 15%, red if negative
  if (data.roe != null) {
    const el = document.getElementById('m-roe');
    if (data.roe > 0.15)  el.classList.add('positive');
    else if (data.roe < 0) el.classList.add('negative');
  }
  // Gross margin: green if > 40%, red if < 10%
  if (data.gross_margin != null) {
    const el = document.getElementById('m-grossmargin');
    if (data.gross_margin > 0.40)  el.classList.add('positive');
    else if (data.gross_margin < 0.10) el.classList.add('negative');
  }
  // D/E: red if > 2x
  if (data.debt_to_equity != null) {
    const el = document.getElementById('m-de');
    if (data.debt_to_equity > 200) el.classList.add('negative');
  }
}

function renderVotes(votes) {
  const container = document.getElementById('votes-row');
  const labels = {
    gap:          'Gap',
    momentum:     'Momentum',
    vwap:         'VWAP',
    volume:       'Volume',
    macro_trend:  'Macro',
  };

  container.innerHTML = Object.entries(votes).map(([key, val]) => {
    const label = labels[key] || key;
    const cls   = val > 0 ? 'vote-up' : val < 0 ? 'vote-down' : 'vote-flat';
    const icon  = val > 0 ? '↑' : val < 0 ? '↓' : '→';
    return `<span class="vote-chip ${cls}">${icon} ${label}</span>`;
  }).join('');
}

// ── Google Trends panel ───────────────────────────────────────────────────────

async function loadTrends() {
  try {
    const resp = await fetch('/api/trends');
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const payload = await resp.json();
    renderTrends(payload.data || []);
  } catch (err) {
    console.warn('Trends load failed:', err);
    renderTrends([]);
  }
}

function renderTrends(trends) {
  if (!trends.length) {
    trendsList.innerHTML = '<p style="color:#444;font-size:11px;padding:8px 0;">Trends unavailable</p>';
    return;
  }

  const maxScore = Math.max(...trends.map(t => t.score), 1);

  const html = trends.map((t, i) => {
    const barPct = Math.round((t.score / maxScore) * 100);
    const barClass = barPct >= 70 ? 'high' : barPct >= 40 ? 'medium' : 'low';
    return `
      <div class="trend-item">
        <span class="trend-rank">${i + 1}</span>
        <span class="trend-term">${t.term}</span>
        <div class="trend-bar-wrap">
          <div class="trend-bar ${barClass}" style="width:0%" data-target="${barPct}%"></div>
        </div>
        <span class="trend-score">${t.score}</span>
      </div>
    `;
  }).join('');

  trendsList.innerHTML = html;

  // Animate bars in after DOM insertion
  requestAnimationFrame(() => {
    trendsList.querySelectorAll('.trend-bar').forEach(bar => {
      bar.style.width = bar.dataset.target;
    });
  });

  // Macro vote
  updateMacroVote(trends);
}

function updateMacroVote(trends) {
  const voteEl = document.getElementById('macro-vote');
  if (!voteEl) return;

  const BULLISH = new Set(['stock market', 'S&P 500', 'earnings report']);
  const BEARISH = new Set(['recession', 'unemployment']);
  const scores = Object.fromEntries(trends.map(t => [t.term, t.score]));

  const bullAvg = [...BULLISH].reduce((s, t) => s + (scores[t] ?? 50), 0) / BULLISH.size;
  const bearAvg = [...BEARISH].reduce((s, t) => s + (scores[t] ?? 50), 0) / BEARISH.size;
  const diff = (bullAvg - bearAvg) / 100;

  let label, cls;
  if (diff > 0.15)       { label = 'Bullish';  cls = 'bullish'; }
  else if (diff < -0.15) { label = 'Bearish';  cls = 'bearish'; }
  else                   { label = 'Neutral';  cls = 'neutral'; }

  voteEl.textContent = label;
  voteEl.className = `macro-vote ${cls}`;
}

// ── Init ──────────────────────────────────────────────────────────────────────

(async function init() {
  // Load universe and trends in parallel
  await Promise.all([loadUniverse(), loadTrends()]);
})();
