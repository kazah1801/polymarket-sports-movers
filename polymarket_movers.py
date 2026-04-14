import asyncio
import csv
import fcntl
import json
import logging
import os
import re
import signal
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

import requests

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> None:
        return None


UTC = timezone.utc
DEFAULT_GAMMA_BASE = "https://gamma.polymarket.com"
CLOB_BASE_DEFAULT = "https://clob.polymarket.com"
GAMMA_BASE_FALLBACKS = (
    "https://gamma.polymarket.com",
    "https://gamma-api.polymarket.com",
)
DEFAULT_POLL_SEC = 30
REQUEST_TIMEOUT_SEC = 20
ALERT_CSV = "alerts.csv"
STATE_JSON = "state.json"

SPORT_ALLOWLIST = {
    "soccer",
    "football",
    "mlb",
    "baseball",
    "nba",
    "basketball",
    "nhl",
    "hockey",
}

LEAGUE_KEYWORDS = {
    "uefa champions league": "UCL",
    "champions league": "UCL",
    " ucl ": "UCL",
    "laliga": "LaLiga",
    "la liga": "LaLiga",
    "ligue 1": "Ligue 1",
    "bundesliga": "Bundesliga",
    "uefa europa league": "UEL",
    "europa league": "UEL",
    " uel ": "UEL",
    "premier league": "EPL",
    "epl": "EPL",
    "super lig": "Turkish League",
    "turkish super lig": "Turkish League",
    "turkish league": "Turkish League",
    "sea games": "SEA",
    "southeast asia": "SEA",
    "mlb": "MLB",
    "nba": "NBA",
    "nhl": "NHL",
}

EXCLUDED_MARKET_TERMS = (
    "total",
    "spread",
    "handicap",
    "prop",
    "btts",
    "both teams to score",
    "exact score",
    "advance",
    "qualify",
    "double chance",
    "draw no bet",
    "1st half",
    "first half",
    "quarter",
    "inning",
    "round",
    "method of victory",
)

PARTIAL_GAME_MARKET_PATTERNS = (
    r"(^|\W)1h(\W|$)",
    r"(^|\W)2h(\W|$)",
    r"(^|\W)1p(\W|$)",
    r"(^|\W)2p(\W|$)",
    r"(^|\W)3p(\W|$)",
    r"(^|\W)q1(\W|$)",
    r"(^|\W)q2(\W|$)",
    r"(^|\W)q3(\W|$)",
    r"(^|\W)q4(\W|$)",
    r"(^|\W)ot(\W|$)",
    r"(^|\W)first\s+quarter(\W|$)",
    r"(^|\W)second\s+quarter(\W|$)",
    r"(^|\W)third\s+quarter(\W|$)",
    r"(^|\W)fourth\s+quarter(\W|$)",
    r"(^|\W)1st\s+period(\W|$)",
    r"(^|\W)2nd\s+period(\W|$)",
    r"(^|\W)3rd\s+period(\W|$)",
    r"(^|\W)period\s+1(\W|$)",
    r"(^|\W)period\s+2(\W|$)",
    r"(^|\W)period\s+3(\W|$)",
    r"(^|\W)first\s+inning(\W|$)",
    r"(^|\W)second\s+inning(\W|$)",
    r"(^|\W)third\s+inning(\W|$)",
)

NON_MONEYLINE_OUTCOME_TERMS = (
    "over",
    "under",
    "yes",
    "no",
    "more",
    "less",
)

MONEYLINE_TITLE_HINTS = (
    "moneyline",
    "match winner",
    "winner",
    "to win",
    "win in regulation",
    "game winner",
)

SPORTS_EVENT_START_FIELDS = ("startTime", "eventStartTime", "eventDate")
SPORTS_MARKET_START_FIELDS = ("startTime", "eventStartTime", "eventDate")
DEBUG_DATE_FIELDS = ("startTime", "eventStartTime", "eventDate", "startDate")
SPORTS_METADATA_SPORTS = {
    "football": ("soccer", "football"),
    "mlb": ("mlb", "baseball"),
    "nba": ("nba", "basketball"),
    "nhl": ("nhl", "hockey"),
}
TARGET_LEAGUE_TAGS = {
    "UCL": ("uefa champions league", "champions league", "ucl"),
    "LaLiga": ("laliga", "la liga"),
    "Ligue 1": ("ligue 1",),
    "Bundesliga": ("bundesliga",),
    "UEL": ("uefa europa league", "europa league", "uel"),
    "EPL": ("premier league", "epl"),
    "Turkish League": ("super lig", "turkish super lig", "turkish league"),
    "SEA": ("sea games", "southeast asia"),
    "MLB": ("mlb", "major league baseball"),
    "NBA": ("nba",),
    "NHL": ("nhl",),
}
ALLOWED_SPORTS = {"football", "mlb", "nba", "nhl"}
ALLOWED_FOOTBALL_LEAGUES = {"UCL", "LaLiga", "Ligue 1", "Bundesliga", "UEL", "EPL", "Turkish League", "SEA"}
ALLOWED_LEAGUES_BY_SPORT = {
    "football": ALLOWED_FOOTBALL_LEAGUES,
    "mlb": {"MLB"},
    "nba": {"NBA"},
    "nhl": {"NHL"},
}
SPORT_SUMMARY_ORDER = ("football", "mlb", "nba", "nhl")
UNWANTED_SPORT_TERMS = (
    "cricket",
    "ipl",
    "tennis",
    "esports",
    "lol",
    "league of legends",
    "cblol",
    "lec",
    "emea masters",
    "valorant",
    "dota",
    "counter-strike",
    "cs2",
    "csgo",
)
PROP_MARKET_TERMS = (
    "toss",
    "who wins the toss",
    "prop",
    "player prop",
    "player to",
    "shots on target",
    "rebounds",
    "assists",
    "points ",
    "spread",
    "handicap",
    "total",
    "totals",
    "over/under",
    "over under",
    "map ",
    " round",
    " set",
    " inning",
    " quarter",
    " half",
    "exact score",
    "scorecast",
)
NON_MATCH_TERMS = (
    "winner of map",
    "map winner",
    "round winner",
    "set winner",
    "inning winner",
    "quarter winner",
    "half winner",
)
FUTURES_EVENT_TERMS = (
    "futures",
    "season long",
    "season-long",
    "regular season",
    "season winner",
    "to make playoffs",
    "playoff seed",
    "division winner",
    "conference winner",
    "series winner",
    "league winner",
    "tournament winner",
    "to qualify",
    "qualify for",
    "qualification",
    "relegation",
    "draft",
    "next fight",
    "who will fight next",
    "end of year",
    "end-of-year",
    "award",
    "mvp",
    "cy young",
    "player of the year",
    "golden boot",
    "win totals",
)
SAFE_COMPETITION_PHRASES = (
    "champions league",
    "uefa champions league",
    "europa league",
    "uefa europa league",
)
MAX_SPREAD_PP = 15.0
HIGH_PROB_THRESHOLD = 0.90
NORMALIZATION_EPSILON = 0.02


@dataclass
class Config:
    tg_token: str
    tg_chat_id: str
    lookback_minutes: int
    min_move_pp: float
    horizon_days: int
    cooldown_minutes: int
    refresh_universe_sec: int
    snapshot_dir: Path
    optional_min_liquidity: Optional[float]
    optional_min_volume_24h: Optional[float]
    optional_league_allowlist: List[str]
    dry_run: bool
    log_accepted_events: bool
    log_accepted_markets: bool
    log_rejected_events: bool
    log_rejected_events_sample: int
    log_rejected_markets: bool
    log_rejected_markets_sample: int
    log_universe_debug: bool
    log_sports_fetch_details: bool
    log_poll_cycle_details: bool
    log_price_samples_count: int
    log_empty_movers: bool


@dataclass
class CandidateEvent:
    sport: str
    league: str
    event_title: str
    start_time: str
    live: bool
    ended: bool
    markets_count: int
    source_tag: str


@dataclass
class OutcomeRef:
    key: str
    market_id: str
    asset_id: str
    outcome_name: str
    sport: str
    league: str
    event_title: str
    market_title: str
    event_url: str
    event_slug: str
    market_slug: str
    start_time: str
    event_live: bool
    event_ended: bool
    source_tag: str


@dataclass
class PriceQuote:
    bid: float
    ask: float
    midpoint: float
    spread_pp: float
    source: str = "orderbook"


@dataclass
class MarketFilterHit:
    market_id: str
    sport: str
    league: str
    event_title: str
    market_title: str
    offending_outcome: str
    offending_price: float
    slug: str
    outcomes_summary: str
    prices_summary: str


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, dry_run: bool) -> None:
        self.token = token
        self.chat_id = chat_id
        self.dry_run = dry_run
        self.session = requests.Session()

    def _send_request(self, text: str) -> Tuple[bool, str]:
        if self.dry_run or not self.token or not self.chat_id:
            print(text, flush=True)
            return True, "dry_run"

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True}
        try:
            response = self.session.post(url, json=payload, timeout=REQUEST_TIMEOUT_SEC)
            response.raise_for_status()
            return True, f"status={response.status_code}"
        except requests.RequestException as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            body = ""
            response = getattr(exc, "response", None)
            if response is not None:
                try:
                    body = response.text[:500]
                except Exception:
                    body = "<unavailable>"
            reason = f"status={status_code} error={exc}"
            if body:
                reason += f" body={body}"
            return False, reason

    def send(self, text: str) -> None:
        success, reason = self._send_request(text)
        if success:
            return
        logging.warning("Не удалось отправить Telegram alert: %s", reason)
        print(text, flush=True)

    def startup_test(self) -> None:
        if self.dry_run or not self.token or not self.chat_id:
            logging.info("Telegram startup self-test skipped: dry_run=%s | has_token=%s | has_chat_id=%s", self.dry_run, bool(self.token), bool(self.chat_id))
            return
        success, reason = self._send_request("polymarket-movers startup test")
        if success:
            logging.info("Telegram startup self-test: success | %s", reason)
            return
        logging.warning("Telegram startup self-test: failed | %s", reason)


class PolymarketSportsMovers:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "polymarket-sports-movers/1.0"})
        configured_gamma_base = os.getenv("GAMMA_BASE", DEFAULT_GAMMA_BASE).strip()
        self.gamma_base = configured_gamma_base or DEFAULT_GAMMA_BASE
        configured_clob_base = os.getenv("CLOB_BASE", CLOB_BASE_DEFAULT).strip()
        self.clob_base = configured_clob_base or CLOB_BASE_DEFAULT
        self.notifier = TelegramNotifier(config.tg_token, config.tg_chat_id, config.dry_run)
        self.outcomes: Dict[str, OutcomeRef] = {}
        self.price_history: Dict[str, Deque[Tuple[float, float]]] = defaultdict(deque)
        self.cooldowns: Dict[str, float] = {}
        self.last_universe_refresh = 0.0
        self.next_universe_refresh_monotonic = 0.0
        self.universe_initialized = False
        self.stop_event = asyncio.Event()
        self.snapshot_dir = config.snapshot_dir
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.alerts_csv_path = self.snapshot_dir / ALERT_CSV
        self.state_json_path = self.snapshot_dir / STATE_JSON
        self.instance_lock_path = self.snapshot_dir / ".polymarket_movers.lock"
        self.instance_lock_handle = None
        self.last_tag_sources: Dict[str, str] = {}
        self.batch_price_fetch_disabled = False
        self.rejected_event_log_count = 0
        self.rejected_event_log_samples: List[Dict[str, Any]] = []
        self.rejected_market_log_count = 0
        self.rejected_market_log_samples: List[Dict[str, Any]] = []

    def run(self) -> None:
        self._acquire_instance_lock()
        self._load_state()
        self._install_signal_handlers()
        logging.info(
            "Startup config: pid=%s | horizon_days=%s | lookback_minutes=%s | min_move_pp=%s | cooldown_minutes=%s | dry_run=%s | has_tg_token=%s | has_tg_chat_id=%s | snapshot_dir=%s",
            os.getpid(),
            self.config.horizon_days,
            self.config.lookback_minutes,
            self.config.min_move_pp,
            self.config.cooldown_minutes,
            self.config.dry_run,
            bool(self.config.tg_token),
            bool(self.config.tg_chat_id),
            self.snapshot_dir,
        )
        try:
            self.notifier.startup_test()
            asyncio.run(self._run_loop())
        finally:
            self._release_instance_lock()

    async def _run_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                if (not self.universe_initialized) or (
                    time.monotonic() >= self.next_universe_refresh_monotonic
                ):
                    self.refresh_universe()
                self.poll_best_asks()
                self._prune_history()
                self._save_state()
            except Exception as exc:
                logging.exception("Ошибка цикла мониторинга: %s", exc)
            await asyncio.sleep(DEFAULT_POLL_SEC)

    def _install_signal_handlers(self) -> None:
        def handle_stop(signum: int, _frame: Any) -> None:
            logging.info("Получен сигнал %s, завершаю работу", signum)
            self.stop_event.set()

        signal.signal(signal.SIGINT, handle_stop)
        signal.signal(signal.SIGTERM, handle_stop)

    def refresh_universe(self) -> None:
        logging.info("Universe refresh started")
        self.rejected_event_log_count = 0
        self.rejected_event_log_samples = []
        self.rejected_market_log_count = 0
        self.rejected_market_log_samples = []
        now = datetime.now(tz=UTC)
        horizon = now + timedelta(days=self.config.horizon_days)
        events, tag_sources, raw_sports_tagged_events, source_counts = self._fetch_sports_universe()
        self.last_tag_sources = tag_sources
        new_outcomes: Dict[str, OutcomeRef] = {}
        raw_market_count = 0
        date_window_count = 0
        active_count = 0
        events_with_markets_count = 0
        events_with_candidate_market_count = 0
        moneyline_count = 0
        candidate_events: List[CandidateEvent] = []
        filter_reason_counts: Dict[str, int] = defaultdict(int)
        market_reject_reason_counts: Dict[str, int] = defaultdict(int)
        lost_events: List[Dict[str, Any]] = []
        sport_counts: Dict[str, int] = defaultdict(int)
        league_counts: Dict[str, int] = defaultdict(int)

        if self.config.log_universe_debug:
            self._log_raw_event_dates(events, now, "Sports-tagged")

        for event in events:
            markets = event.get("markets") or []
            raw_market_count += len(markets)
            event_start, event_start_source = self._choose_event_start(event, markets)
            sport = self._normalize_sport(event, markets)
            league = self._normalize_league(event, markets)
            event_sources = self._prune_event_sources(event, sport, league)
            if not event_sources:
                filter_reason_counts["rejected_source_mismatch"] += 1
                self._record_lost_event(lost_events, event, sport, event_start, markets, "rejected_source_mismatch")
                self._log_event_decision("REJECTED EVENT", event, sport, league, event_start, event_start_source, "rejected_source_mismatch", 0, 0)
                continue
            for source in event_sources:
                source_counts[source]["deduped"] += 1

            if markets:
                events_with_markets_count += 1
                for source in event_sources:
                    source_counts[source]["with_markets"] += 1

            event_haystack = self._event_haystack(event, markets)
            event_reject_reason = self._event_universe_reject_reason(sport, league, event_haystack)
            if event_reject_reason:
                filter_reason_counts[event_reject_reason] += 1
                self._record_lost_event(lost_events, event, sport, event_start, markets, event_reject_reason)
                self._log_event_decision("REJECTED EVENT", event, sport, league, event_start, event_start_source, event_reject_reason, 0, 0)
                continue

            candidate_events.append(
                CandidateEvent(
                    sport=sport,
                    league=league,
                    event_title=str(event.get("title") or event.get("question") or ""),
                    start_time=event_start.isoformat() if event_start else "-",
                    live=bool(event.get("live")),
                    ended=bool(event.get("ended")),
                    markets_count=len(markets),
                    source_tag=self._event_source_label(event),
                )
            )

            activity_filter_reason = self._event_activity_filter_reason(event, event_start, now)
            if activity_filter_reason:
                filter_reason_counts[activity_filter_reason] += 1
                self._record_lost_event(lost_events, event, sport, event_start, markets, activity_filter_reason)
                self._log_event_decision("REJECTED EVENT", event, sport, league, event_start, event_start_source, activity_filter_reason, 0, 0)
                continue
            active_count += 1
            for source in event_sources:
                source_counts[source]["active"] += 1

            event_has_moneyline = False
            event_in_date_window = False
            event_candidate_market_count = 0
            event_market_reasons: Dict[str, int] = defaultdict(int)
            for market in markets:
                market_start, market_start_source = self._choose_market_start(event, market)
                market_date_filter_reason = self._event_date_filter_reason(market_start, now, horizon)
                if market_date_filter_reason:
                    market_reject_reason_counts[market_date_filter_reason] += 1
                    event_market_reasons[market_date_filter_reason] += 1
                    self._log_market_decision("REJECTED MARKET", event, market, sport, league, market_start, market_start_source, market_date_filter_reason)
                    continue
                event_in_date_window = True

                market_reject_reason = self._primary_moneyline_reject_reason(event, market, sport)
                if market_reject_reason:
                    market_reject_reason_counts[market_reject_reason] += 1
                    event_market_reasons[market_reject_reason] += 1
                    self._log_market_decision("REJECTED MARKET", event, market, sport, league, market_start, market_start_source, market_reject_reason)
                    continue
                if self._market_fails_optional_filters(market):
                    market_reject_reason_counts["optional_filters"] += 1
                    event_market_reasons["optional_filters"] += 1
                    self._log_market_decision("REJECTED MARKET", event, market, sport, league, market_start, market_start_source, "optional_filters")
                    continue
                event_candidate_market_count += 1
                event_has_moneyline = True
                self._log_market_decision("ACCEPTED MARKET", event, market, sport, league, market_start, market_start_source, "moneyline")
                new_outcomes.update(self._extract_outcomes(event, market, sport, league, market_start))

            if not event_in_date_window:
                top_reason = self._top_reason(
                    {
                        reason: count
                        for reason, count in event_market_reasons.items()
                        if reason in {"no_start_time", "started_already", "beyond_horizon"}
                    }
                ) or "no_start_time"
                filter_reason_counts[top_reason] += 1
                self._record_lost_event(lost_events, event, sport, event_start, markets, top_reason)
                self._log_event_decision("REJECTED EVENT", event, sport, league, event_start, event_start_source, top_reason, 0, 0)
                continue

            date_window_count += 1
            for source in event_sources:
                source_counts[source]["date_window"] += 1

            if event_candidate_market_count > 0:
                events_with_candidate_market_count += 1
                for source in event_sources:
                    source_counts[source]["candidate_markets"] += 1
            else:
                top_reason = self._top_reason(event_market_reasons) or "no_markets"
                self._record_lost_event(lost_events, event, sport, event_start, markets, f"no_candidate_market:{top_reason}")
                self._log_event_decision("REJECTED EVENT", event, sport, league, event_start, event_start_source, f"no_candidate_market:{top_reason}", len(markets), 0)

            if event_has_moneyline:
                moneyline_count += 1
                sport_counts[sport] += 1
                league_counts[league] += 1
                for source in event_sources:
                    source_counts[source]["moneyline"] += 1
                self._log_event_decision("ACCEPTED EVENT", event, sport, league, event_start, event_start_source, "moneyline", len(markets), event_candidate_market_count)

        removed = set(self.outcomes) - set(new_outcomes)
        for key in removed:
            self.price_history.pop(key, None)
            self.cooldowns.pop(key, None)

        self.outcomes = new_outcomes
        self.last_universe_refresh = time.time()
        self.next_universe_refresh_monotonic = time.monotonic() + self.config.refresh_universe_sec
        self.universe_initialized = True
        self._log_refresh_debug(
            raw_events=raw_sports_tagged_events,
            deduped_events=len(events),
            raw_markets=raw_market_count,
            date_window_events=date_window_count,
            active_events=active_count,
            events_with_markets=events_with_markets_count,
            events_with_candidate_markets=events_with_candidate_market_count,
            moneyline_events=moneyline_count,
            tracked_outcomes=len(self.outcomes),
        )
        self._log_filter_reason_counts(filter_reason_counts)
        self._log_market_reject_reason_counts(market_reject_reason_counts)
        self._log_rejected_market_samples()
        self._log_universe_summary(sport_counts, league_counts)
        self._log_source_pipeline(source_counts)
        self._log_rejected_event_samples()
        if self.config.log_universe_debug:
            self._log_lost_events(lost_events)
        if self.config.dry_run and self.config.log_universe_debug:
            self._log_candidate_events(candidate_events)
        self._log_tag_sources()
        logging.info("Universe refreshed: %s outcomes", len(self.outcomes))
        if self.config.log_universe_debug:
            self._log_tracked_outcomes()
            self._log_tracked_market_summary()
        logging.info(
            "Price polling ready: tracked asset ids count=%s",
            len({item.asset_id for item in self.outcomes.values() if item.asset_id}),
        )

    def poll_best_asks(self) -> None:
        if self.config.log_poll_cycle_details:
            logging.info("Price polling started")
        if not self.outcomes:
            logging.info("Нет подходящих outcomes для мониторинга")
            return

        asset_ids = [item.asset_id for item in self.outcomes.values() if item.asset_id]
        unique_asset_ids = list(dict.fromkeys(asset_ids))
        if self.config.log_poll_cycle_details:
            logging.info("Tracked asset ids count: %s", len(unique_asset_ids))
        if not unique_asset_ids:
            logging.info("Price polling skipped: нет валидных asset ids")
            return
        prices, missing_asset_ids, price_reject_counts = self._fetch_best_asks(unique_asset_ids)
        logging.info("Prices fetched count: %s", len(prices))
        logging.info("Missing prices count: %s", len(missing_asset_ids))
        if missing_asset_ids:
            logging.info("Missing price asset ids sample: %s", ", ".join(missing_asset_ids[:5]))
        filtered_market_ids, filtered_market_hits = self._filter_market_ids_by_high_prob(prices)
        self._log_filtered_markets_gt_90(filtered_market_hits)
        now_ts = time.time()
        scanned_at = datetime.now(tz=UTC).isoformat()
        alerts_triggered = 0
        outcomes_with_price = 0
        alert_checks_count = 0
        price_samples: List[Tuple[OutcomeRef, PriceQuote, Optional[float], Optional[float], Optional[float]]] = []
        movers: List[Tuple[float, OutcomeRef, float, Optional[float]]] = []
        monitored_refs: List[OutcomeRef] = []

        for key, ref in self.outcomes.items():
            quote = prices.get(ref.asset_id)
            if quote is None:
                continue

            outcomes_with_price += 1
            current_price = self._monitored_price(quote)
            if ref.market_id in filtered_market_ids:
                continue

            monitored_refs.append(ref)
            history = self.price_history[key]
            history.append((now_ts, current_price))
            self._trim_history(history, now_ts)
            old_price, lookback_age_sec = self._price_at_lookback(history, now_ts)
            move_pp_value = abs(current_price - old_price) * 100.0 if old_price is not None else None
            if len(price_samples) < 10:
                price_samples.append((ref, quote, old_price, move_pp_value, lookback_age_sec))
            if move_pp_value is not None:
                movers.append((move_pp_value, ref, current_price, old_price))
            if old_price is None:
                continue

            alert_checks_count += 1
            move_pp = move_pp_value
            if move_pp is None:
                continue
            if move_pp < self.config.min_move_pp:
                continue
            if not self._cooldown_expired(key, now_ts):
                continue

            direction = "UP" if current_price > old_price else "DOWN"
            self.cooldowns[key] = now_ts
            self._write_alert_csv(scanned_at, ref, old_price, current_price, move_pp, direction)
            self.notifier.send(self._format_alert(ref, old_price, current_price, move_pp, direction))
            alerts_triggered += 1

        history_keys = sum(1 for history in self.price_history.values() if history)
        history_points = sum(len(history) for history in self.price_history.values())
        lookback_ready_count = 0
        lookback_missing_count = 0
        for key in self.outcomes:
            prev_price, _lookback_age_sec = self._price_at_lookback(self.price_history[key], now_ts)
            if prev_price is not None:
                lookback_ready_count += 1
            else:
                lookback_missing_count += 1
        if self.config.log_poll_cycle_details:
            logging.info("Outcomes with price count: %s", outcomes_with_price)
        self._log_market_reject_reason_counts(price_reject_counts)
        self._log_monitoring_outcomes(monitored_refs, filtered_market_hits)
        self._log_price_samples(price_samples)
        self._log_top_movers(movers)
        if self.config.log_poll_cycle_details:
            logging.info(
                "Lookback readiness: lookback_ready_count=%s | lookback_missing_count=%s | history_points_total=%s | history_cache_size=%s",
                lookback_ready_count,
                lookback_missing_count,
                history_points,
                history_keys,
            )
            logging.info("History points count: %s | cache size: %s", history_points, history_keys)
            logging.info("Alert checks count: %s", alert_checks_count)
        logging.info("Alerts triggered count: %s", alerts_triggered)

    def _fetch_sports_universe(self) -> Tuple[List[Dict[str, Any]], Dict[str, str], int, Dict[str, Dict[str, int]]]:
        sports_metadata: List[Dict[str, Any]] = []
        try:
            sports_metadata = self._fetch_sports_metadata()
        except requests.RequestException as exc:
            logging.warning("Не удалось получить sports metadata: %s", exc)
        except ValueError as exc:
            logging.warning("Не удалось разобрать sports metadata: %s", exc)

        tags = self._fetch_all_tags()
        tag_sources = self._resolve_target_tag_ids(sports_metadata, tags)
        if not tag_sources:
            raise ValueError("Не удалось определить sports tag_id для целевых лиг")

        raw_events: List[Dict[str, Any]] = []
        deduped_events: Dict[str, Dict[str, Any]] = {}
        source_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for tag_id, tag_source in tag_sources.items():
            tag_events = self._fetch_events_for_tag(tag_id)
            source_counts[tag_source]["raw"] += len(tag_events)
            mismatch_before = source_counts[tag_source].get("source_mismatch", 0)
            matched_before = source_counts[tag_source].get("source_matched", 0)
            filtered_tag_events = self._filter_events_for_source(tag_events, tag_source, source_counts[tag_source])
            mismatch_delta = source_counts[tag_source].get("source_mismatch", 0) - mismatch_before
            matched_delta = source_counts[tag_source].get("source_matched", 0) - matched_before
            logging.info(
                "Sports universe fetch: tag_id=%s | source=%s | raw=%s | source_matched=%s | source_mismatch=%s",
                tag_id,
                tag_source,
                len(tag_events),
                matched_delta,
                mismatch_delta,
            )
            raw_events.extend(filtered_tag_events)
            for event in filtered_tag_events:
                event_key = str(event.get("id") or event.get("slug") or event.get("title") or "")
                if event_key:
                    existing = deduped_events.get(event_key)
                    if existing is None:
                        event_copy = dict(event)
                        event_copy["_source_tags"] = [tag_source]
                        deduped_events[event_key] = event_copy
                    else:
                        merged_sources = list(dict.fromkeys([*existing.get("_source_tags", []), tag_source]))
                        existing["_source_tags"] = merged_sources
                        if len(existing.get("markets") or []) < len(event.get("markets") or []):
                            for key, value in event.items():
                                if key != "_source_tags":
                                    existing[key] = value
                            existing["_source_tags"] = merged_sources

        if self.config.log_sports_fetch_details:
            logging.info("Sports universe fetch: broad source events:all disabled; using only resolved sports/league tags")
        return list(deduped_events.values()), tag_sources, len(raw_events), source_counts

    def _fetch_sports_metadata(self) -> List[Dict[str, Any]]:
        response = self._gamma_get("/sports")
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            sports = data.get("sports")
            if isinstance(sports, list):
                return sports
        raise ValueError("Неожиданный ответ sports API")

    def _fetch_all_tags(self) -> List[Dict[str, Any]]:
        limit = 500
        offset = 0
        tags: List[Dict[str, Any]] = []
        while True:
            response = self._gamma_get(
                "/tags",
                params={"limit": limit, "offset": offset},
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                page = data
            elif isinstance(data, dict) and isinstance(data.get("tags"), list):
                page = data["tags"]
            else:
                raise ValueError("Неожиданный ответ tags API")
            tags.extend(page)
            if len(page) < limit:
                break
            offset += limit
        return tags

    def _fetch_events_for_tag(self, tag_id: Optional[str]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        limit = 100
        offset = 0
        while True:
            params = {
                "active": "true",
                "closed": "false",
                "order": "start_date",
                "ascending": "true",
                "limit": limit,
                "offset": offset,
            }
            if tag_id is not None:
                params["tag_id"] = tag_id
            response = self._gamma_get(
                "/events",
                params=params,
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                page = data
            elif isinstance(data, dict) and isinstance(data.get("events"), list):
                page = data["events"]
            else:
                raise ValueError("Неожиданный ответ events API")
            if not page:
                break
            events.extend(page)
            if len(page) < limit:
                break
            offset += limit
        return events

    def _resolve_target_tag_ids(
        self,
        sports_metadata: List[Dict[str, Any]],
        tags: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        resolved: Dict[str, str] = {}

        for item in sports_metadata:
            sport_name = str(item.get("sport") or "").strip().lower()
            if not sport_name:
                continue
            for normalized_sport, aliases in SPORTS_METADATA_SPORTS.items():
                if sport_name not in aliases:
                    continue
                if normalized_sport == "football":
                    break
                for tag_id in self._csv_list(item.get("tags")):
                    resolved[tag_id] = f"sports:{sport_name}"
                break

        for tag in tags:
            tag_id = str(tag.get("id") or "").strip()
            label = str(tag.get("label") or "").strip()
            slug = str(tag.get("slug") or "").strip()
            haystack = f" {label} {slug} ".lower()
            if not tag_id or not haystack.strip():
                continue

            if " all sports " in haystack:
                continue

            for sport_name, keywords in SPORTS_METADATA_SPORTS.items():
                if sport_name == "football":
                    continue
                sports_prefixes = tuple(f" sports:{keyword}" for keyword in keywords) + tuple(
                    f" sports-{keyword}" for keyword in keywords
                )
                if any(prefix in haystack for prefix in sports_prefixes):
                    resolved[tag_id] = f"sports:{sport_name}"
                    break
            if tag_id in resolved:
                continue

            if " sport " in haystack:
                continue
            for target_name, keywords in TARGET_LEAGUE_TAGS.items():
                if any(keyword in haystack for keyword in keywords):
                    resolved[tag_id] = f"tags:{target_name}"
                    break

        return dict(sorted(resolved.items(), key=lambda item: item[1]))

    def _filter_events_for_source(
        self,
        events: List[Dict[str, Any]],
        source: str,
        source_stats: Dict[str, int],
    ) -> List[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        for event in events:
            markets = event.get("markets") or []
            sport = self._normalize_sport(event, markets)
            league = self._normalize_league(event, markets)
            if not self._source_matches_event(source, sport, league):
                source_stats["source_mismatch"] += 1
                continue
            source_stats["source_matched"] += 1
            event_copy = dict(event)
            event_copy["_source_tags"] = [source]
            filtered.append(event_copy)
        return filtered

    def _source_matches_event(self, source: str, sport: str, league: str) -> bool:
        if not source:
            return False
        if source.startswith("sports:"):
            expected_sport = source.split(":", 1)[1].strip().lower()
            return bool(sport) and sport == expected_sport
        if source.startswith("tags:"):
            expected_league = source.split(":", 1)[1].strip()
            expected_sport = self._infer_sport_from_league(expected_league)
            if expected_sport and sport != expected_sport:
                return False
            return bool(league) and league == expected_league
        return False

    def _prune_event_sources(self, event: Dict[str, Any], sport: str, league: str) -> List[str]:
        compatible_sources = [
            source
            for source in self._event_sources(event)
            if self._source_matches_event(source, sport, league)
        ]
        event["_source_tags"] = compatible_sources
        return compatible_sources

    def _fetch_best_asks(self, asset_ids: Iterable[str]) -> Tuple[Dict[str, PriceQuote], List[str], Dict[str, int]]:
        asset_list = [asset_id for asset_id in asset_ids if asset_id]
        if not asset_list:
            return {}, [], {}

        if self.config.log_poll_cycle_details:
            logging.info("Batch price fetch count: %s", len(asset_list))
        result: Dict[str, PriceQuote] = {}
        reject_counts: Dict[str, int] = defaultdict(int)
        for asset_id in asset_list:
            quote, reject_reason = self._fetch_price_quote_from_book(asset_id)
            if quote is not None:
                result[asset_id] = quote
            elif reject_reason:
                reject_counts[reject_reason] += 1
        missing_asset_ids = [asset_id for asset_id in asset_list if asset_id not in result]
        return result, missing_asset_ids, dict(reject_counts)

    def _gamma_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        last_error: Optional[requests.RequestException] = None
        bases: List[str] = []
        for base in (self.gamma_base, *GAMMA_BASE_FALLBACKS):
            normalized = base.strip().rstrip("/")
            if normalized and normalized not in bases:
                bases.append(normalized)

        for base in bases:
            try:
                response = self.session.get(
                    f"{base}{path}",
                    params=params,
                    timeout=REQUEST_TIMEOUT_SEC,
                )
                if base != self.gamma_base:
                    logging.info("Переключаю gamma base на %s", base)
                    self.gamma_base = base
                return response
            except requests.RequestException as exc:
                last_error = exc
                logging.warning("Не удалось получить %s через %s: %s", path, base, exc)

        if last_error is None:
            raise RuntimeError(f"Не удалось выполнить gamma GET {path}")
        raise last_error

    def _fetch_price_quote_from_book(self, asset_id: str) -> Tuple[Optional[PriceQuote], Optional[str]]:
        endpoints = (
            ("/book", {"token_id": asset_id}),
            ("/book", {"asset_id": asset_id}),
            ("/orderbook", {"token_id": asset_id}),
            ("/orderbook", {"asset_id": asset_id}),
        )
        last_error: Optional[requests.RequestException] = None
        last_empty_reason: Optional[str] = None
        for path, params in endpoints:
            try:
                response = self.session.get(
                    f"{self.clob_base}{path}",
                    params=params,
                    timeout=REQUEST_TIMEOUT_SEC,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                last_error = exc
                logging.info("Orderbook request failed: path=%s | asset_id=%s | error=%s", path, asset_id, exc)
                continue

            payload = response.json()
            if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                payload = payload["data"]

            asks = payload.get("asks") if isinstance(payload, dict) else None
            bids = payload.get("bids") if isinstance(payload, dict) else None
            if not isinstance(asks, list) or not asks:
                asks_count = len(asks) if isinstance(asks, list) else 0
                bids_count = len(bids) if isinstance(bids, list) else 0
                last_empty_reason = f"path={path} bids={bids_count} asks={asks_count}"
                logging.info(
                    "Orderbook missing asks: path=%s | asset_id=%s | bids=%s | asks=%s",
                    path,
                    asset_id,
                    bids_count,
                    asks_count,
                )
                continue

            best_ask = self._best_orderbook_ask(asks)
            if best_ask is None or best_ask <= 0 or best_ask > 1:
                last_empty_reason = f"path={path} invalid_best_ask"
                continue

            best_bid: Optional[float] = None
            if isinstance(bids, list) and bids:
                best_bid = self._best_orderbook_bid(bids)
                if best_bid is not None and (best_bid <= 0 or best_bid > 1):
                    best_bid = None
                if best_bid is not None and best_bid > best_ask:
                    logging.info(
                        "Orderbook crossed book ignored for bid side: asset_id=%s | best_bid=%.4f | best_ask=%.4f",
                        asset_id,
                        best_bid,
                        best_ask,
                    )
                    best_bid = None

            spread_pp = (best_ask - best_bid) * 100.0 if best_bid is not None else 0.0
            source = f"{path}:best_ask(best_ask={best_ask:.4f}"
            if best_bid is not None:
                source += f",best_bid={best_bid:.4f},spread_pp={spread_pp:.2f})"
            else:
                source += ",best_bid=missing,spread_pp=unknown)"

            return PriceQuote(
                bid=best_bid if best_bid is not None else 0.0,
                ask=best_ask,
                midpoint=best_ask,
                spread_pp=spread_pp,
                source=source,
            ), None

        if last_error is not None:
            logging.warning("Не удалось получить orderbook для %s: %s", asset_id, last_error)
            return None, "rejected_bad_price_source"
        if last_empty_reason is not None:
            logging.info("Price quote not found for %s: %s", asset_id, last_empty_reason)
        return None, "rejected_bad_price_source"

    def _extract_outcomes(
        self,
        event: Dict[str, Any],
        market: Dict[str, Any],
        sport: str,
        league: str,
        event_start: datetime,
    ) -> Dict[str, OutcomeRef]:
        token_ids = self._json_list(market.get("clobTokenIds"))
        outcomes = self._json_list(market.get("outcomes"))
        refs: Dict[str, OutcomeRef] = {}

        if not token_ids or not outcomes or len(token_ids) != len(outcomes):
            return refs

        event_slug = event.get("slug") or event.get("id") or ""
        event_url = f"https://polymarket.com/event/{event_slug}" if event_slug else "https://polymarket.com"
        market_slug = str(market.get("slug") or market.get("marketSlug") or "")
        source_tag = self._event_source_label(event)
        market_id = str(market.get("id") or market.get("conditionId") or market.get("question") or market.get("title") or "")

        for token_id, outcome_name in zip(token_ids, outcomes):
            outcome_name_str = str(outcome_name).strip()
            if not self._is_supported_outcome(outcome_name_str, sport):
                continue
            key = f"{market_id}::{outcome_name_str}"
            refs[key] = OutcomeRef(
                key=key,
                market_id=market_id,
                asset_id=str(token_id),
                outcome_name=outcome_name_str,
                sport=sport,
                league=league,
                event_title=str(event.get("title") or event.get("question") or ""),
                market_title=str(market.get("question") or market.get("title") or ""),
                event_url=event_url,
                event_slug=str(event_slug),
                market_slug=market_slug,
                start_time=event_start.isoformat(),
                event_live=bool(event.get("live")),
                event_ended=bool(event.get("ended")),
                source_tag=source_tag,
            )
        return refs

    def _choose_event_start(
        self,
        event: Dict[str, Any],
        markets: List[Dict[str, Any]],
    ) -> Tuple[Optional[datetime], str]:
        for field in SPORTS_EVENT_START_FIELDS:
            parsed = self._parse_datetime(event.get(field))
            if parsed is not None:
                return parsed, f"event.{field}"

        for field in SPORTS_MARKET_START_FIELDS:
            market_candidates: List[Tuple[datetime, str]] = []
            for market in markets:
                parsed = self._parse_datetime(market.get(field))
                if parsed is not None:
                    market_candidates.append((parsed, f"market.{field}"))
            if market_candidates:
                market_candidates.sort(key=lambda item: item[0])
                return market_candidates[0]

        return None, "none"

    def _choose_market_start(
        self,
        event: Dict[str, Any],
        market: Dict[str, Any],
    ) -> Tuple[Optional[datetime], str]:
        for field in SPORTS_MARKET_START_FIELDS:
            parsed = self._parse_datetime(market.get(field))
            if parsed is not None:
                return parsed, f"market.{field}"
        for field in SPORTS_EVENT_START_FIELDS:
            parsed = self._parse_datetime(event.get(field))
            if parsed is not None:
                return parsed, f"event.{field}"
        return None, "none"

    def _contains_partial_game_marker(self, text: str) -> bool:
        normalized = str(text or "").lower()
        if not normalized:
            return False
        return any(re.search(pattern, normalized) for pattern in PARTIAL_GAME_MARKET_PATTERNS)

    def _primary_moneyline_reject_reason(
        self,
        event: Dict[str, Any],
        market: Dict[str, Any],
        sport: str,
    ) -> Optional[str]:
        title = " ".join(
            [
                str(event.get("title") or ""),
                str(event.get("question") or ""),
                str(market.get("question") or ""),
                str(market.get("title") or ""),
            ]
        ).lower()

        if not title:
            return "empty_title"
        if any(term in title for term in UNWANTED_SPORT_TERMS):
            if any(term in title for term in ("cricket", "ipl")):
                return "rejected_cricket"
            if any(term in title for term in ("lol", "league of legends", "cblol", "lec", "emea masters", "valorant", "dota", "counter-strike", "cs2", "csgo", "esports")):
                return "rejected_esports"
            return "rejected_wrong_sport"
        if any(term in title for term in PROP_MARKET_TERMS):
            return "non_moneyline"
        if self._looks_like_future_market_text(title):
            return "not_game_market"
        if any(term in title for term in NON_MATCH_TERMS):
            return "non_moneyline"
        if any(term in title for term in EXCLUDED_MARKET_TERMS):
            return "non_moneyline"
        if self._contains_partial_game_marker(title):
            return "non_moneyline"
        if any(flag for flag in (market.get("closed"), market.get("archived"), market.get("inactive"))):
            return "market_inactive"

        raw_outcomes = [str(item).strip() for item in self._json_list(market.get("outcomes")) if str(item).strip()]
        if len(raw_outcomes) not in {2, 3}:
            return "unsupported_outcome_count"

        normalized_outcomes = {self._normalize_market_outcome_name(item) for item in raw_outcomes if item.strip()}
        if not normalized_outcomes:
            return "unsupported_outcome_count"
        if any(outcome in NON_MONEYLINE_OUTCOME_TERMS for outcome in normalized_outcomes):
            return "non_moneyline"

        has_draw = any(item in {"draw", "tie", "x"} for item in normalized_outcomes)
        has_home_away = {"home", "away"}.issubset(normalized_outcomes)
        has_matchup_hint = any(
            hint in title
            for hint in (
                *MONEYLINE_TITLE_HINTS,
                " vs ",
                " vs. ",
                " v ",
                " v. ",
                "@",
            )
        )
        has_named_competitors = all(
            outcome not in {"draw", "tie", "x", "home", "away"}
            for outcome in normalized_outcomes
        )

        if len(raw_outcomes) == 2:
            if has_home_away and has_matchup_hint:
                return None
            if has_named_competitors and has_matchup_hint:
                return None
            return "non_moneyline"

        if sport == "football" and len(raw_outcomes) == 3 and has_draw and has_matchup_hint:
            return None
        return "non_moneyline"

    def _is_primary_moneyline_market(self, event: Dict[str, Any], market: Dict[str, Any], sport: str) -> bool:
        return self._primary_moneyline_reject_reason(event, market, sport) is None

    def _market_fails_optional_filters(self, market: Dict[str, Any]) -> bool:
        liquidity = self._safe_float(market.get("liquidity"))
        volume24 = self._safe_float(market.get("volume24hr") or market.get("volume24h"))
        if self.config.optional_min_liquidity is not None and (liquidity is None or liquidity < self.config.optional_min_liquidity):
            return True
        if self.config.optional_min_volume_24h is not None and (volume24 is None or volume24 < self.config.optional_min_volume_24h):
            return True
        return False

    def _sport_league_allowed(self, sport: str, league: str) -> bool:
        effective_sport = sport if sport in ALLOWED_SPORTS else self._infer_sport_from_league(league)
        if effective_sport not in ALLOWED_SPORTS:
            return False
        if league not in ALLOWED_LEAGUES_BY_SPORT.get(effective_sport, set()):
            return False
        if self.config.optional_league_allowlist:
            normalized = league.lower()
            return any(item in normalized for item in self.config.optional_league_allowlist)
        return True

    def _normalize_sport(self, event: Dict[str, Any], markets: Optional[List[Dict[str, Any]]] = None) -> str:
        haystack = self._event_haystack(event, markets)
        if any(keyword in haystack for keyword in UNWANTED_SPORT_TERMS):
            if any(keyword in haystack for keyword in ("cricket", "ipl")):
                return "cricket"
            if "tennis" in haystack:
                return "tennis"
            return "esports"
        if any(keyword in haystack for keyword in ("soccer", "football")):
            return "football"
        if any(keyword in haystack for keyword in ("ucl", "uel", "champions league", "europa league", "laliga", "la liga", "ligue 1", "bundesliga", "premier league", "epl", "super lig", "turkish", "sea games", "southeast asia")):
            return "football"
        if "mlb" in haystack or "baseball" in haystack:
            return "mlb"
        if any(keyword in haystack for keyword in ("ufc", "mixed martial arts", "mma")):
            return "mma"
        if "nba" in haystack or "basketball" in haystack:
            return "nba"
        if "nhl" in haystack or "hockey" in haystack:
            return "nhl"
        return ""

    def _normalize_league(self, event: Dict[str, Any], markets: Optional[List[Dict[str, Any]]] = None) -> str:
        haystack = self._event_haystack(event, markets)
        haystack = f" {haystack} "
        for keyword, normalized in LEAGUE_KEYWORDS.items():
            pattern = keyword if keyword.startswith(" ") or keyword.endswith(" ") else f" {keyword} "
            if pattern in haystack:
                return normalized
        return ""

    def _is_event_inactive(self, event: Dict[str, Any]) -> bool:
        return any(
            bool(event.get(field))
            for field in ("live", "closed", "ended", "archived", "started")
        )

    def _event_filter_reason(
        self,
        event: Dict[str, Any],
        event_start: Optional[datetime],
        now: datetime,
        horizon: datetime,
    ) -> Optional[str]:
        return self._event_date_filter_reason(event_start, now, horizon) or self._event_activity_filter_reason(event, event_start, now)

    def _event_date_filter_reason(
        self,
        event_start: Optional[datetime],
        now: datetime,
        horizon: datetime,
    ) -> Optional[str]:
        if not event_start:
            return "no_start_time"
        if event_start <= now:
            return "started_already"
        if event_start > horizon:
            return "beyond_horizon"
        return None

    def _event_activity_filter_reason(
        self,
        event: Dict[str, Any],
        event_start: Optional[datetime],
        now: datetime,
    ) -> Optional[str]:
        if bool(event.get("live")):
            return "live"
        if bool(event.get("ended")):
            return "ended"
        if bool(event.get("closed")) or bool(event.get("archived")):
            return "closed"
        if bool(event.get("started")) or (event_start is not None and event_start <= now):
            return "started_already"
        return None

    def _is_supported_outcome(self, outcome_name: str, sport: str) -> bool:
        name = outcome_name.lower().strip()
        if not name:
            return False
        if name in NON_MONEYLINE_OUTCOME_TERMS:
            return False
        return not any(term in name for term in PROP_MARKET_TERMS)

    def _log_refresh_debug(
        self,
        raw_events: int,
        deduped_events: int,
        raw_markets: int,
        date_window_events: int,
        active_events: int,
        events_with_markets: int,
        events_with_candidate_markets: int,
        moneyline_events: int,
        tracked_outcomes: int,
    ) -> None:
        logging.info(
            "Universe debug: raw sports-tagged events=%s | deduped events=%s | raw markets=%s | events with markets=%s | after date window=%s | after exclude live/ended/started/closed=%s | events with >=1 candidate market=%s | after moneyline=%s | tracked outcomes=%s",
            raw_events,
            deduped_events,
            raw_markets,
            events_with_markets,
            date_window_events,
            active_events,
            events_with_candidate_markets,
            moneyline_events,
            tracked_outcomes,
        )

    def _log_raw_event_dates(self, events: List[Dict[str, Any]], now: datetime, prefix: str) -> None:
        limit = min(20, len(events))
        logging.info("%s event date debug: showing first %s of %s", prefix, limit, len(events))
        for idx, event in enumerate(events[:limit], start=1):
            markets = event.get("markets") or []
            chosen_start, chosen_source = self._choose_event_start(event, markets)
            reason = self._event_filter_reason(event, chosen_start, now, now + timedelta(days=self.config.horizon_days)) or "passed"
            event_values = {field: event.get(field) for field in DEBUG_DATE_FIELDS if event.get(field) not in (None, "")}
            market_values = self._collect_market_date_values(markets)
            logging.info(
                "%s event %02d | title=%s | event.startTime=%s | event.eventStartTime=%s | event.eventDate=%s | event.startDate=%s | market.startTime=%s | market.eventStartTime=%s | market.eventDate=%s | market.startDate=%s | chosen_start=%s | chosen_source=%s | filter_reason=%s",
                prefix,
                idx,
                str(event.get("title") or event.get("question") or ""),
                event_values.get("startTime", "-"),
                event_values.get("eventStartTime", "-"),
                event_values.get("eventDate", "-"),
                event_values.get("startDate", "-"),
                market_values.get("startTime", "-"),
                market_values.get("eventStartTime", "-"),
                market_values.get("eventDate", "-"),
                market_values.get("startDate", "-"),
                chosen_start.isoformat() if chosen_start else "-",
                chosen_source,
                reason,
            )

    def _collect_market_date_values(self, markets: List[Dict[str, Any]]) -> Dict[str, Any]:
        values: Dict[str, Any] = {}
        for field in DEBUG_DATE_FIELDS:
            for market in markets:
                value = market.get(field)
                if value not in (None, ""):
                    values[field] = value
                    break
        return values

    def _log_filter_reason_counts(self, counts: Dict[str, int]) -> None:
        if not counts:
            logging.info("Event filter reasons: none")
            logging.info(
                "Reject counters: wrong_sport=0 | wrong_league=0 | no_start_time=0 | beyond_horizon=0 | not_game_market=0 | non_moneyline=0"
            )
            return
        ordered = [
            "rejected_wrong_sport",
            "rejected_wrong_league",
            "rejected_esports",
            "rejected_cricket",
            "rejected_future_event",
            "rejected_source_mismatch",
            "no_start_time",
            "started_already",
            "beyond_horizon",
            "live",
            "ended",
            "closed",
        ]
        parts = [f"{reason}={counts[reason]}" for reason in ordered if counts.get(reason)]
        for reason, count in counts.items():
            if reason not in ordered:
                parts.append(f"{reason}={count}")
        logging.info("Event filter reasons: %s", " | ".join(parts))
        logging.info(
            "Reject counters: wrong_sport=%s | wrong_league=%s | no_start_time=%s | beyond_horizon=%s | not_game_market=0 | non_moneyline=0",
            counts.get("rejected_wrong_sport", 0),
            counts.get("rejected_wrong_league", 0),
            counts.get("no_start_time", 0),
            counts.get("beyond_horizon", 0),
        )

    def _log_market_reject_reason_counts(self, counts: Dict[str, int]) -> None:
        if not counts:
            if self.config.log_poll_cycle_details:
                logging.info("Market reject reasons: none")
                logging.info("Reject counters market: not_game_market=0 | non_moneyline=0")
            return
        parts = [f"{reason}={count}" for reason, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]
        logging.info("Market reject reasons: %s", " | ".join(parts))
        logging.info(
            "Reject counters market: not_game_market=%s | non_moneyline=%s",
            counts.get("not_game_market", 0),
            counts.get("non_moneyline", 0) + counts.get("unsupported_outcome_count", 0),
        )

    def _normalize_market_quotes(self, prices: Dict[str, PriceQuote]) -> Dict[str, PriceQuote]:
        logging.info("Price normalization skipped: best ask mode enabled")
        return prices

    def _log_universe_summary(
        self,
        sport_counts: Dict[str, int],
        league_counts: Dict[str, int],
        filtered_market_count: int = 0,
        remaining_market_count: Optional[int] = None,
        remaining_outcome_count: Optional[int] = None,
    ) -> None:
        sport_parts = [f"{sport}={sport_counts.get(sport, 0)}" for sport in SPORT_SUMMARY_ORDER]
        logging.info("Universe summary by sport: %s", " | ".join(sport_parts))
        if league_counts:
            league_parts = [f"{league}={count}" for league, count in sorted(league_counts.items())]
            logging.info("Universe summary by league: %s", " | ".join(league_parts))
        else:
            logging.info("Universe summary by league: none")
        market_count = remaining_market_count if remaining_market_count is not None else len({ref.market_id for ref in self.outcomes.values()})
        outcome_count = remaining_outcome_count if remaining_outcome_count is not None else len(self.outcomes)
        logging.info(
            "Universe summary counts: filtered_markets_gt_90=%s | monitored_markets=%s | monitored_outcomes=%s",
            filtered_market_count,
            market_count,
            outcome_count,
        )

    def _log_source_pipeline(self, source_counts: Dict[str, Dict[str, int]]) -> None:
        if not source_counts:
            logging.info("Universe source pipeline: none")
            return
        for source, counts in sorted(source_counts.items()):
            logging.info(
                "Universe source pipeline: source=%s | raw=%s | source_matched=%s | source_mismatch=%s | deduped=%s | with_markets=%s | after_date_window=%s | after_exclude_live_ended_started_closed=%s | with_candidate_market=%s | after_moneyline=%s",
                source,
                counts.get("raw", 0),
                counts.get("source_matched", 0),
                counts.get("source_mismatch", 0),
                counts.get("deduped", 0),
                counts.get("with_markets", 0),
                counts.get("date_window", 0),
                counts.get("active", 0),
                counts.get("candidate_markets", 0),
                counts.get("moneyline", 0),
            )

    def _log_lost_events(self, lost_events: List[Dict[str, Any]]) -> None:
        if not lost_events:
            logging.info("Lost events debug: none")
            return
        logging.info("Lost events debug: showing first %s of %s", min(20, len(lost_events)), len(lost_events))
        for idx, item in enumerate(lost_events[:20], start=1):
            logging.info(
                "Lost event %02d | sport=%s | event=%s | chosen_start=%s | source=%s | markets=%s | reason=%s",
                idx,
                item["sport"] or "-",
                item["event_title"],
                item["chosen_start"],
                item["source_tag"],
                item["markets_count"],
                item["reason"],
            )

    def _log_rejected_event_samples(self) -> None:
        if self.config.log_rejected_events or self.config.log_rejected_events_sample <= 0:
            return
        if self.rejected_event_log_count == 0:
            if self.config.log_universe_debug:
                logging.info("Rejected events sample: none")
            return
        logging.info(
            "Rejected events sample: showing first %s of %s",
            len(self.rejected_event_log_samples),
            self.rejected_event_log_count,
        )
        for idx, item in enumerate(self.rejected_event_log_samples, start=1):
            logging.info(
                "REJECTED EVENT SAMPLE %02d | sport=%s | league=%s | event=%s | start=%s | start_source=%s | markets=%s | candidate_markets=%s | source=%s | reason=%s",
                idx,
                item["sport"],
                item["league"],
                item["event"],
                item["start"],
                item["start_source"],
                item["markets"],
                item["candidate_markets"],
                item["source"],
                item["reason"],
            )

    def _log_candidate_events(self, candidates: List[CandidateEvent]) -> None:
        if not candidates:
            logging.info("Dry-run sports-tagged candidate events: 0")
            return
        logging.info("Dry-run sports-tagged candidate events: showing first %s of %s", min(20, len(candidates)), len(candidates))
        for idx, candidate in enumerate(candidates[:20], start=1):
            logging.info(
                "Candidate %02d | sport=%s | league=%s | event=%s | start=%s | live=%s | ended=%s | markets=%s | source=%s",
                idx,
                candidate.sport,
                candidate.league or "-",
                candidate.event_title,
                candidate.start_time,
                candidate.live,
                candidate.ended,
                candidate.markets_count,
                candidate.source_tag,
            )

    def _event_sources(self, event: Dict[str, Any]) -> List[str]:
        raw_sources = event.get("_source_tags")
        if isinstance(raw_sources, list):
            return [str(item).strip() for item in raw_sources if str(item).strip()]
        return []

    def _event_source_label(self, event: Dict[str, Any]) -> str:
        sources = self._event_sources(event)
        return ",".join(sources) if sources else "-"

    def _event_haystack(self, event: Dict[str, Any], markets: Optional[List[Dict[str, Any]]] = None) -> str:
        target_source_tags = self._target_source_tags(event)
        source_tags = " ".join(target_source_tags)
        source_tag_labels = " ".join(
            item.split(":", 1)[1] if ":" in item else item
            for item in target_source_tags
        )
        market_haystack = ""
        if markets:
            market_haystack = " ".join(
                " ".join(
                    [
                        str(market.get("title") or ""),
                        str(market.get("question") or ""),
                        str(market.get("description") or ""),
                        str(market.get("slug") or ""),
                        str(market.get("marketSlug") or ""),
                    ]
                )
                for market in markets[:5]
            )
        return " ".join(
            [
                str(event.get("sport") or ""),
                str(event.get("category") or ""),
                str(event.get("league") or ""),
                str(event.get("competition") or ""),
                str(event.get("title") or ""),
                str(event.get("question") or ""),
                str(event.get("slug") or ""),
                source_tags,
                source_tag_labels,
                market_haystack,
            ]
        ).lower()

    def _target_source_tags(self, event: Dict[str, Any]) -> List[str]:
        return [source for source in self._event_sources(event) if source.startswith("tags:")]

    def _event_universe_reject_reason(self, sport: str, league: str, haystack: str) -> Optional[str]:
        if any(term in haystack for term in ("cricket", "ipl")):
            return "rejected_cricket"
        if any(term in haystack for term in UNWANTED_SPORT_TERMS if term not in {"cricket", "ipl"}):
            return "rejected_esports" if "tennis" not in haystack else "rejected_wrong_sport"
        if self._looks_like_future_market_text(haystack):
            return "rejected_future_event"
        if sport not in ALLOWED_SPORTS:
            return "rejected_wrong_sport"
        if league not in ALLOWED_LEAGUES_BY_SPORT.get(sport, set()):
            return "rejected_wrong_league"
        return None

    def _log_event_decision(
        self,
        prefix: str,
        event: Dict[str, Any],
        sport: str,
        league: str,
        event_start: Optional[datetime],
        event_start_source: str,
        reason: str,
        markets_count: int,
        candidate_market_count: int,
    ) -> None:
        if prefix == "ACCEPTED EVENT" and not self.config.log_accepted_events:
            return
        if prefix == "REJECTED EVENT":
            if self.config.log_rejected_events:
                logging.info(
                    "%s | sport=%s | league=%s | event=%s | start=%s | start_source=%s | markets=%s | candidate_markets=%s | source=%s | reason=%s",
                    prefix,
                    self._log_field(sport or "-"),
                    self._log_field(league or "-"),
                    self._log_field(str(event.get("title") or event.get("question") or "-")),
                    event_start.isoformat() if event_start else "-",
                    event_start_source,
                    markets_count,
                    candidate_market_count,
                    self._log_field(self._event_source_label(event)),
                    reason,
                )
                return
            self.rejected_event_log_count += 1
            if len(self.rejected_event_log_samples) < self.config.log_rejected_events_sample:
                self.rejected_event_log_samples.append(
                    {
                        "sport": self._log_field(sport or "-"),
                        "league": self._log_field(league or "-"),
                        "event": self._log_field(str(event.get("title") or event.get("question") or "-")),
                        "start": event_start.isoformat() if event_start else "-",
                        "start_source": event_start_source,
                        "markets": markets_count,
                        "candidate_markets": candidate_market_count,
                        "source": self._log_field(self._event_source_label(event)),
                        "reason": reason,
                    }
                )
            return
        logging.info(
            "%s | sport=%s | league=%s | event=%s | start=%s | start_source=%s | markets=%s | candidate_markets=%s | source=%s | reason=%s",
            prefix,
            self._log_field(sport or "-"),
            self._log_field(league or "-"),
            self._log_field(str(event.get("title") or event.get("question") or "-")),
            event_start.isoformat() if event_start else "-",
            event_start_source,
            markets_count,
            candidate_market_count,
            self._log_field(self._event_source_label(event)),
            reason,
        )

    def _log_market_decision(
        self,
        prefix: str,
        event: Dict[str, Any],
        market: Dict[str, Any],
        sport: str,
        league: str,
        market_start: Optional[datetime],
        market_start_source: str,
        reason: str,
    ) -> None:
        if prefix == "ACCEPTED MARKET" and not self.config.log_accepted_markets:
            return
        if prefix == "REJECTED MARKET" and not self.config.log_rejected_markets:
            self.rejected_market_log_count += 1
            if len(self.rejected_market_log_samples) < self.config.log_rejected_markets_sample:
                self.rejected_market_log_samples.append(
                    {
                        "sport": self._log_field(sport or "-"),
                        "league": self._log_field(league or "-"),
                        "event": self._log_field(str(event.get("title") or event.get("question") or "-")),
                        "market": self._log_field(str(market.get("question") or market.get("title") or "-")),
                        "start": market_start.isoformat() if market_start else "-",
                        "start_source": market_start_source,
                        "outcomes": self._log_field(",".join(str(item).strip() for item in self._json_list(market.get("outcomes")) if str(item).strip()) or "-"),
                        "slug": self._log_field(str(market.get("slug") or market.get("marketSlug") or "-")),
                        "reason": reason,
                    }
                )
            return
        logging.info(
            "%s | sport=%s | league=%s | event=%s | market=%s | start=%s | start_source=%s | outcomes=%s | slug=%s | reason=%s",
            prefix,
            self._log_field(sport or "-"),
            self._log_field(league or "-"),
            self._log_field(str(event.get("title") or event.get("question") or "-")),
            self._log_field(str(market.get("question") or market.get("title") or "-")),
            market_start.isoformat() if market_start else "-",
            market_start_source,
            self._log_field(",".join(str(item).strip() for item in self._json_list(market.get("outcomes")) if str(item).strip()) or "-"),
            self._log_field(str(market.get("slug") or market.get("marketSlug") or "-")),
            reason,
        )

    def _looks_like_future_market_text(self, text: str) -> bool:
        lowered = f" {text.lower()} "
        if any(phrase in lowered for phrase in SAFE_COMPETITION_PHRASES):
            sanitized = lowered
            for phrase in SAFE_COMPETITION_PHRASES:
                sanitized = sanitized.replace(phrase, " ")
        else:
            sanitized = lowered
        if any(term in sanitized for term in FUTURES_EVENT_TERMS):
            return True
        future_patterns = (
            r"\bwinner of (?:the )?(?:league|tournament|division|conference|season)\b",
            r"\b(?:league|tournament|division|conference|season|series)\s+winner\b",
            r"\b(?:world|national|american)\s+league\s+champion\b",
            r"\bteam to qualify\b",
            r"\bwho will fight next\b",
            r"\baward\b",
            r"\bmvp\b",
            r"\bchampion\b",
            r"\bchampionship winner\b",
            r"\brelegation\b",
            r"\btop[- ](?:4|6)\b",
            r"\bplayoffs?\b",
        )
        return any(re.search(pattern, sanitized) for pattern in future_patterns)

    def _log_rejected_market_samples(self) -> None:
        if self.config.log_rejected_markets or self.config.log_rejected_markets_sample <= 0:
            return
        if self.rejected_market_log_count == 0:
            if self.config.log_universe_debug:
                logging.info("Rejected markets sample: none")
            return
        logging.info(
            "Rejected markets sample: showing first %s of %s",
            len(self.rejected_market_log_samples),
            self.rejected_market_log_count,
        )
        for idx, item in enumerate(self.rejected_market_log_samples, start=1):
            logging.info(
                "REJECTED MARKET SAMPLE %02d | sport=%s | league=%s | event=%s | market=%s | start=%s | start_source=%s | outcomes=%s | slug=%s | reason=%s",
                idx,
                item["sport"],
                item["league"],
                item["event"],
                item["market"],
                item["start"],
                item["start_source"],
                item["outcomes"],
                item["slug"],
                item["reason"],
            )

    def _record_lost_event(
        self,
        lost_events: List[Dict[str, Any]],
        event: Dict[str, Any],
        sport: str,
        event_start: Optional[datetime],
        markets: List[Dict[str, Any]],
        reason: str,
    ) -> None:
        if len(lost_events) >= 20:
            return
        lost_events.append(
            {
                "sport": sport,
                "event_title": str(event.get("title") or event.get("question") or ""),
                "chosen_start": event_start.isoformat() if event_start else "-",
                "source_tag": self._event_source_label(event),
                "markets_count": len(markets),
                "reason": reason,
            }
        )

    @staticmethod
    def _top_reason(counts: Dict[str, int]) -> Optional[str]:
        if not counts:
            return None
        return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]

    @staticmethod
    def _normalize_market_outcome_name(value: str) -> str:
        normalized = value.strip().lower()
        if normalized in {"draw", "tie", "x"}:
            return normalized
        return normalized

    @staticmethod
    def _infer_sport_from_league(league: str) -> str:
        normalized = league.strip().lower()
        if any(item in normalized for item in ("ucl", "uel", "laliga", "la liga", "ligue 1", "bundesliga", "epl", "premier league", "turkish league", "sea games", "southeast asia")):
            return "football"
        if "mlb" in normalized:
            return "mlb"
        if "nba" in normalized:
            return "nba"
        if "nhl" in normalized:
            return "nhl"
        return ""

    def _log_tag_sources(self) -> None:
        if not self.last_tag_sources:
            logging.info("Sports tag sources used: none")
            return
        logging.info(
            "Sports tag sources used: %s",
            " | ".join(f"{tag_id}={source}" for tag_id, source in sorted(self.last_tag_sources.items())),
        )

    def _log_tracked_outcomes(self) -> None:
        if not self.outcomes:
            logging.info("TRACKED none")
            return
        self._log_tracked_refs(list(self.outcomes.values())[:10])

    def _log_price_samples(self, price_samples: List[Tuple[OutcomeRef, PriceQuote, Optional[float], Optional[float], Optional[float]]]) -> None:
        sample_limit = max(0, self.config.log_price_samples_count)
        if sample_limit <= 0:
            return
        if not price_samples:
            if self.config.log_poll_cycle_details:
                logging.info("PRICE none")
            return
        for idx, (ref, quote, prev_price, delta_pp, lookback_age_sec) in enumerate(price_samples[:sample_limit], start=1):
            slug_value = ref.event_url if ref.event_url and ref.event_url != "https://polymarket.com" else (ref.market_slug or ref.event_slug or "-")
            current_price = self._monitored_price(quote)
            logging.info(
                "PRICE %02d | sport=%s | league=%s | event=%s | market=%s | outcome=%s | asset_id=%s | start=%s | best_ask=%.4f | price_source=%s | prev_best_ask=%s | delta_pp=%s | lookback_age_sec=%s | slug=%s",
                idx,
                self._log_field(ref.sport or "-"),
                self._log_field(ref.league or "-"),
                self._log_field(ref.event_title or "-"),
                self._log_field(ref.market_title or "-"),
                self._log_field(ref.outcome_name or "-"),
                self._log_field(ref.asset_id or "-"),
                self._log_field(ref.start_time or "-"),
                current_price,
                self._log_field(quote.source),
                f"{prev_price:.4f}" if prev_price is not None else "-",
                f"{delta_pp:.2f}" if delta_pp is not None else "-",
                f"{lookback_age_sec:.1f}" if lookback_age_sec is not None else "-",
                self._log_field(slug_value),
            )

    def _log_monitoring_outcomes(
        self,
        monitored_refs: List[OutcomeRef],
        filtered_market_hits: List[MarketFilterHit],
    ) -> None:
        sport_counts: Dict[str, int] = defaultdict(int)
        league_counts: Dict[str, int] = defaultdict(int)
        for ref in monitored_refs:
            sport_counts[ref.sport] += 1
            league_counts[ref.league or "-"] += 1
        logging.info(
            "Monitoring summary by sport: %s",
            " | ".join(f"{sport}={sport_counts.get(sport, 0)}" for sport in SPORT_SUMMARY_ORDER),
        )
        if league_counts:
            logging.info(
                "Monitoring summary by league: %s",
                " | ".join(
                    f"{league}={count}"
                    for league, count in sorted(league_counts.items())
                ),
            )
        else:
            logging.info("Monitoring summary by league: none")
        self._log_universe_summary(
            sport_counts,
            league_counts,
            filtered_market_count=len(filtered_market_hits),
            remaining_market_count=len({ref.market_id for ref in monitored_refs}),
            remaining_outcome_count=len(monitored_refs),
        )
        if self.config.log_universe_debug:
            if monitored_refs:
                self._log_tracked_refs(monitored_refs[:10])
            else:
                logging.info("TRACKED none")

    def _log_tracked_refs(self, refs: List[OutcomeRef]) -> None:
        for ref in refs:
            slug_value = ref.event_url if ref.event_url and ref.event_url != "https://polymarket.com" else (ref.market_slug or ref.event_slug or "-")
            logging.info(
                "TRACKED | sport=%s | league=%s | event=%s | market=%s | outcome=%s | asset_id=%s | start=%s | slug=%s",
                self._log_field(ref.sport or "-"),
                self._log_field(ref.league or "-"),
                self._log_field(ref.event_title or "-"),
                self._log_field(ref.market_title or "-"),
                self._log_field(ref.outcome_name or "-"),
                self._log_field(ref.asset_id or "-"),
                self._log_field(ref.start_time or "-"),
                self._log_field(slug_value),
            )

    def _log_tracked_market_summary(self) -> None:
        if not self.outcomes:
            logging.info("TRACKED MARKET SUMMARY none")
            return
        grouped: Dict[str, List[OutcomeRef]] = defaultdict(list)
        for ref in self.outcomes.values():
            grouped[ref.market_id].append(ref)
        for idx, refs in enumerate(list(grouped.values())[:10], start=1):
            first = refs[0]
            outcomes = ", ".join(self._log_field(ref.outcome_name or "-") for ref in refs)
            logging.info(
                "TRACKED MARKET SUMMARY %02d | sport=%s | league=%s | event=%s | market=%s | start=%s | outcomes=%s",
                idx,
                self._log_field(first.sport or "-"),
                self._log_field(first.league or "-"),
                self._log_field(first.event_title or "-"),
                self._log_field(first.market_title or "-"),
                self._log_field(first.start_time or "-"),
                outcomes,
            )

    def _filter_market_ids_by_high_prob(self, prices: Dict[str, PriceQuote]) -> Tuple[set[str], List[MarketFilterHit]]:
        expected_counts: Dict[str, int] = defaultdict(int)
        for ref in self.outcomes.values():
            if ref.market_id:
                expected_counts[ref.market_id] += 1

        grouped: Dict[str, List[Tuple[OutcomeRef, PriceQuote]]] = defaultdict(list)
        for ref in self.outcomes.values():
            quote = prices.get(ref.asset_id)
            if quote is None or not ref.market_id:
                continue
            grouped[ref.market_id].append((ref, quote))

        filtered_market_ids: set[str] = set()
        filtered_hits: List[MarketFilterHit] = []
        for market_id, items in grouped.items():
            if len(items) != expected_counts.get(market_id, 0):
                continue
            offending: Optional[Tuple[OutcomeRef, PriceQuote]] = None
            for ref, quote in items:
                best_ask = self._monitored_price(quote)
                if best_ask > HIGH_PROB_THRESHOLD:
                    offending = (ref, quote)
                    break
            if offending is None:
                continue
            ref, quote = offending
            filtered_market_ids.add(market_id)
            slug_value = ref.event_url if ref.event_url and ref.event_url != "https://polymarket.com" else (ref.market_slug or ref.event_slug or "-")
            filtered_hits.append(
                MarketFilterHit(
                    market_id=market_id,
                    sport=ref.sport,
                    league=ref.league,
                    event_title=ref.event_title,
                    market_title=ref.market_title,
                    offending_outcome=ref.outcome_name,
                    offending_price=self._monitored_price(quote),
                    slug=slug_value,
                    outcomes_summary=", ".join(self._log_field(item_ref.outcome_name or "-") for item_ref, _item_quote in items),
                    prices_summary=", ".join(
                        f"{self._log_field(item_ref.outcome_name or '-')}: {self._monitored_price(item_quote):.4f}"
                        for item_ref, item_quote in items
                    ),
                )
            )
        return filtered_market_ids, filtered_hits

    def _log_filtered_markets_gt_90(self, hits: List[MarketFilterHit]) -> None:
        if not hits:
            if self.config.log_poll_cycle_details:
                logging.info("MARKET_SKIPPED_GT_90 none")
                logging.info("Reject counters price: gt_90_market=0")
            return
        logging.info("Reject counters price: gt_90_market=%s", len(hits))
        for hit in hits[:20]:
            logging.info(
                "MARKET_SKIPPED_GT_90 | event=%s | market=%s | outcomes=%s | best_asks=%s | offending_outcome=%s | offending_best_ask=%.4f | sport=%s | league=%s | slug=%s",
                self._log_field(hit.event_title or "-"),
                self._log_field(hit.market_title or "-"),
                self._log_field(hit.outcomes_summary or "-"),
                self._log_field(hit.prices_summary or "-"),
                self._log_field(hit.offending_outcome or "-"),
                hit.offending_price,
                self._log_field(hit.sport or "-"),
                self._log_field(hit.league or "-"),
                self._log_field(hit.slug or "-"),
            )

    def _log_top_movers(self, movers: List[Tuple[float, OutcomeRef, float, Optional[float]]]) -> None:
        if not movers:
            if self.config.log_empty_movers and self.config.log_poll_cycle_details:
                logging.info("MOVER none")
            return
        for idx, (delta_pp, ref, price, prev_price) in enumerate(sorted(movers, key=lambda item: item[0], reverse=True)[:10], start=1):
            logging.info(
                "MOVER %02d | event=%s | outcome=%s | best_ask=%.4f | prev_best_ask=%s | delta_pp=%.2f",
                idx,
                self._log_field(ref.event_title or "-"),
                self._log_field(ref.outcome_name or "-"),
                price,
                f"{prev_price:.4f}" if prev_price is not None else "-",
                delta_pp,
            )

    @staticmethod
    def _log_field(value: Any) -> str:
        text = str(value or "-")
        return text.replace("\r", " ").replace("\n", " ").replace("|", "/").strip() or "-"

    def _cooldown_expired(self, key: str, now_ts: float) -> bool:
        last = self.cooldowns.get(key)
        if last is None:
            return True
        return now_ts - last >= self.config.cooldown_minutes * 60

    def _trim_history(self, history: Deque[Tuple[float, float]], now_ts: float) -> None:
        min_ts = now_ts - (self.config.lookback_minutes * 60) - 60
        while history and history[0][0] < min_ts:
            history.popleft()

    def _prune_history(self) -> None:
        now_ts = time.time()
        for history in self.price_history.values():
            self._trim_history(history, now_ts)

    def _price_at_lookback(self, history: Deque[Tuple[float, float]], now_ts: float) -> Tuple[Optional[float], Optional[float]]:
        target = now_ts - self.config.lookback_minutes * 60
        candidate_price: Optional[float] = None
        candidate_ts: Optional[float] = None
        for timestamp, price in history:
            if timestamp <= target:
                candidate_price = price
                candidate_ts = timestamp
            else:
                break
        if candidate_price is None or candidate_ts is None:
            return None, None
        return candidate_price, now_ts - candidate_ts

    def _write_alert_csv(
        self,
        scanned_at: str,
        ref: OutcomeRef,
        old_price: float,
        new_price: float,
        move_pp: float,
        direction: str,
    ) -> None:
        file_exists = self.alerts_csv_path.exists()
        with self.alerts_csv_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            if not file_exists:
                writer.writerow(
                    [
                        "scanned_at",
                        "sport",
                        "league",
                        "event_title",
                        "market_title",
                        "event_url",
                        "start_time",
                        "asset_id or market_id",
                        "old_best_ask",
                        "new_best_ask",
                        "best_ask_move_10m_pp",
                        "direction",
                        "live",
                        "ended",
                    ]
                )
            writer.writerow(
                [
                    scanned_at,
                    ref.sport,
                    ref.league,
                    ref.event_title,
                    ref.market_title,
                    ref.event_url,
                    ref.start_time,
                    ref.asset_id,
                    f"{old_price:.4f}",
                    f"{new_price:.4f}",
                    f"{move_pp:.2f}",
                    direction,
                    ref.event_live,
                    ref.event_ended,
                ]
            )

    def _format_alert(
        self,
        ref: OutcomeRef,
        old_price: float,
        new_price: float,
        move_pp: float,
        direction: str,
    ) -> str:
        return (
            f"[{ref.sport.upper()} {ref.league}] {ref.event_title}\n"
            f"Маркет: {ref.market_title} | Outcome: {ref.outcome_name}\n"
            f"BEST_ASK {direction}: {old_price:.3f} -> {new_price:.3f} ({move_pp:.2f} pp за {self.config.lookback_minutes}m)\n"
            f"Старт: {ref.start_time}\n"
            f"{ref.event_url}"
        )

    def _load_state(self) -> None:
        if not self.state_json_path.exists():
            return
        try:
            payload = json.loads(self.state_json_path.read_text(encoding="utf-8"))
            raw_history = payload.get("price_history", {})
            for key, values in raw_history.items():
                self.price_history[key] = deque((float(ts), float(price)) for ts, price in values)
            self.cooldowns = {key: float(value) for key, value in payload.get("cooldowns", {}).items()}
        except Exception as exc:
            logging.warning("Не удалось загрузить state snapshot: %s", exc)

    def _save_state(self) -> None:
        serializable_history = {
            key: list(history)[-128:]
            for key, history in self.price_history.items()
            if history
        }
        payload = {
            "saved_at": datetime.now(tz=UTC).isoformat(),
            "price_history": serializable_history,
            "cooldowns": self.cooldowns,
        }
        self.state_json_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")

    def _acquire_instance_lock(self) -> None:
        handle = self.instance_lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.seek(0)
            holder = handle.read().strip() or "unknown"
            handle.close()
            raise RuntimeError(f"Уже запущен другой экземпляр бота: {holder}")
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()} started_at={datetime.now(tz=UTC).isoformat()}\n")
        handle.flush()
        self.instance_lock_handle = handle

    def _release_instance_lock(self) -> None:
        if self.instance_lock_handle is None:
            return
        try:
            fcntl.flock(self.instance_lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.instance_lock_handle.close()
            self.instance_lock_handle = None

    @staticmethod
    def _json_list(value: Any) -> List[Any]:
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            try:
                parsed = json.loads(stripped)
                return parsed if isinstance(parsed, list) else []
            except json.JSONDecodeError:
                return [item.strip() for item in stripped.split(",") if item.strip()]
        return []

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            if value is None or value == "":
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _extract_price_value(self, entry: Any) -> Optional[float]:
        if not isinstance(entry, dict):
            return self._safe_float(entry)
        candidates = (
            entry.get("best_ask"),
            entry.get("bestAsk"),
            entry.get("ask"),
            entry.get("SELL"),
            entry.get("sell"),
            entry.get("price"),
            entry.get("mid"),
            entry.get("midpoint"),
            entry.get("BUY"),
            entry.get("buy"),
        )
        for candidate in candidates:
            parsed = self._safe_float(candidate)
            if parsed is not None:
                return parsed
        return None

    def _extract_orderbook_price(self, entry: Any) -> Optional[float]:
        if isinstance(entry, dict):
            return self._safe_float(entry.get("price") or entry.get("rate") or entry.get("value"))
        if isinstance(entry, (list, tuple)) and entry:
            return self._safe_float(entry[0])
        return None

    def _monitored_price(self, quote: PriceQuote) -> float:
        if 0 < quote.ask <= 1:
            return quote.ask
        raise ValueError(f"Некорректная котировка для monitored best ask: {quote}")

    def _best_orderbook_bid(self, entries: List[Any]) -> Optional[float]:
        prices = [price for price in (self._extract_orderbook_price(entry) for entry in entries) if price is not None and 0 < price <= 1]
        return max(prices) if prices else None

    def _best_orderbook_ask(self, entries: List[Any]) -> Optional[float]:
        prices = [price for price in (self._extract_orderbook_price(entry) for entry in entries) if price is not None and 0 < price <= 1]
        return min(prices) if prices else None

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            normalized = str(value).replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if not parsed.tzinfo:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            return None

    @staticmethod
    def _csv_list(value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            result: List[str] = []
            for item in value:
                if isinstance(item, dict):
                    candidate = item.get("id") or item.get("tag_id") or item.get("tagId") or item.get("slug")
                    if candidate:
                        result.append(str(candidate).strip())
                        continue
                item_str = str(item).strip()
                if item_str:
                    result.append(item_str)
            return result
        return [item.strip() for item in str(value).split(",") if item.strip()]


def load_config() -> Config:
    load_dotenv()

    def env_str(key: str, default: str = "") -> str:
        return os.getenv(key, default).strip()

    def env_int(key: str, default: int) -> int:
        raw = env_str(key, str(default))
        return int(raw)

    def env_float_optional(key: str) -> Optional[float]:
        raw = env_str(key)
        return float(raw) if raw else None

    def env_bool(key: str, default: bool) -> bool:
        raw = env_str(key)
        if not raw:
            return default
        return raw.lower() in {"1", "true", "yes", "on"}

    allowlist_raw = env_str("OPTIONAL_LEAGUE_ALLOWLIST")
    allowlist = [item.strip().lower() for item in allowlist_raw.split(",") if item.strip()]
    horizon_days = env_int("HORIZON_DAYS", 7)

    return Config(
        tg_token=env_str("TG_TOKEN"),
        tg_chat_id=env_str("TG_CHAT_ID"),
        lookback_minutes=env_int("LOOKBACK_MINUTES", 10),
        min_move_pp=float(env_str("MIN_MOVE_PP", "1")),
        horizon_days=horizon_days,
        cooldown_minutes=env_int("COOLDOWN_MINUTES", 10),
        refresh_universe_sec=env_int("REFRESH_UNIVERSE_SEC", 300),
        snapshot_dir=Path(env_str("SNAPSHOT_DIR", "./snapshots")),
        optional_min_liquidity=env_float_optional("OPTIONAL_MIN_LIQUIDITY"),
        optional_min_volume_24h=env_float_optional("OPTIONAL_MIN_VOLUME_24H"),
        optional_league_allowlist=allowlist,
        dry_run=env_bool("DRY_RUN", not env_str("TG_TOKEN") or not env_str("TG_CHAT_ID")),
        log_accepted_events=env_bool("LOG_ACCEPTED_EVENTS", False),
        log_accepted_markets=env_bool("LOG_ACCEPTED_MARKETS", False),
        log_rejected_events=env_bool("LOG_REJECTED_EVENTS", False),
        log_rejected_events_sample=max(0, env_int("LOG_REJECTED_EVENTS_SAMPLE", 0)),
        log_rejected_markets=env_bool("LOG_REJECTED_MARKETS", False),
        log_rejected_markets_sample=max(0, env_int("LOG_REJECTED_MARKETS_SAMPLE", 0)),
        log_universe_debug=env_bool("LOG_UNIVERSE_DEBUG", False),
        log_sports_fetch_details=env_bool("LOG_SPORTS_FETCH_DETAILS", False),
        log_poll_cycle_details=env_bool("LOG_POLL_CYCLE_DETAILS", False),
        log_price_samples_count=max(0, env_int("LOG_PRICE_SAMPLES_COUNT", 0)),
        log_empty_movers=env_bool("LOG_EMPTY_MOVERS", False),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    config = load_config()
    app = PolymarketSportsMovers(config)
    app.run()


if __name__ == "__main__":
    main()
