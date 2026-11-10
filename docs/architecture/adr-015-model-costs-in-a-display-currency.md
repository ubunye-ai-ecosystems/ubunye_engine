# ADR 015: Model costs in a display currency

**Status:** accepted, 0.9.2

## Context

`ubunye.llm` priced, capped and reported model calls in US dollars only: the price
table, `UBUNYE_LLM_MAX_USD`, the `ubunye plan` bill, the run record and the FOCUS
rows. Providers price in dollars, but many people who read the bill do not pay in
dollars. A South African team thinks in rand, and a ceiling of "$2" means little
to them.

An exchange rate moves every day. A rate the engine fetched or guessed would make
the same run cost different amounts on different days, and a receipt that cannot
say which rate it used is not a receipt.

## Decision

- **Prices stay in dollars.** The price table, `price=` and `UBUNYE_LLM_PRICES`
  do not change. Only what is shown and what is capped is converted.
- **A display currency, with an explicit, dated rate.** `UBUNYE_LLM_CURRENCY=ZAR`
  names any ISO 4217 code. `UBUNYE_LLM_FX_ZAR=18.40@2026-11-10` gives the rate
  (units per US dollar) and the day it was read, in one variable so they cannot
  drift apart. The variable is named after the code, so switching to `EUR` with a
  leftover rand rate is an error, not a wrong bill. The engine never fetches or
  guesses a rate: a currency with no rate, or a rate with no date, stops the run
  before any call with an error that says what to set. Unset, or `USD`, is dollars,
  as before.
- **A ceiling in the display currency.** `UBUNYE_LLM_MAX_ZAR=50` (the pattern is
  `UBUNYE_LLM_MAX_<CODE>`) caps the run in rand: each call's worst case is
  converted at the given rate and checked before it is sent, like
  `UBUNYE_LLM_MAX_USD`, which keeps working exactly as before. With both set, both
  apply. A `UBUNYE_LLM_MAX_<CODE>` that does not match the display currency is an
  error, so a cap is never silently ignored.
- **Environment, not config.** Budgets were already environment variables, and the
  `llm` package has no place in `config.yaml`. The currency, the rate and the cap
  belong to whoever runs and pays for the task, and the rate changes daily; the
  task (what the data is, what the rule is) would not change if it moved, so these
  do not belong in its config (see "What the config owns" in CLAUDE.md).
- **The receipt stays honest.** Every cost in the run record stays in dollars
  (`cost_usd`, `estimated_usd`, `spent_usd`). The record's `llm_budget` adds
  `currency: {code, per_usd, as_of}`, the rate the run used, and the display cap
  as `max_<code>`. No new record field and no record version bump: `llm_budget` is
  already a free-form dict, and a record without `currency` reads as dollars.
- **Shown in the display currency.** `ubunye plan` prints the bill in the display
  currency, with one line naming the rate, its date and the dollar estimate.
  `ubunye lineage trace` and the progress line convert with the rate the run used.
- **FOCUS rows use the run's rate.** `BillingCurrency` is the display currency and
  the cost columns are converted. FOCUS 1.4 has columns for a price in a different
  currency, so the dollar figures stay on each row: `PricingCurrency` (`USD`),
  `PricingCurrencyListUnitPrice`, `PricingCurrencyContractedUnitPrice` and
  `PricingCurrencyEffectiveCost`. Custom `x_ExchangeRate` and `x_ExchangeRateAsOf`
  name the rate. These columns appear only when the run had a display currency,
  so dollar rows are unchanged.

## Consequences

- A rand reader sees rand everywhere, and every number can be traced back to the
  dollar price and the stated rate.
- The rate is the user's to keep current. A stale rate gives a stale bill, but the
  bill says which rate and date it used.
- A port's own ceiling stays in dollars (`llm.port(..., max_usd=)`); only the run's
  ceiling has a display-currency form.
