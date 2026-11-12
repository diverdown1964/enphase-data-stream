# Retiring the Enphase polling function

Status: **phases 1 to 4 done 2026-11-12** (timer disabled, cutover pinned to 2026-11-11T23:45:00Z, HaleJoliSolar model repointed to `Gold_SolarTelemetryUnified()` and refreshed, Enphase credentials rotated by John, resource group `enphaseenergypolling` deleted). Phases 5 and 6 not started.

## Why

The Kona Home Assistant now streams the full Enphase Envoy integration into the Fabric
Eventhouse (`HaleJoli.Raw_HaleJoliAllSignals`). The `kql/` scripts in this repo turn that into
`Silver_Solar` and `Gold_SolarIntervals()`, which reproduce `SolarTelemetry`'s 15-minute grain
within rounding (production ±13 Wh, consumption ±29 Wh per interval, verified 2026-11-08..10).
`Gold_SolarTelemetryUnified()` stitches the function's history (2025-11-30 → cutover) onto the
Home Assistant-derived intervals, so nothing is lost when the function stops.

## What retirement touches

| Where | Item | Action |
|---|---|---|
| Azure, RG `enphaseenergypolling` (sub "Microsoft Azure Sponsorship", Canada Central) | Function App `EnphaseEnergyPolling` (Flex Consumption plan `FLEX-EnphaseEnergyPolling-3afa`) | disable, then delete |
| | Storage account `enphaseenergypolling` (containers `enphase-tokens`, `azure-webjobs-*`, `app-package-*`) | delete with the RG |
| | App Insights `enphaseenergypolling`, Log Analytics `workspace-enphaseenergypolling`, two Smart Detection rules | delete with the RG |
| Enphase developer portal | The v4 application (client id/secret, API key) and its refresh token | delete or rotate |
| Fabric, HaleJoli DB | `SolarTelemetry` (30k rows of API history), `DimSystem`, `SolarEvents` (always empty) | keep `SolarTelemetry` and `DimSystem`; drop `SolarEvents` |
| Fabric, Connected Home | `HaleJoliSolar` semantic model (import, source = Kusto cluster) and report | repoint to `Gold_SolarTelemetryUnified()` |
| GitHub `diverdown1964/enphase-data-stream` (public) | repo, with real OAuth tokens in `.tokens.json` history | commit `kql/`, update README, scrub history, archive |

Nothing outside the resource group references the function (checked by resource name across the
subscription). The function's managed identity has no role on the Connected Home workspace and no
explicit principal on the HaleJoli database, so there is no Fabric access to revoke.

## Phases

Each phase is independently reversible until phase 4.

### Phase 1: stop polling (reversible in one command) — DONE 2026-11-12

Disable the timer trigger without touching anything else:

```bash
az functionapp config appsettings set -n EnphaseEnergyPolling -g enphaseenergypolling --settings AzureWebJobs.enphase_poller.Disabled=true
```

Rollback: set it to `false`. `SolarTelemetry` stops growing; the last API row becomes the natural
cutover. After this, pin the unified function's default cutover to that last row so the seam is
fixed rather than dependent on the default:

```kusto
SolarTelemetry | summarize max(reading_time)
```

then change `cutover: datetime = datetime(2026-11-07T00:00:00Z)` in `kql/02_gold_solar.kql`
to that value and re-apply the `Gold_SolarTelemetryUnified` command.

### Phase 2: move the report — DONE 2026-11-12

The `HaleJoliSolar` model imports from the Kusto cluster. Its `Solar Telemetry` partition now
queries `Gold_SolarTelemetryUnified()` instead of the `SolarTelemetry` table, with the same
projected columns (the unified function also carries `production_devices`, `consumption_devices`
and `battery_devices` so the model's columns are unchanged). A full refresh on F4 took 11 seconds.
Not yet surfaced in the model: `battery_mode`, `battery_status`, `consumption_ct_wh`, `source`.
If refreshes slow down as Silver_Solar grows, materialise the Gold output into a table on a
schedule instead of querying the function live.

### Phase 3: credentials — DONE 2026-11-12 (rotated by John in the Enphase portal)

In the Enphase developer portal (developer-v4.enphase.com), delete the application or rotate its
client secret and API key. This matters regardless of retirement: `.tokens.json` with a real
access and refresh token was committed in `bb5a186` and `ce3e4c4` and the repo is public.

### Phase 4: delete the Azure resources (irreversible) — DONE 2026-11-12

```bash
az group delete -n enphaseenergypolling --yes
```

This removes all seven resources, including the blob container holding the persisted refresh
token. Do this only after phase 3, so the deleted token is already dead. Rollback after this point
means redeploying from the repo with `func azure functionapp publish` and re-entering settings.

### Phase 5: repository

1. Commit `kql/` and this file.
2. Add a retirement note to the top of `README.md` pointing at `kql/` and the unified function.
3. Scrub `.tokens.json` from history (`git filter-repo --path .tokens.json --invert-paths`) and
   force-push, or accept that the tokens are dead after phase 3 and leave history alone.
4. Archive the repository on GitHub.

### Phase 6: tidy Fabric

```kusto
.drop table SolarEvents ifexists
```

Keep `SolarTelemetry` and `DimSystem`: they are the pre-November-2026 history.

## Open question

The Connected Home workspace has an **Admin** principal named "Storage Account3"
(id 280fbdff-dfa8-424b-abb6-fd6c777c1221) of type User. It is not the function's identity
(92c5010e-29f9-4fbb-ab88-ab30e22c5abb) and nothing in this plan depends on it, but it is worth
knowing what it is.
