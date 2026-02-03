# Censys collector (Hosts -> JSONL/CSV)

## What it does
- Runs a Censys Hosts search query
- Saves raw results as JSONL
- Extracts matching HTTP endpoints and exports a flattened CSV
- Filenames include JST date (YYYY-MM-DD)

## Setup (GitHub)
1. Add repository secrets:
   - `CENSYS_API_ID`
   - `CENSYS_API_SECRET`

2. Run:
   - GitHub Actions -> `censys-collect` -> `Run workflow`

3. Outputs
- `out/censys_hosts_jp_moltbot_clawdbot_YYYY-MM-DD.jsonl`
- `out/censys_hosts_jp_moltbot_clawdbot_YYYY-MM-DD.csv`

Artifacts are uploaded as `censys-results`.
