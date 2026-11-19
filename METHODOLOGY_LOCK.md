# WDSI construction: November 19, 2026

Authority: [Yang, Zhang, Kyriakou and Papapostolou, Section 2.2.3](ssrn-6437669.pdf#page=8).
The web series implement the paper's construction on the reviewed site corpus. A
website release date is not the end of the observation or regression sample.

| Series | Construction | Use |
| --- | --- | --- |
| `c1_14` (country files), `ma14` (WDSI panel) | Take the most adverse official score each publication day, carry that stance through nonpublication days, and average the last 14 consecutive calendar days. Equal weight per day. | Board, explorer default, international comparisons (including China); Figures 3 and 4 use a restricted plotting sample. |
| `wdsi14_countryday` | Mean of available daily 14-day country-day values in each calendar month or quarter. Each valid day has equal weight, including in quarters. Empty periods have no value. | International signal panels; filter `full_signal_period` when complete periods are required. |

All windows end on and include the date shown; require a full calendar window at
the start, and publish no dates after the country's reviewed coverage. In all
public WDSI fields, +3 is most adverse and -3 most conciliatory. The archival
record scale is the reverse: negate once. EDSI and ODSI retain their historical
scale (higher = more conciliatory), same-day minimum, forward fill and 3/7/30-day means.

## Rebuilding and reproducing the data

- `inputs/reviewed_daily.csv.gz`: archived daily raw minima of all three branches from the reviewed country panel, with the actual publication calendar. All 15 public dashboards, country files and the main WDSI panel use identical fields, controls and aggregation.
- `inputs/paper_countryday.csv.gz`: frozen international regression input (legacy polarity), using the shared paper country-day source except for Korea, where the paper uses its own archived `k1` series. Do not refresh this file when extending the site data.
- `data/wdsi_site_periods.csv` and `data/wdsi_paper_periods.csv` are the extended and frozen international signals, not the final outcome-specific financial regression samples. The main WDSI workbook contains the site daily and period panels.
- Separate paper replication workbook: `data/wdsi_paper_replication.xlsx` contains the frozen international daily and period panels and the historical Stock Connect series. For the latter only, `inputs/paper_document_days.csv.gz` supplies frozen daily adverse-positive document sums and counts. The historical 14-day measure weights each accepted source record equally, retaining non-war zeros, adding no observations on nonpublication dates and carrying the prior mean only across wholly empty windows. Its plotting calendar matches Figure 2; this measure never replaces the public cross-country index.
- Rebuild from this checkout: `python scripts/build_dsi_site_data.py`, then `python scripts/check_method_lock.py`. For full export QA add `--workbooks`.
- To advance the site snapshot, rebuild the reviewed research corpus and three-branch daily panel first, then run `python scripts/import_wdsi_inputs.py`. The historical `records/*.csv` are **not** a complete document store for China; importing them as the historical baseline would change the weights. A newer, not-yet-reviewed date stops publication instead of silently relabeling stale series.
- Research sources for audit: the integrated `三类指数结果_扩展15国/all_countries_daily.csv`, accepted `情绪测度结果数据_扩展15国/CN_scores.csv`, Figure 2's `docroll_14_signal_series.csv`, and `downstream_corrected_wdsi14_audit/{shared/daily_wdsi14.csv,us_jp_kr/kr_daily_same_source_wdsi14.csv}`. The paper's tables use the adverse-positive presentation; the archived signal reference files use the opposite sign.
