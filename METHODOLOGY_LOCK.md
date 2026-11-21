# WDSI construction: November 19, 2026

The website uses the document-weighted WDSI for all 15 economies. The
[November 19 paper, Section 2.2.3](ssrn-6437669.pdf#page=8) uses document
weights for Stock Connect but day weights for its international estimates. A
website release date is not the end of the observation or regression sample.

| Series | Construction | Use |
| --- | --- | --- |
| `doc_count`, `doc_sum` | Number of accepted scored source records and sum of their adverse-positive scores on each date. Retain zero scores and count the active score store once, with its original record multiplicity. No record is invented on a silent day. | All country exports and both daily panels. |
| `c1_raw`, `c1` | Mean score of that day's documents; missing without documents. The carried daily value is supplied for reference, not used to compute rolling means. | Publication-day view and daily downloads. |
| `c1_14`, `rolling14`, `ma14` | Sum of document scores divided by number of documents in the last 14 consecutive calendar days. Carry the last valid window mean only when the whole window is empty. The 3-, 7- and 30-day WDSI use the same rule. | Website baseline for every country. |
| `wdsi14_document` | Arithmetic mean of available daily 14-day document indices in each calendar month or quarter. Each valid day has equal weight at this second stage. | Site period panels; filter `full_signal_period` for complete periods. |
| `wdsi14_countryday` | Period mean of the paper's 14-day equal-day series. | Frozen paper replication files and the day-weighted comparator in the historical comparison ZIP. |

All windows end on and include the date shown; require a full calendar window at
the start, and publish no dates after the country's reviewed coverage. In all
public WDSI fields, +3 is most adverse and -3 most conciliatory. The archival
record scale is the reverse: negate once. EDSI and ODSI retain their historical
scale (higher = more conciliatory), same-day minimum, forward fill and 3/7/30-day means.

## Rebuilding and reproducing the data

The [international weighting comparison](data/wdsi_weighting_comparison_20261119.zip)
pairs the paper's estimates with document-weighted estimates on the same financial
samples. Its period signals use the historical text vintage, not the extended site
vintage. It contains estimates and index inputs, not raw financial observations.
To regenerate the frozen download, run `python scripts/export_wdsi_weighting_comparison.py PATH_TO_VERIFIED_RERUN`;
routine index updates do not change it.

- `inputs/reviewed_daily.csv.gz`: original three-branch calendar/minima plus the WDSI sums and counts from the final active country score files on that calendar. Use the score record's assigned date (`time`). In the combined three-branch files, `publication` retains its historical meaning; in WDSI-only data it means `doc_count > 0`. For instance, 33 historical Canadian publication dates have no accepted document in the current score store. We do not reconstruct documents from an old daily score or append superseded records.
- `inputs/paper_countryday.csv.gz`: frozen international regression input (legacy polarity), using the shared paper country-day source except for Korea, where the paper uses its own archived `k1` series. Do not refresh this file when extending the site data.
- `data/wdsi_site_periods.csv` and `data/wdsi_paper_periods.csv` are the extended and frozen international signals, not the final outcome-specific financial regression samples. The main WDSI workbook contains the site daily and period panels.
- Separate paper replication workbook: `data/wdsi_paper_replication.xlsx` contains the original international daily/period panels and the Stock Connect series on the Figure 2 plotting calendar. `inputs/paper_document_days.csv.gz` contains the latter's frozen document statistics. Routine builds leave the frozen workbook and period CSV untouched.
- Rebuild from this checkout: `python scripts/build_dsi_site_data.py`, then `python scripts/check_method_lock.py`. For full export QA add `--workbooks`.
- For Russia from 2026-08-07, use the central ministry's English originals. Check the News, Official Statements and Briefings archives through the start-date boundary, and count cross-listed publication IDs once. For an event announcement use the issue date in its document number, not the scheduled event date. Do not substitute official-channel posts, unverified dates or partial batches. Full texts are archived; the established scorer reads at most the first 40,000 characters per document.
- To advance the site snapshot, rebuild the reviewed research corpus and three-branch daily panel first, then run `python scripts/import_wdsi_inputs.py`. The historical `records/*.csv` are **not** a complete document store for China; importing them as the historical baseline would change the weights. A newer, not-yet-reviewed date stops publication instead of silently relabeling stale series.
- Research sources for audit: the integrated `三类指数结果_扩展15国/all_countries_daily.csv`, all 15 active `情绪测度结果数据_扩展15国/*_scores.csv`, Figure 2's `docroll_14_signal_series.csv`, and `downstream_corrected_wdsi14_audit/{shared/daily_wdsi14.csv,us_jp_kr/kr_daily_same_source_wdsi14.csv}`. The paper's tables use the adverse-positive presentation; the archived signal reference files use the opposite sign.
