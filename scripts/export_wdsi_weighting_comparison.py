"""Export public comparison tables from the verified November 19 historical rerun."""
import argparse
import hashlib
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import pandas as pd

SITE = Path(__file__).resolve().parents[1]
ARCHIVE = SITE / "data/wdsi_weighting_comparison_20261119.zip"

NOTES = """International WDSI weighting comparison | November 19, 2026
Companion to Diplomatic Sentiment and International Capital Flows: Do Words Move Money?

This is a same-sample check of the paper's international results, not a change
to the paper or a regression on the extended website data. Only the WDSI and
factors derived from it change. Financial outcomes, periods, controls,
seasonal effects and inference follow the original specification.

Method
Both daily indices cover the date shown and the preceding 13 calendar days.
paper_day: take the most adverse document each publication day, carry the
daily score across silent days, then average 14 equally weighted daily scores.
document: divide the sum of all accepted record scores in the window by the
number of those records, including zeros. Carry the last valid window mean
only across wholly empty windows. Positive WDSI denotes more adverse rhetoric.
Both monthly and quarterly signals are arithmetic means of available daily
index values; neither construction pools all documents for an entire quarter.
In Korea the historical document roster was reconstructed using the original
collection rules. It reproduces all 5,951 original daily stances, which does
not uniquely identify the historical document list. Using the complete Korean
score archive also leaves its main monthly debt net-flow estimate insignificant
and its quarterly estimate negative and significant at 5%.

Files
historical_index_periods.csv: all 15 countries. wdsi14_countryday and
wdsi14_document are paired HISTORICAL period means, not the current site
vintage. M = calendar month, Q = quarter; date is the period start.
signal_days counts available daily indices; required_days is the number of
calendar days in the full period. full_signal_period compares the two counts.
These are candidate signal periods, not every model's financial observations.

transaction_estimates.csv: all available non-U.S. country/asset/frequency
combinations for equity and fund shares, debt securities, and total portfolio
transactions. in = nonresidents' net purchases of domestic securities;
out = residents' net purchases of foreign securities; net = in minus out
before transformation. ihs_usd_million = asinh(flow in USD millions);
ihs_unit_invariant = asinh(flow / same-sample raw-flow standard deviation).
The main transaction results use ihs_usd_million and HC1 (normal reference).
HAC6 (monthly) and HAC2 (quarterly) use calendar-gap Bartlett weights and
a t reference. Results are not available for every country at both frequencies.

us_own_holdings.csv: Table 10A; 131 monthly observations. in/out are market
values of foreign holdings of U.S. equities / U.S. holdings of foreign
equities. net is the difference before transformation. These are positions
in USD millions transformed by asinh, not transactions. Main inference: HC1.
us_holdings_components.csv: Table 10B / IA.35. Equal or first principal
component (pc1) factors are re-estimated from the other 14 economies on 122
common months, November 2015 to December 2025. The fitted common part of
standardized U.S. WDSI and the U.S.-specific residual enter the same model.
common_minus_specific tests their coefficient difference. Holdings use 121
months ending November 2025; main inference is HAC6, t(99), with factors
treated as fixed. margin has the same meaning as in the own-holdings file.

For each estimate, paper_day_* and document_* are paired on identical
observations. b = coefficient; se = standard error; p = raw two-sided
p-value. n, start, end describe the financial sample, not corpus coverage.
For net transaction outcomes, bh and holm are adjusted p-values within
each asset/frequency/transformation/inference family. Blanks mean not
applicable. Total-portfolio adjustments are supplementary. A change in
significance alone is not a test of the difference between estimates.
The unchanged Stock Connect estimates in Table 4 already use document
weights and are not among the international re-estimates.
"""


def paired(source, keys, expected, adjusted=False):
    frame = pd.read_csv(source, dtype=str, keep_default_na=False)
    if "country" in keys:
        frame["country"] = frame.country.replace({"GB": "UK"})
    modes = [frame.loc[frame["mode"].eq(mode)].set_index(keys, verify_integrity=True).sort_index()
             for mode in ("paper_day", "doc14")]
    sample = ["n", "start", "end"]
    pd.testing.assert_frame_equal(modes[0][sample], modes[1][sample])
    if len(modes[0]) != expected:
        raise ValueError(f"Unexpected result count in {source.name}")
    values = ["b", "se", "p"] + (["bh", "holm"] if adjusted else [])
    return pd.concat([modes[0][sample], modes[0][values].add_prefix("paper_day_"),
                      modes[1][values].add_prefix("document_")], axis=1).reset_index()


def build(source):
    audit = json.loads((source / "pdf_checks.json").read_text())
    if audit["source_sha256"] != hashlib.sha256((SITE / "ssrn-6437669.pdf").read_bytes()).hexdigest():
        raise ValueError("The audited paper and website PDF are different versions")
    periods = pd.read_csv(source / "period_signals.csv", dtype=str, keep_default_na=False).rename(
        columns={"paper_day": "wdsi14_countryday", "doc14": "wdsi14_document"})
    files = {
        "historical_index_periods.csv": periods[["country_code", "frequency", "period", "date",
            "wdsi14_countryday", "wdsi14_document", "signal_days", "required_days", "full_signal_period"]],
        "transaction_estimates.csv": paired(source / "transactions_all.csv",
            ["country", "frequency", "asset_scope", "outcome", "transform", "inference"], 696, True),
        "us_own_holdings.csv": paired(source / "us_direct.csv", ["outcome", "inference"], 6),
        "us_holdings_components.csv": paired(source / "us_factor_holdings.csv",
            ["factor", "margin", "term", "inference"], 36),
    }
    with ZipFile(ARCHIVE, "w") as archive:
        items = {"README.txt": NOTES, **{name: t.to_csv(index=False, lineterminator="\n") for name,t in files.items()}}
        for name, content in items.items():
            info = ZipInfo(name, date_time=(2026, 11, 19, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            archive.writestr(info, content.encode("utf-8"))
    print(ARCHIVE.name, {name: len(t) for name, t in files.items()})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Directory containing the verified historical comparison outputs")
    build(parser.parse_args().source)
