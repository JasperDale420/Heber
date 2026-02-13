## 2026-02-13 - Hotloader Dataset Deduping
**Debt:** The hotloader CLI accepted duplicate datasets and would sync them multiple times in one run.
**Why it matters:** Repeated syncs add avoidable load and slow one-off runs without any data benefit.
**Next time:** Normalize the dataset list early and keep it unique to avoid duplicate work.
