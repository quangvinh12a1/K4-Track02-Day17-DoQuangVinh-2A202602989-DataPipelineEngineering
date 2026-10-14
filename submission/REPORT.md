# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

- **Họ tên / MSSV:** Đỗ Quang Vinh / 2A202602989
- **Repo:** https://github.com/quangvinh12a1/K4-Track02-Day17-DoQuangVinh-2A202602989-DataPipelineEngineering
- **Commit bài nộp:** `b346320` (commit mã nguồn cuối; output ở phần 5 chạy trên commit này)
- **AI đã dùng và phạm vi hỗ trợ:** Claude Code (Claude Opus 5.5) — đọc code, đề xuất cách sửa, chạy lệnh kiểm chứng, nháp REPORT và bonus. Tôi đã review và giải thích được mọi thay đổi.
- **Nguồn tham khảo khác:** slide Ngày 17; tài liệu Debezium (change event, tombstone).

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify: `silver_tickets` 24 dòng cho 12 ticket; T-91 có 3 trạng thái | `gold_feature_daily` lệch full recompute (`c50b…` ≠ `8630…`); u05 ngày 08-12 = (2, 0), cần (5, 1) | T-97 `is_deleted=False`, còn tên/nội dung; còn trong snapshot mới nhất và 2 chunk RAG |
| **Nguyên nhân gốc** | Chỉ dedup trong batch rồi `INSERT`: mỗi batch thêm hàng, không ghi theo khoá | `LOOKBACK_DAYS=0`: event đến muộn 3 ngày rơi vào partition không bao giờ được tính lại | Lấy `ticket_id` từ `after`; delete có `after=null` → id null → bị lọc mất |
| **Cách sửa** | `silver.py`: `MERGE ON ticket_id`, chỉ `UPDATE` khi `s._lsn > t._lsn` | `config.py`: `LOOKBACK_DAYS = 3` | `staging.py`: `coalesce(key, after, before).ticket_id` |
| **Khái niệm** | Silver có khoá, upsert idempotent, LSN chống ghi đè khi replay | Event time vs ingest time, lookback + overwrite-partition | CDC log-based, tombstone, xoá lan xuống Gold |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (P50 0.00, P95 2.90, max 3) → `LOOKBACK_DAYS = 3` = ceil(P99), đơn vị ngày lịch.
- `submission/checksums.txt`: **PASS** — Gold checksum `39e115c510ecdf526800eac227158a4f`
- Parity: **PARITY** (`silver_tickets` và `gold_feature_daily` trùng checksum)

## 3. Lựa chọn công cụ / kỹ thuật

- **MERGE vs overwrite-partition:** ticket là thực thể có khoá nên cập nhật theo khoá + LSN; feature là aggregate theo ngày nên xoá và tính lại cả cửa sổ lookback từ Silver là cách idempotent đơn giản nhất.
- **Tombstone thay vì xoá hẳn:** giữ `ticket_id` + LSN của lệnh xoá để replay batch cũ không "hồi sinh" ticket; cột PII đã null nên vẫn đáp ứng yêu cầu xoá.
- **Snapshot dựng lại từ Bronze "as of":** model đã train trên `v<day>` phải tái lập được; dữ liệu muộn tạo version mới thay vì âm thầm sửa tập cũ.
- **DuckDB/dbt thay vì Spark:** vài chục dòng/ngày vừa một máy; DuckDB chạy in-process, dbt cho contract/test/incremental khai báo; Spark chỉ thêm chi phí vận hành.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot bất biến về *nội dung huấn luyện* nhưng không đứng trên nghĩa vụ pháp lý: chạy job erasure loại T-97 khỏi mọi snapshot cũ và Bronze (hoặc crypto-shredding: mã hoá PII theo user, huỷ khoá khi có yêu cầu xoá), ghi audit log version nào bị sửa, rồi retrain nếu cần.
2. Đặt chốt ở ranh giới Bronze → Silver (cạnh `mask_pii`): thêm NER tiếng Việt để che tên, đối chiếu với danh sách tên trong bảng user. Đo bằng tập mẫu gán nhãn tay (precision/recall) và một contract đếm số dòng Silver/Gold còn bị detector PII bắt (phải = 0), theo dõi theo ngày.

## 5. Output (dán nguyên văn)

Chạy trên Windows PowerShell với `$env:PYTHONUTF8='1'` (console cp1252 không in được tiếng Việt) và `$env:DO_NOT_TRACK='1'`.

```text
$ .\.venv\Scripts\python.exe main.py
=== Day 17 pipeline: fresh build 2026-08-10 .. 2026-08-16 ===
  2026-08-10  bronze+0   tickets:4   events:6   quarantined:0 features[2026-08-07..2026-08-10] snapshot v2026-08-10:1 chunks:2 (embedded 2)
  2026-08-11  bronze+0   tickets:6   events:11  quarantined:0 features[2026-08-08..2026-08-11] snapshot v2026-08-11:2 chunks:4 (embedded 2)
  2026-08-12  bronze+0   tickets:8   events:17  quarantined:0 features[2026-08-09..2026-08-12] snapshot v2026-08-12:5 chunks:5 (embedded 1)
  2026-08-13  bronze+0   tickets:9   events:21  quarantined:1 features[2026-08-10..2026-08-13] snapshot v2026-08-13:6 chunks:6 (embedded 1)
  2026-08-14  bronze+0   tickets:10  events:25  quarantined:0 features[2026-08-11..2026-08-14] snapshot v2026-08-14:7 chunks:6 (embedded 1)
  2026-08-15  bronze+0   tickets:11  events:32  quarantined:1 features[2026-08-12..2026-08-15] snapshot v2026-08-15:7 chunks:6 (embedded 1)
  2026-08-16  bronze+0   tickets:12  events:39  quarantined:0 features[2026-08-13..2026-08-16] snapshot v2026-08-16:9 chunks:8 (embedded 3)

Gold checksums:
  gold_feature_daily   8630e04a61d10aa963b7a49e148926b0
  gold_training_set    9370ca77af233dfc2640e8cb61ef12d1
  gold_doc_chunks      cb9ebd12fdccafa4b746141147471964
  gold (combined)      39e115c510ecdf526800eac227158a4f

$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 2.57s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ .\.venv\Scripts\python.exe main.py --land-only
$ cd dbt_project; ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
03:00:07  Running with dbt=1.12.5
03:00:07  Registered adapter: duckdb=1.11.0
03:00:07  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
03:00:07  
03:00:07  Concurrency: 1 threads (target='dev')
03:00:07  
03:00:08  1 of 19 START sql view model main.stg_events ................................... [RUN]
03:00:08  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.08s]
03:00:08  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
03:00:08  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
03:00:08  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
03:00:08  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.11s]
03:00:08  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
03:00:08  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.10s]
03:00:08  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
03:00:08  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.12s]
03:00:08  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
03:00:08  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.03s]
03:00:08  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
03:00:08  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
03:00:08  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
03:00:08  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
03:00:08  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
03:00:08  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
03:00:08  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
03:00:08  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
03:00:08  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
03:00:08  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
03:00:08  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
03:00:08  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
03:00:08  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
03:00:08  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
03:00:08  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
03:00:08  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
03:00:08  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
03:00:08  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.01s]
03:00:08  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
03:00:08  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
03:00:08  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.04s]
03:00:08  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
03:00:08  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.03s]
03:00:08  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
03:00:08  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
03:00:08  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
03:00:08  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.02s]
03:00:08  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
03:00:08  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.03s]
03:00:08  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
03:00:08  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.03s]
03:00:08  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
03:00:08  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.03s]
03:00:08  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.22s]
03:00:08  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
03:00:08  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
03:00:08  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
03:00:08  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
03:00:08  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
03:00:09  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
03:00:09  
03:00:09  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.07 seconds (1.07s).
03:00:09  
03:00:09  Completed successfully
03:00:09  
03:00:09  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

### Bonus

**B1 — LLM step** (`pipeline/llm_label.py`): cache key = sha256(model + prompt_version + text) trong `llm_label_cache`; mọi câu trả lời thô (kể cả sai schema) được cache nên chạy lại = 0 call; `parse_label` validate, nhãn ngoài `bug/billing/other` vào `llm_label_quarantine`; ước tính token/chi phí trước khi gọi; mỗi dòng Gold mang `model` + `prompt_version`.

```text
$ .\.venv\Scripts\python.exe -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

**B2 — (b) brainstorm:** [`bonus/DESIGN.md`](../bonus/DESIGN.md) — pipeline hồ sơ khám bệnh scan → RAG cho bác sĩ tuyến huyện (6 quyết định có đánh đổi, phương án bị loại: knowledge graph toàn bộ bệnh án, sơ đồ kiến trúc).
