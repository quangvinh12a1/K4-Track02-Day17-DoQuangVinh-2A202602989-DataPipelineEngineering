# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Đỗ Quang Vinh / 2A202602989
**Repo:** https://github.com/quangvinh12a1/K4-Track02-Day17-DoQuangVinh-2A202602989-DataPipelineEngineering
**Commit bài nộp:** commit cuối trên nhánh `main` (xem lịch sử Git)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus 5.5): đọc code, đề xuất 3 cách sửa, chạy lệnh kiểm chứng, nháp REPORT. Tôi đã review từng thay đổi và tự giải thích được.
**Nguồn tham khảo khác (nếu có):** Slide Ngày 17; tài liệu Debezium (định dạng change event, tombstone).

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify: `silver_tickets` 24 dòng cho 12 ticket; T-91 có 3 dòng (low/open, high/open, high/closed/bug); kéo theo `gold_doc_chunks` 22 dòng / 9 chunk | `gold_feature_daily` lệch full recompute (`c50b8851affe` ≠ `8630e04a61d1`); u05 ngày 08-12 ra (2, 0) thay vì (5, 1); `LOOKBACK_DAYS=0` < P99 = 3 | T-97 trong Silver `is_deleted=False`, còn tên + nội dung; snapshot mới nhất còn T-97; RAG index còn 2 chunk T-97 |
| **Nguyên nhân gốc** | `upsert_silver_tickets` chỉ dedup *trong* batch rồi `INSERT` — mỗi batch thêm hàng mới, không ghi theo khoá giữa các batch | Feature tính theo `event_time` nhưng mỗi run chỉ recompute đúng ngày của nó; event offline của u05 (xảy ra 08-12, đến 08-15) rơi vào partition 08-12 không bao giờ được tính lại | `ticket_changes_sql` lấy `ticket_id` từ `after`; với `op='d'` thì `after = null` → `ticket_id` null → bản ghi delete bị filter `WHERE ticket_id IS NOT NULL` vứt bỏ |
| **Cách sửa** | `pipeline/silver.py`: đổi `INSERT` thành `MERGE ... ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT` | `pipeline/config.py`: `LOOKBACK_DAYS = 3` = ceil(P99 = 3.00 ngày); mỗi run xoá + tính lại các partition `[day-3, day]` theo event date | `pipeline/staging.py`: `ticket_id = coalesce(key.ticket_id, after.ticket_id, before.ticket_id)`; tombstone Kafka (`value=null`) vẫn bị bỏ qua nhờ `_op IS NOT NULL` |
| **Khái niệm trên slide** | Silver có khoá: MERGE/upsert idempotent; LSN làm thứ tự — replay batch cũ không ghi đè trạng thái mới | Event time vs processing time, lateness đo được, lookback + overwrite-partition | CDC log-based: phong bì Debezium (`before`/`after`/`op`), delete → tombstone trong Silver, xoá truyền xuống Gold |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (P50 = 0.00, P95 = 2.90, max = 3, 43 bản ghi) → `LOOKBACK_DAYS = 3` (làm tròn lên theo ngày lịch để phủ hết P99, ở đây trùng với max)
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY** (`silver_tickets` 3c15dfd43701, `gold_feature_daily` 8630e04a61d1)

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: Silver là trạng thái hiện tại của một thực thể nên cập nhật theo khoá + LSN, còn feature là aggregate theo ngày nên xoá và tính lại cả cửa sổ lookback từ Silver là cách idempotent đơn giản nhất.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ `ticket_id` + `_lsn` của lệnh xoá để replay batch cũ (LSN nhỏ hơn) không "hồi sinh" ticket, đồng thời các cột PII đã null nên vẫn thoả yêu cầu xoá dữ liệu.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: model đã train trên `v<day>` phải tái lập được; dữ liệu đến muộn tạo version mới thay vì âm thầm thay đổi tập train cũ.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu vài chục dòng/ngày vừa một máy, DuckDB chạy in-process không cần cluster, dbt cho contract/test/incremental khai báo — Spark chỉ thêm chi phí vận hành.

## 4. Hai câu hỏi suy ngẫm

1. Tôi coi snapshot là bất biến *về nội dung huấn luyện* nhưng không bất biến trước nghĩa vụ pháp lý: khi có yêu cầu xoá, chạy một job "erasure" ghi đè/loại bỏ hàng của T-97 trong mọi snapshot cũ (hoặc mã hoá PII theo từng user và huỷ khoá — crypto-shredding), ghi log audit version nào bị sửa và vì sao, rồi retrain model dùng các snapshot đó nếu cần. Bronze cũng phải áp dụng cùng cơ chế, nếu không rebuild sẽ khôi phục lại dữ liệu.
2. Đặt chốt PII ở ranh giới Bronze → Silver (chỗ `mask_pii`): thêm NER tiếng Việt (vd. underthesea/PhoBERT NER) để che tên người, kết hợp danh sách tên/địa chỉ đã biết từ bảng user; Bronze chỉ cho số ít người truy cập. Đo bằng một tập mẫu gán nhãn tay (precision/recall của che PII), cộng contract tự động đếm số dòng Silver/Gold còn khớp detector PII (phải = 0) và giám sát tỷ lệ che theo ngày.

## 5. Output (dán nguyên văn)

Chạy trên Windows PowerShell, `$env:PYTHONUTF8='1'` (console cp1252 không in được tiếng Việt).

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
34 passed in 2.43s

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
02:53:08  Running with dbt=1.12.5
02:53:08  Registered adapter: duckdb=1.11.0
02:53:09  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
02:53:09  
02:53:09  Concurrency: 1 threads (target='dev')
02:53:09  
02:53:09  1 of 19 START sql view model main.stg_events ................................... [RUN]
02:53:09  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.07s]
02:53:09  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
02:53:09  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.04s]
02:53:09  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
02:53:09  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.16s]
02:53:09  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
02:53:09  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.14s]
02:53:09  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
02:53:10  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.22s]
02:53:10  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
02:53:10  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
02:53:10  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
02:53:10  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.03s]
02:53:10  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
02:53:10  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.03s]
02:53:10  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
02:53:10  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.05s]
02:53:10  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
02:53:10  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.03s]
02:53:10  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
02:53:10  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.04s]
02:53:10  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
02:53:10  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
02:53:10  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
02:53:10  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.03s]
02:53:10  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
02:53:10  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
02:53:10  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
02:53:10  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
02:53:10  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
02:53:10  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
02:53:10  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.05s]
02:53:10  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
02:53:10  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.03s]
02:53:10  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
02:53:10  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.05s]
02:53:10  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
02:53:10  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.02s]
02:53:10  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
02:53:10  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.03s]
02:53:10  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
02:53:10  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.04s]
02:53:10  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
02:53:10  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.03s]
02:53:10  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.28s]
02:53:10  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
02:53:10  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.04s]
02:53:10  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
02:53:10  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
02:53:10  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
02:53:10  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.03s]
02:53:10  
02:53:10  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.44 seconds (1.44s).
02:53:10  
02:53:10  Completed successfully
02:53:10  
02:53:10  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
