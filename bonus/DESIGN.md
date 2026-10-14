# Bonus B2 — Thiết kế: pipeline hồ sơ khám bệnh scan → RAG cho bác sĩ tuyến huyện

**Học viên:** Đỗ Quang Vinh — 2A202602989

## 1. Bài toán và ràng buộc thực

Một chuỗi phòng khám/bệnh viện tuyến huyện muốn có trợ lý hỏi đáp cho bác sĩ:
*"Bệnh nhân này đã từng dị ứng thuốc gì? Lần khám trước kê đơn gì, HbA1c bao nhiêu?"*.
Nguồn dữ liệu là hàng chục nghìn **bệnh án và đơn thuốc giấy đã scan** (PDF ảnh, chữ in
lẫn chữ viết tay, bảng xét nghiệm nhiều cột), cộng với export từ phần mềm HIS cũ
(CSV, schema mỗi cơ sở một kiểu). Người dùng là bác sĩ đang khám, nên câu trả lời phải
**đúng bệnh nhân, có trích dẫn trang gốc**, và chậm vài giây vẫn chấp nhận được.

Vì sao khó: OCR tiếng Việt có dấu trên ảnh scan chất lượng thấp sai nhiều; dữ liệu là
**dữ liệu sức khoẻ — loại dữ liệu cá nhân nhạy cảm** theo Nghị định 13/2023/NĐ-CP;
hạ tầng tại cơ sở yếu (băng thông thấp, không có GPU); và một câu trả lời "bịa" về
dị ứng thuốc có thể gây hại thật.

## 2. Các câu hỏi then chốt và quyết định

### Q1 — Nguồn & hình dạng: dữ liệu bẩn cỡ nào?
**Quyết định:** giữ Bronze bất biến gồm *file PDF gốc + kết quả OCR thô (JSON có toạ độ,
độ tin cậy từng từ)*, versioned theo `ocr_engine_version`. Silver là "trang đã chuẩn
hoá": Unicode NFC, tách bảng xét nghiệm thành dòng có cấu trúc, gắn `patient_id`.
**Đánh đổi:** lưu OCR thô tốn dung lượng hơn chỉ lưu text sạch, nhưng khi đổi engine OCR
(chắc chắn sẽ đổi) ta chạy lại từ Bronze như lab chạy lại từ Parquet, thay vì scan lại giấy.
Cùng ý tưởng cache embedding trong lab: key = hash(trang ảnh) + phiên bản OCR.

### Q2 — Batch hay streaming?
**Quyết định:** **batch hằng đêm** cho hồ sơ scan, cộng một đường **micro-batch 15 phút**
cho export HIS của ngày hôm đó.
**Đánh đổi X vs Y:** streaming (Kafka + Flink) cho độ tươi tính bằng giây, nhưng hồ sơ giấy
vốn được scan theo lô cuối ngày — độ tươi không thể tốt hơn tốc độ người scan. Batch đơn
giản, chạy lại/backfill bằng cùng code path (như DAG theo ngày của lab), và rẻ.

### Q3 — Hợp đồng & chất lượng trước khi vào index
**Quyết định:** ba chốt chặn, dòng xấu đi quarantine chứ không dừng run:
(1) trang có độ tin cậy OCR trung bình < 0.85 → hàng đợi **người duyệt**;
(2) không gán được `patient_id` chắc chắn (khớp mã BN + ngày sinh) → quarantine, **không bao giờ
index** — trả lời nhầm bệnh nhân tệ hơn không trả lời;
(3) giá trị xét nghiệm ngoài khoảng sinh lý (HbA1c = 75%) → cờ nghi lỗi OCR.
Cảnh báo khi tỷ lệ quarantine của một cơ sở tăng > 2 lần trung bình 7 ngày (thường là
máy scan hỏng hoặc đổi mẫu phiếu).
**Đánh đổi:** ngưỡng chặt làm recall thấp và tăng việc cho người duyệt; tôi chấp nhận vì
trong y tế precision quan trọng hơn độ phủ.

### Q4 — Phi cấu trúc → RAG hay Knowledge Graph?
**Quyết định:** **lai**. Dữ kiện có cấu trúc (dị ứng, chẩn đoán ICD-10, đơn thuốc, xét
nghiệm) vào bảng Gold dạng *patient timeline* để tra cứu chính xác bằng SQL; văn bản tự do
(diễn biến bệnh, ghi chú) vào vector index **lọc cứng theo `patient_id`** trước khi tìm.
**Phương án bị loại — knowledge graph toàn bộ bệnh án:** câu hỏi của bác sĩ gần như luôn
nằm trong *một* bệnh nhân, không phải multi-hop qua nhiều thực thể; dựng và duy trì KG
(trích quan hệ bằng LLM trên OCR lỗi) tốn kém, khó kiểm chứng, và lỗi trích quan hệ
lan truyền âm thầm. Timeline + lọc theo bệnh nhân đạt phần lớn giá trị với chi phí nhỏ.

### Q5 — Failure semantics và quyền được xoá
**Quyết định:** mọi bước idempotent theo khoá (`patient_id`, `document_id`, `page_no`),
MERGE theo phiên bản như `silver_tickets` trong lab. Khi bệnh nhân rút đồng ý: tombstone
ở Silver, xoá chunk khỏi index, và **crypto-shredding** — mỗi bệnh nhân một khoá mã hoá
cho Bronze; huỷ khoá thì bản gốc trong Bronze và backup cũ cũng không đọc được.
**Đánh đổi:** quản lý khoá (KMS) phức tạp hơn nhiều so với "snapshot bất biến", nhưng đây
chính là mâu thuẫn ở câu suy ngẫm 1 của lab — với dữ liệu sức khoẻ, luật thắng tính bất biến.

### Q6 — Chi phí & bối cảnh Việt Nam
**Quyết định:** OCR + embedding chạy **on-premise / cloud trong nước** (dữ liệu sức khoẻ
không nên rời Việt Nam); dùng model embedding đa ngôn ngữ cỡ nhỏ chạy CPU, LLM sinh câu
trả lời gọi qua API chỉ với các đoạn đã che danh tính (thay tên bằng mã).
80% chi phí nằm ở OCR lần đầu và người duyệt quarantine — cắt bằng cache theo hash trang
(không OCR lại trang trùng — bản photo nhiều lần rất phổ biến) và ưu tiên duyệt theo
tần suất bệnh nhân tái khám.
**Đánh đổi:** model tự host kém hơn API lớn vài điểm chất lượng, đổi lại tuân thủ và chi
phí cố định, dự báo được.

## 3. Sơ đồ kiến trúc

```
 Máy scan tại cơ sở ──┐                     ┌─▶ quarantine (OCR kém / không rõ BN)
 (PDF ảnh, theo lô)   │                     │        └─▶ hàng đợi người duyệt
                      ▼                     │
 Export HIS (CSV) ─▶ BRONZE ──OCR (cache)──▶ SILVER ───────────┬─▶ GOLD patient_timeline
                     PDF gốc + OCR thô      trang chuẩn hoá,   │   (dị ứng, ICD-10, đơn thuốc,
                     mã hoá theo BN         gắn patient_id,    │    xét nghiệm) → tra SQL
                     (crypto-shredding)     PII tách riêng     └─▶ GOLD note_chunks + embedding
                                                                    (cache hash+model, lọc BN)
                                                                          │
                       Bác sĩ hỏi ─▶ API: lọc patient_id ─▶ SQL timeline + vector search
                                     ─▶ LLM (đoạn đã che danh tính) ─▶ trả lời + trích trang gốc
```

## 4. Điều mang theo từ lab

Bronze bất biến để chạy lại khi đổi OCR; ghi theo khoá + phiên bản để replay không ghi đè
dữ liệu mới; tombstone lan xuống mọi index; cache theo hash + model version để chạy lại
không tốn tiền; và "chạy lại một ngày cũ ba lần, ba checksum giống nhau" là bài kiểm tra
tôi sẽ đặt vào CI cho pipeline này trước khi cho bác sĩ dùng.
