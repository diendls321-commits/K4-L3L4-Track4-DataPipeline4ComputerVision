# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Văn Diện |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602615` |
| GitHub | `diendls321-commits` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Nguồn A có 3.021 dòng, B có 3.234 dòng; hai bảng có các khác biệt sau:

| Thuộc tính | Nguồn A | Nguồn B |
|---|---|---|
| Thời gian | `ts_utc`, chuỗi ISO UTC, ví dụ `2025-10-07T08:00:00.014Z` | `timestamp_ms`, epoch mili giây, ví dụ `1759933801509` |
| Tốc độ | `speed_mps`, m/s | `spd_kmh`, km/h |
| Hướng | `heading_deg`, độ | `hdg`, độ |
| Vị trí và cao độ | `lat`, `lon`, `alt_m` | `latitude`, `longitude`, `altitude` |
| Gia tốc | `acc_x`, `acc_y` | `ax`, `ay` |
| Bàn đạp ga/phanh | `gas`, `brake`, tỷ lệ 0–1 | `throttle_pct`, `brake_pct`, phần trăm 0–100 |
| Góc lái | `steer` | `steering` |
| Đồng hồ telemetry so với video | Độ lệch khai báo 0 ms | Độ lệch khai báo +1.500 ms, chưa được trừ ở Bronze |

Mỗi nguồn có bảng Bronze riêng để giữ đúng schema gốc, giúp truy vết và xử lý lại khi quy tắc chuẩn hoá thay đổi.
Các cột dữ liệu CSV được giữ ở dạng `VARCHAR`, kể cả ô trống, để không mất dữ liệu hoặc làm hỏng ingest khi gặp giá trị sai; việc ép kiểu, đổi đơn vị và cách ly lỗi thực hiện ở Silver.
Schema thực tế có ngoại lệ ở metadata do pipeline tạo: `_row_no` là `BIGINT`, `_ingested_at` là timestamp có múi giờ; bảng metadata video cũng có cột số.
Sau khi nạp lại cả A và B, video và telemetry đều báo `already present`, số dòng vẫn 3.021/3.234; mỗi bảng telemetry chỉ có một commit Delta (`version=0`).
Pipeline nhận diện batch bằng băm SHA-256 của nội dung (dùng 12 ký tự đầu làm `_batch_id`); bảng video giữ đủ SHA-256 và con trỏ, còn video gốc nằm ở `s3://lake/bronze/video/<drive>/raw.mp4`.
Bước 1 đạt **5/5 checks passed**; đầu ra và truy vấn kiểm chứng được lưu tại [`report/evidence/step1_bronze.txt`](report/evidence/step1_bronze.txt).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging giữ tốc độ A ở m/s, đổi tốc độ B từ km/h sang m/s bằng cách chia 3,6; đổi bàn đạp B từ % sang tỷ lệ bằng cách chia 100 và ánh xạ tên cột về schema của A.
Thời gian A được ép sang timestamp có múi giờ; B được trừ `telemetry_clock_offset_ms=1500` trước khi đổi epoch mili giây sang timestamp UTC, rồi hai nguồn được hợp nhất theo tên cột.
Mẫu sạch đầu tiên của B là `2025-10-08 14:30:00.009000+00:00`, chỉ sau mốc bắt đầu video `14:30:00 UTC` 9 ms; tốc độ tối đa làm tròn của A/B là 71/100 km/h.

| Drive | Trùng (`duplicate`) | Thiếu tốc độ (`speed_missing`) | Tốc độ vô lý (`speed_out_of_range`) | Tổng cách ly |
|---|---:|---:|---:|---:|
| `drive_a` | 20 | 30 | 3 | 53 |
| `drive_b` | 20 | 30 | 3 | 53 |
| **Tổng** | **40** | **60** | **6** | **106** |

Dữ liệu được bảo toàn: A có 2.968 dòng sạch + 53 dòng cách ly = 3.021 dòng nhận vào; B có 3.181 + 53 = 3.234; tổng sạch là 6.149 dòng.
Các dòng lỗi nằm trong `silver.silver_telemetry_rejects`, kèm lý do, `_batch_id` và `_row_no`, để truy lại nguồn và xử lý lại khi cần thay vì xoá mất bằng chứng.
Bước 2 đạt **7/7 checks passed**; `dbt test` đạt `PASS=6 WARN=1 ERROR=0 SKIP=0`. Đầu ra được lưu trong [`step2_telemetry.txt`](report/evidence/step2_telemetry.txt).

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Trong `dbt/models/schema.yml`, `max_gap_ms` kiểm tra khoảng cách giữa hai mẫu liên tiếp vượt 1.000 ms và được đặt rõ `severity: warn`: mỗi drive có một khoảng vượt ngưỡng, dài 3.073 ms ở A và 3.124 ms ở B; lab chấp nhận mất tín hiệu này nhưng cần cảnh báo để theo dõi và xử lý đồng bộ ở bước sau.
`in_range` giới hạn tốc độ trong 0–70 m/s và dùng mức mặc định `error`; tốc độ vô lý có thể làm sai dữ liệu huấn luyện nên phải chặn các task phía sau khi cổng kiểm tra thất bại.
Thí nghiệm nới luật phân loại lên 1.000 m/s nhưng giữ nguyên test 70 m/s: `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng**, cho kết quả `PASS=5 WARN=1 ERROR=1 SKIP=0`.
Kết quả cho thấy cổng chất lượng kiểm tra độc lập với luật làm sạch: dù luật bị nới sai, dữ liệu xấu vẫn bị phát hiện trước khi đi tiếp.
Sau khi khôi phục luật 70 m/s, test trở lại `PASS=6 WARN=1 ERROR=0 SKIP=0`; WARN về mất tín hiệu vẫn còn và không làm lần chạy thất bại.
Bằng chứng gắn với MSSV `2A202602615` được lưu tại [`break_gate.json`](report/evidence/break_gate.json), cùng [`step2_break_gate.txt`](report/evidence/step2_break_gate.txt) và [`step2_observations.txt`](report/evidence/step2_observations.txt).

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
