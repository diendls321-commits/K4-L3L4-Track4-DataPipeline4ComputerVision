# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Văn Diện |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602615` |
| GitHub | `diendls321-commits` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-02134` (lấy toàn bộ chữ số MSSV: `2202602615 % 26 = 5`, theo lựa chọn của sinh viên) |

## Kết quả cuối

- `dataset_version` của bạn: `b3bd5bc6347a` (giữ nguyên sau bài tập sập/chạy lại và lần chạy cuối `step all --reset`; khớp `report/evidence/run_summary.json`).
- Số khung hình train / val / test của bạn: **692 / 172 / 155** (tổng **1.019** khung, thuộc 26 trip).
- Kiểm tra cuối: **27/27 checks passed**, cả năm bước đều đạt; xem [`report/verify.txt`](report/verify.txt) và [`final_pipeline.txt`](report/evidence/final_pipeline.txt).
- Ảnh trong `report/screenshots/`: Graph lần chạy đầu `airflow_graph.png`, log lỗi `airflow_crash_log.jpg`, Graph khôi phục `airflow_recovered.jpg`, `minio_lake.jpg`, `metabase_dashboard.jpg` và `metabase_question.jpg`.

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

Thí nghiệm chạy lại `trip-00003` (60 khung) hai lần: toàn bảng `silver.frames` vẫn **1.246 dòng**, dấu vân tay trước/sau đều là **`6c7480dfde7e`**; `unchanged=true` trong [`idempotency.json`](report/evidence/idempotency.json).
Trong bảng thử riêng, ghi `append` hai lần tạo **120 dòng** từ 60 khung, tức nhân đôi dữ liệu; pipeline tránh việc này bằng cách thay thế đúng tập dòng có `trip_id = 'trip-00003'`.
Delta công bố việc thay thế qua một commit nguyên tử: người đọc thấy phiên bản cũ hoặc mới, tránh trạng thái đã xoá dữ liệu nhưng chưa ghi xong như cách xoá rồi ghi lại bằng hai thao tác.
Nếu lần ghi chưa commit thành công thì phiên bản bảng cũ vẫn đọc được; chạy lại với cùng predicate thay thế dữ liệu của trip đó, đồng thời giữ dữ liệu của các trip khác.
Lịch sử Delta ghi nhận hai lần `Overwrite` ở version 26 và 27, mỗi lần thêm 60 dòng và có đúng predicate trên. Trip là đơn vị xử lý/thay thế; partition vật lý của bảng thực tế là `drive_id`.
Dấu vân tay thí nghiệm kiểm tra ID khung/trip, độ sáng, độ nét và dHash; không so `_processed_at` vì thời điểm xử lý thay đổi. Tính nguyên tử nói trên áp dụng cho bảng Delta; upload các ảnh JPG là thao tác riêng.
Bước 3 đạt **6/6 checks passed**, `dbt test` đạt **8 PASS**, và cả **9 unit test** đều qua; log được lưu trong [`step3_frames.txt`](report/evidence/step3_frames.txt), [`step3_unit_tests.txt`](report/evidence/step3_unit_tests.txt) và [`step3_observations.txt`](report/evidence/step3_observations.txt).

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung **đen** có độ sáng trung bình dưới 12, gần như không có thông tin; pipeline gắn `is_black` để loại khỏi Gold. Khung **tối** có độ sáng từ 12 đến dưới 40, vẫn có thể chứa cảnh đường hầm/bóng râm nên gắn `is_dark` và không loại chỉ vì thiếu sáng.
Trong 602 khung của A có 6 đen, 9 mờ, 4 tối, 149 gần trùng; trong 644 khung của B có 4 đen, 0 mờ, 61 tối, 53 gần trùng. Các cờ có thể chồng lấp; mỗi drive có 7 khung mất đồng bộ.
Tôi đồng ý giữ các cảnh tối để dataset phản ánh điều kiện thực tế; giữ vì tối không có nghĩa bỏ qua cờ khác: A có 4/4 khung tối đủ điều kiện vào Gold, B có 58/61 khung tối đủ điều kiện.
Bảng trên lấy từ [`threshold.json`](report/evidence/threshold.json): dHash so với khung liền trước trong cùng trip; tăng ngưỡng làm nhiều khung bị coi là gần trùng hơn.
Tôi chọn ngưỡng **2 bit** đang dùng: ngưỡng 0 quá chặt với nhiễu nhỏ, còn ngưỡng 6 gắn cờ khoảng 60% khung A và 38% khung B, có nguy cơ bỏ nhiều thay đổi cảnh hữu ích; đây là lựa chọn cân bằng cần kiểm tra thêm bằng ảnh.
Ở tốc độ lấy mẫu 2 khung/giây, xe chạy chậm tạo nhiều khung gần giống nên dễ bị gắn cờ; khi xe chạy nhanh, cảnh đổi nhanh hơn và khoảng cách dHash có thể tăng. Không nên tăng ngưỡng chỉ vì xe nhanh; cần đánh giá cảnh và khoảng thời gian lấy mẫu để tránh loại nhầm.
Join ASOF theo event time cho độ trễ trung bình A/B **66/73 ms**; cực đại **2.884/3.112 ms** cho thấy các đoạn mất tín hiệu. Bằng chứng bổ sung nằm trong [`step3_threshold.txt`](report/evidence/step3_threshold.txt) và [`step3_observations.txt`](report/evidence/step3_observations.txt).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Thí nghiệm trên **1.019 khung đạt chất lượng, thuộc 26 trip**: chia theo khung hình làm **26/26 trip** nằm ở nhiều split, và cả **26 trip** đều có ảnh ở train lẫn test.
Chia theo trip như Gold cho **0 trip bị chia cắt**; mỗi drive có 9 trip train, 2 trip val và 2 trip test, tổng 18/4/4 trip.
Các khung gần nhau trong cùng chuyến có bối cảnh, góc camera và vật thể tương tự; chia chúng sang hai tập khiến mô hình có thể dựa vào cảnh đã thấy thay vì tổng quát hoá sang chuyến mới.
Vì vậy điểm test có thể cao giả tạo so với khả năng hoạt động trên cảnh mới; thí nghiệm này chứng minh rò rỉ trip, chưa đo mức chênh lệch điểm của một mô hình cụ thể.
Bằng chứng gắn với MSSV `2A202602615` nằm trong [`leakage.json`](report/evidence/leakage.json) và [`step4_leakage.txt`](report/evidence/step4_leakage.txt).

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Lần xuất đầu và lần chạy lại `export-dataset` đều cho **`b3bd5bc6347a`**, với train/val/test **692/172/155 ảnh**. Băm dựa trên nội dung xác định dataset giúp nhận diện cùng một kết quả dù chạy vào thời điểm khác; ngày giờ hay số đếm chỉ nhận diện lần chạy.
Cụ thể, mã hiện tại sắp xếp theo `frame_id`, ghép từng dòng `frame_id|split`, rồi lấy 12 ký tự đầu của SHA-256. Vì vậy version định danh tập khung và cách chia split, không phải băm toàn bộ byte ảnh, telemetry hay lineage.
MSSV được ghép với `trip_id` để tính MD5 xếp thứ tự trip trong từng drive, sau đó chia train/val/test; đổi MSSV có thể đổi trip thuộc từng split và số ảnh mỗi split dù tổng ảnh giữ nguyên.
Đổi tạm sang **`2A202602615-test`** cho version **`cafe62e1005c`**, số ảnh **693/175/151**; tổng vẫn **1.019 ảnh**, số trip vẫn **18/4/4**. Đây là tác động đã đo được của MSSV lên cách chia dataset.
Sau khi khôi phục nguyên vẹn `.env` và áp dụng lại Docker với **`2A202602615`**, chạy lại bước 4 trả về **`b3bd5bc6347a`**, **692/172/155 ảnh** và đạt **7/7 kiểm tra** như ban đầu.
Kết quả và log của lần thử/lần khôi phục được lưu tại [`step4_alternate_id.json`](report/evidence/step4_alternate_id.json), [`step4_restored.json`](report/evidence/step4_restored.json) và [`step4_repeat_export.txt`](report/evidence/step4_repeat_export.txt).

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest lưu ở [`step4_manifest.json`](report/evidence/step4_manifest.json) cho dataset **`b3bd5bc6347a`** gồm **436 ảnh drive A + 583 ảnh drive B = 1.019 ảnh**; lineage tại lần xuất bước 4 ghi các phiên bản bảng sau:

| Bảng đầu vào | Delta version |
|---|---:|
| `bronze.raw_telemetry_a` | 0 |
| `bronze.raw_telemetry_b` | 0 |
| `bronze.trips_raw` | 0 |
| `bronze.video_files` | 1 |
| `silver.frames` | 27 |
| `silver.telemetry` | 0 |
| `silver.frames_flagged` | 0 |
| `silver.frame_telemetry` | 0 |

SHA-256 video A: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; video B: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`.
Manifest ghi luật loại khung đen, mờ, gần trùng, mất đồng bộ; chia theo trip, phân tầng theo drive và thứ tự MD5. Dataset có train/val/test **692/172/155 ảnh**, **18/4/4 trip**.
Để tái tạo sau 3 tháng, cần giữ dữ liệu nguồn, các snapshot/log Delta tương ứng, manifest JSON/Parquet, MSSV **`2A202602615`**, phiên bản mã và cấu hình xử lý; lineage giúp chọn đúng đầu vào và SHA-256 giúp đối chiếu video.
Lineage hiện không tự lưu MSSV, phiên bản mã hay toàn bộ ngưỡng xử lý, nên cần lưu kèm báo cáo, cấu hình và commit Git. Chỉ giữ số version Delta sau khi reset/xoá lake thì không thể đọc lại snapshot cũ.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

MSSV chứa chữ nên bài dùng quy ước đã ghi ở đầu báo cáo: lấy các chữ số `2202602615`, chia dư 26 được **5**, chọn `trip-02134` trong danh sách đã sắp xếp.
Ở run `manual__dien_fail_pipeline_20261013T043615`, **1 task đỏ** là `silver_frames[5]`; **7 task `upstream_failed`** là hai cặp dbt run/test và ba task publish/export; **29 task** còn lại đã thành công. Log lần thử 1 ghi `simulated crash while extracting trip-02134; run it again`.
Clear riêng task đỏ làm `silver_frames[5]` thành công ở **lần thử 2**, nhưng 7 task đã mang trạng thái kết thúc `upstream_failed` vẫn bị chặn. Vì vậy phải clear thêm đúng 7 task này để chúng chạy lần đầu; không chạy lại 25 trip thành công, ba task Bronze hay `trip_commands`.
Kết quả là **37/37 task success**: chỉ **1 task thực sự chạy lại**, còn 7 task phía sau được thực thi lần đầu. Đây là khác biệt quan sát được so với hướng dẫn rút gọn “chỉ clear task đỏ thì DAG xanh”; hai thao tác clear đều có bằng chứng riêng.
Silver có **1.246 dòng / 1.246 frame_id khác nhau**, riêng trip lỗi có **60 khung**; Gold có **1.019 dòng / 1.019 frame_id khác nhau**, nên không nhân đôi. `dataset_version` vẫn **`b3bd5bc6347a`** như lần DAG ban đầu, và verify đạt **27/27**.
Bằng chứng: [`step5_airflow_failed.json`](report/evidence/step5_airflow_failed.json), [`step5_after_clear_one.json`](report/evidence/step5_after_clear_one.json), [`step5_clear_blocked_downstream.json`](report/evidence/step5_clear_blocked_downstream.json), [`step5_airflow_recovered.json`](report/evidence/step5_airflow_recovered.json) và [`step5_recovery_validation.json`](report/evidence/step5_recovery_validation.json).
Ảnh [`log lần thử 1 bị lỗi`](report/screenshots/airflow_crash_log.jpg) thấy đúng trip và ô đỏ của lần thử 1; ảnh [`Graph sau khôi phục`](report/screenshots/airflow_recovered.jpg) xanh toàn bộ. Sau thí nghiệm đã bỏ `LAB_FAIL_ONCE` khỏi `.env`, áp dụng lại Docker và chạy toàn bộ pipeline từ lake trống thành công.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba nút thắt dự kiến dưới đây được suy ra từ mã và cấu hình lab; chưa phải kết quả đo tải 500 xe/ngày. Cần biết thêm thời lượng video mỗi xe, số camera và hạn xử lý để tính công suất thực tế.

| Thành phần | Giới hạn thấy trong lab | Hướng nâng cấp |
|---|---|---|
| Giải mã video / Airflow worker | `LocalExecutor`, tổng parallelism 2, riêng `silver_frames` chỉ cho một task chạy mỗi DAG run; mỗi trip tải lại cả video drive rồi dùng FFmpeg lấy 2 fps. CPU, tải lặp và hàng đợi sẽ tăng nhanh. | Tách worker giải mã theo trip/đoạn video trên nhiều máy, cache hoặc đọc đoạn cần thiết; mở rộng worker theo hàng đợi và quản lý số lần ghi đồng thời vào Delta. Airflow tiếp tục điều phối. |
| Biến đổi và join / DuckDB | Một file DuckDB, dbt chạy một thread, các bảng và mart được dựng lại; CPU, RAM và đường ghi của một máy giới hạn lượng dữ liệu xử lý trong ngày. | Xử lý tăng dần theo ngày/xe/trip; khi đo tải vượt khả năng một máy, dùng Spark với Delta cho biến đổi/join phân tán, giữ nguyên các quality gate và khoá chống trùng. |
| Lưu trữ và phục vụ dữ liệu / MinIO–Postgres | MinIO một máy, nhiều JPG nhỏ; Gold sao chép từng ảnh; sáu mart được thay toàn bộ trong Postgres giới hạn 512 MB, dùng chung máy với Airflow/Metabase. I/O, dung lượng và truy vấn BI cạnh tranh tài nguyên. | Dùng object storage có khả năng mở rộng, phân vùng và chính sách vòng đời; cân nhắc đóng gói ảnh theo shard cho huấn luyện. Tách warehouse khỏi metadata Airflow/Metabase, nạp tăng dần và phục vụ BI từ bảng tổng hợp. |

Cách phân tích nối kiến trúc tổng thể với lựa chọn engine và mở rộng xử lý ở slide 12, 22, 23: đo CPU/RAM/I/O, độ dài hàng đợi và thời gian từng bước trước khi quyết định thay thành phần nào. Dù mở rộng, vẫn phải giữ xử lý idempotent theo trip, lineage và split theo trip.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**Câu hỏi:** Những trip nào có tỷ lệ khung hình bị loại cao nhất, và chúng có tốc độ trung bình/điều kiện ghi hình thế nào?

Đã lưu câu hỏi [“Trips: tỷ lệ khung bị loại cao nhất”](http://localhost:3000/question/46) và thêm vào [dashboard Day 9](http://localhost:3000/dashboard/2):

```sql
SELECT drive_id, trip_id, frames, frames_kept,
       frames - frames_kept AS frames_dropped,
       ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2) AS drop_pct,
       avg_speed_kmh, lighting, conditions
FROM marts.mart_trip_summary
ORDER BY drop_pct DESC, trip_id
LIMIT 10;
```

Kết quả cao nhất là **`drive_a / trip-04622`: 60 khung, giữ 26, loại 34 (56,67%)**, tốc độ trung bình **67,1 km/h**; tiếp theo là `trip-03904` **50%** và `trip-02086` **46,67%**.
Tỷ lệ này đếm số khung thực sự không vào Gold, tránh cộng chồng các cờ lỗi. Nó giúp chọn trip cần xem ảnh/log trước; riêng tốc độ và nhãn Day/Clouds chưa đủ kết luận nguyên nhân bị loại.
SQL và 10 dòng kết quả được lưu trong [`step5_metabase_question.json`](report/evidence/step5_metabase_question.json); xem ảnh [`dashboard`](report/screenshots/metabase_dashboard.jpg) và [`câu hỏi riêng`](report/screenshots/metabase_question.jpg).

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điểm đáng chú ý là khung tối không đồng nghĩa với khung hỏng: B có 61 khung tối nhưng 58 khung vẫn đủ điều kiện vào Gold.
Bài tập Airflow cho thấy cần phân biệt task đã chạy lỗi với task chưa được chạy do `upstream_failed`; clear task lỗi chưa tự khôi phục mọi trạng thái kết thúc phía sau trong lần thử này.
Ngưỡng dHash 2 bit hiện là lựa chọn hợp lý cho lab, nhưng vẫn cần kiểm tra ảnh theo tốc độ xe/cảnh để biết có bỏ nhầm thay đổi quan trọng hay không.
Pipeline còn thiếu kiểm tra GPS nhảy bất thường hoặc toạ độ ngoài phạm vi hợp lệ: tốc độ hợp lệ và timestamp đúng chưa đảm bảo vị trí xe đúng.
Có thể bổ sung kiểm tra phạm vi lat/lon và tính nhất quán giữa quãng đường GPS, thời gian và tốc độ; các dòng nghi ngờ cần được gắn cờ/cách ly để xem lại.
