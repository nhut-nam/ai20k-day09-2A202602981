# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Trần Nhựt Nam |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602981 |
| GitHub | nhut-nam |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bronze ghi 3.021 dòng telemetry A, 3.234 dòng B, 26 trip và đúng 2 video. A có `ts_utc` dạng ISO UTC, `speed_mps`, `heading_deg`, `gas` và `brake` dạng tỉ lệ; B có `timestamp_ms` dạng epoch mili giây, `spd_kmh`, `hdg`, `throttle_pct` và `brake_pct` dạng phần trăm. B còn khai báo đồng hồ nhanh hơn video 1.500 ms. Giữ hai bảng riêng và mọi giá trị gốc dạng chữ giúp bảo toàn dữ liệu, truy vết schema của từng nguồn và cách ly lỗi ép kiểu ở Silver. Chạy lại ingest A báo `already present`, số dòng không tăng.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_a` ép `ts_utc` sang timestamp UTC, giữ tốc độ m/s. `stg_telemetry_b` đổi `timestamp_ms` từ epoch mili giây sang UTC sau khi trừ 1.500 ms theo `bronze.video_files`, đổi km/h sang m/s và bàn đạp từ phần trăm sang tỉ lệ. Sau chuẩn hóa, B bắt đầu lúc 2025-10-08 14:30:00.009 UTC, lệch video chỉ 9 ms; tốc độ tối đa A/B lần lượt 71/100 km/h. Quarantine ở **mỗi** drive: 20 `duplicate`, 30 `speed_missing`, 3 `speed_out_of_range` (tổng 106). Dòng sạch A/B là 2.968/3.181; sạch cộng cách ly khớp đúng 3.021/3.234 dòng gốc. Giữ rejects giúp kiểm toán và sửa nguồn, không làm mất dấu dữ liệu đầu vào.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` báo khoảng mất tín hiệu cảm biến đã biết; đây là sự cố cần quan sát nhưng không nhất thiết làm hỏng toàn bộ telemetry. `in_range` kiểm tra tốc độ 0–70 m/s, nên giá trị vượt ngưỡng sẽ chặn dữ liệu đi tiếp. Bình thường dbt test có PASS=6, WARN=1, ERROR=0. Khi `break-gate` nới luật làm sạch lên 1.000 m/s, test `in_range_silver_telemetry_speed_mps__70__0` bắt 6 dòng: PASS=5, WARN=1, ERROR=1. Sau khôi phục luật gốc lại PASS=6, WARN=1, ERROR=0; cổng chất lượng phát hiện dữ liệu vô lý dù bước làm sạch bị nới sai.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Với trip-00003, trước và sau hai lần chạy lại bảng Silver đều có 1.246 dòng; dấu vân tay đều là `6c7480dfde7e`. Nếu append hai lần, riêng trip có 60 khung thành 120 dòng. Delta `overwrite` với predicate `trip_id` tạo một commit thay thế đúng phần dữ liệu của trip; nếu thao tác lỗi trước commit, bảng cũ vẫn đọc được. Xoá rồi ghi là hai thao tác riêng, nên lỗi giữa chừng có thể để lại trip rỗng hoặc thiếu khung.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen dưới ngưỡng sáng 12 thường là hỏng tín hiệu: A có 6, B có 4 và Gold loại bỏ. Khung tối (12–40) vẫn có nội dung đường hầm/bóng râm: A có 4, B có 61, được gắn cờ nhưng giữ nếu qua các điều kiện khác. Mình đồng ý giữ khung tối vì điều kiện thiếu sáng là tình huống thật cần học. Chọn ngưỡng dHash 2 như pipeline: ngưỡng 6 loại quá nhiều khung có thể khác nhau thật, còn 0 bỏ sót các bản gần trùng. Xe chạy chậm làm các khung liên tiếp giống nhau hơn; vì vậy cần xem ngưỡng cùng tốc độ và kiểm tra ảnh mẫu, không chỉ nhìn tổng số.

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
