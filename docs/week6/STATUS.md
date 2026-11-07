# Trạng thái 10 task của tuần 6

**Phân biệt:** (a) code có file, (b) test unit/local, (c) cluster integration, (d) handover được người khác ký. Hạng mục (c,d) **chưa** được thực hiện. G5 hiện **INCONCLUSIVE**; G3/G4 cũng chưa có bằng chứng PASS thực tế.

| Task | Đã phát triển | Kiểm thử local | Để nghiệm thu thật |
|---|---|---|---|
| P1-W6A | reproduction isolated copy / optional clean Git clone, quickstart, negative config | checkout copy, 3 fixture commands | **P5/người ngoài tác giả** checkout commit sạch, bootstrap→batch→stream→validator→UI thật, ghi blocker và log |
| P1-W6B | source release manifest, audit secret, architecture/doc/demo | checksum/tamper/unit tests | 5 người diễn tập, architecture đối chiếu container thực, đóng blocker và freeze commit |
| P2-W6A | data dictionary + source→Silver→Gold trace validator | source locator và count/sum Gold fixture | P3/P5 ingest Iceberg fixture độc lập, SQL trace, schema snapshot + HDFS URI |
| P2-W6B | batch demo/compaction runbook, evidence links | W3 Gold + W4/W5 previous fixture checks | Stage overlap/compaction thật trước/sau, manifest ID/sum, scale + limitations |
| P3-W6A | model card/manifest validation/report references | reject invalid threshold/order/checksum | P1 xem card số AP/PR/Recall/threshold linked to pinned measured report và artifact HDFS |
| P3-W6B | independent Spark reload comparator for A/A+H | syntactic import and negative metadata audit only | **P4** spark-submit process mới, score equality/restart, verify model artifact hash |
| P4-W6A | fresh/resume guard, checkpoint/startup runbook | missing config, collision, warm-up, invalid resume | P1 test Kafka/Mongo/HDFS in clean env; restart, quarantine, reconcile |
| P4-W6B | release golden/restart demo, limitations, operator fallback | previous local W3/W4 parity | P5 verify live prediction alert across Kafka→Mongo→API, trace evidence, rehearse backup operator |
| P5-W6A | package audit, isolated fixture reproduction, G5 checker | negative tests + file tamper | P2 independently checkout/validate/record genuine run (not local copy) |
| P5-W6B | claim→report hash/value checker, G5 fail-closed, 30-case checklist, demo docs | reports reject wrong unit/model/version/fixture; G5 rejects fake proof | P1 signs verified proof, all five signoffs, independently measured p95/recovery/parity claims |

## Môi trường tạo patch

- Ghép W1→W6: `287 passed, 3 skipped` trong Python local. Skip là các ca cần môi trường Spark/integration mà container không có.
- `scripts.week6.cli fixture-reproduce`: stateful W3, parity W4, evaluator W5 lần lượt exit 0; **filesystem_copy_not_checkout / fixture_only=true**.
- Không truy cập được source repo Git thực của bạn, HDFS/Kafka/Mongo/Spark cluster, và không biết secret/URI ngoài ảnh. Không tạo số liệu hiệu năng thực, không ký thay P1–P5, không tuyên bố G5 PASS.

## Blocker để đóng G5

1. Chạy G3 integration thật của tuần 4; G4 benchmark/fault thật của tuần 5 với 3 repeats và correctness PASS.
2. Merge W1–W6 vào repo thật, freeze commit, xác nhận source checksum + schema + model artifact, environment dependency (Spark, Iceberg, Kafka, HDFS, Mongo).
3. Người khác tác giả chạy clean Git checkout independent theo quickstart, có exact commands stdout và API/UI/validator report; thử config thiếu và restart.
4. Mọi số liệu có claim→report hash và pinned run/snapshot/commit, không dùng synthetic; 30 ca checklist và 5 ký tên thật. Sau đó mới chạy G5 gate và xét release.
