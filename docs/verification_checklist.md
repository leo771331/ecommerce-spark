# 검증 체크리스트

| 항목 | 결과 |
|---|---|
| sbt compile | 성공 |
| sbt package | 성공 |
| 샘플 CSV 생성 | 성공 |
| 샘플 Spark 실행 | 성공 |
| UTC → KST 변환 검증 | 성공 |
| KST 기준 event_date_kst 생성 검증 | 성공 |
| 299초 gap 같은 세션 처리 | 성공 |
| 300초 gap 새 세션 처리 | 성공 |
| Parquet Snappy 저장 | 성공 |
| staging / backup / job_status commit | 성공 |
| Kaggle Oct/Nov CSV 다운로드 | 성공 |
| 전체 Oct/Nov Spark 처리 | 성공 |
| Hive External Table 생성 | 성공 |
| partition check | 성공 |
| user_id 기준 WAU 계산 | 성공 |
| generated_session_id 기준 WAU 계산 | 성공 |
