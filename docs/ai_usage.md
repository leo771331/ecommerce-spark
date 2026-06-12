# AI 활용 내역

## 1. 사용한 AI 도구

ChatGPT를 사용했습니다.

## 2. AI 활용 범위

AI는 다음 영역에서 보조적으로 활용했습니다.

- 요구사항 분해
- Spark batch 설계 방향 검토
- Scala/Spark 코드 구조 리뷰
- 오류 로그 해석
- Git commit 계획 수립
- README 및 문서 초안 작성 보조

## 3. 직접 수행한 범위

다음 작업은 직접 수행하고 결과를 검증했습니다.

- WSL 개발 환경 구성
- GitHub repository 생성 및 push
- Scala Spark Application 작성 및 수정
- sbt compile/package 실행
- 샘플 데이터 실행 검증
- Kaggle 데이터 다운로드
- 원본 CSV의 event_time, user_id null 없음 확인
- 전체 Oct/Nov 데이터 처리
- Hive External Table 생성
- partition check 실행
- WAU 쿼리 실행 및 결과 확인

## 4. 프롬프트 전략

한 번에 전체 구현을 생성하지 않고, 작은 commit 단위로 나누어 진행했습니다.

각 단계마다 다음 순서로 진행했습니다.

1. 이번 단계의 목표 확인
2. 수정할 파일 확인
3. 코드 작성 또는 수정
4. 로컬 검증
5. commit
6. 다음 단계 진행

제출 코드는 면접에서 라인 단위로 설명할 수 있도록 직접 확인하면서 작성했습니다.
