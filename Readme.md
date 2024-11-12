### 아래 사용자 activity 로그를 Hive table 로 제공하기 위한 Spark Application 을 작성하세요
#### version : java ,spark, hadoop, hive, [delta](https://docs.delta.io/latest/releases.html)



### 요구사항
- KST 기준 daily partition 처리
- 재처리 후 parquet, snappy 처리
- External Table 방식으로 설계
- 추가 기간 처리에 대응 가능 하도록 구현
- 배치 장애시 복구를 위한 장치 구현
+ 2019-Nov.csv,  2019-Oct.csv
+ Spark Application 구현에 사용가능한 언어는 **Scala나 Java**로 제한합니다.
+ File Path를 포함하여 제출시 특정할 수 없는 정보는 모두 임의로 작성해주세요.

### 시나리오
1. 테이블생성 : .sql 파일 읽기, CREATE EXTERNAL TABLE IF NOT EXISTS....
2. hivewearhouse write(snappy.parquet)
3. delta 에도 동일하게 write - 최신 상태 유지 merge into(upsert)
4. 배치 장애시 복구 - delta로 hivewearhouse 백업 

### 이슈 사항  
+ Hive Transaction 제약 사항
  + ORC 파일 포맷만 사용 가능 
  + Transaction (transactional=true)을 지원하는 테이블은 LOAD DATA 쿼리를 사용할 수 없음 
  + External Table의 경우 ACID 테이블을 만들 수 없음, transaction 또한 지원하지 않음
    + 해결방안 : spark 로 transaction 처리, 최종 데이터 테이블 따로 생성하고 external table 처리 
+ 장애시 delta 로 복구 
  + delta 에 upsert 사용하고자 했으나  **You can preprocess the source table to eliminate the possibility of multiple matches.** 에러 발생 
  + 원인 :소스 데이터 세트의 여러 행이 일치하고 병합이 대상 델타 테이블의 동일한 행을 업데이트하려고 시도하는 경우 작업 merge이 실패할 수 있습니다. 병합의 SQL 의미론에 따르면 이러한 업데이트 작업은 일치하는 대상 행을 업데이트하는 데 어떤 소스 행을 사용해야 하는지 명확하지 않으므로 모호합니다. 소스 테이블을 사전 처리하여 여러 일치 가능성을 제거할 수 있습니다.
  + 해결방법 : 기존에 없는 행 insert 이후, 있는 행은 update 
+ 추가 기간 처리 대응
  + 기본 파일 쓰기는 append, 중복 데이터는 overwrite 옵션 활성화 ??  




 



