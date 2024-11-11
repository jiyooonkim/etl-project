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
1. 테이블생성 :  .sql 파일 읽기, CREATE EXTERNAL TABLE IF NOT EXISTS....
2. hivewearhouse write(snappy.parquet)
3. delta 에도 동일하게 write
4. 배치 장애시 복구 - delta로 hivewearhouse 백업 

### 제약사항 
+ Hive Transaction 제약 사항
  + ORC 파일 포맷만 사용 가능 
  + Transaction (transactional=true)을 지원하는 테이블은 LOAD DATA 쿼리를 사용할 수 없음 
  + External Table의 경우 ACID 테이블을 만들 수 없음, transaction 또한 지원하지 않음
    + 해결방안 : spark 로 transaction 처리, 최종 데이터 테이블 따로 생성하고 external table 처리 
+ Delta 복구
+ 추가 기간 처리 대응
  + 기본 파일 쓰기는 append, 중복 데이터는 overwrite 옵션 활성화 ??  


 



