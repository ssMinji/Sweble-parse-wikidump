# Wiki dump Pagerank 계산

**사용 데이터**
- 2019-01-01 날짜의 wikidump 데이터(https://dumps.wikimedia.org/kowiki/20190101/)
- kowiki-20190101-pages-meta-current.xml
- kowiki-20190101-pagelinks.sql  --> pagelinks.tsv로 변환 후 저장
- kowiki-20190101-redirect.sql --> redirect.tsv로 변환 후 저장

**Core Package**
1. HDFS
2. YARN

## Step01. Data cleansing
- Xml Parsing : <page></page> 단위로 잘라서 고유한 id와 그에 해당하는 title만을 가져옴
- Tsv 정제 : pagelinks.tsv와 redirect.tsv에는 문서의 성격을 나타내는 nameSpace 컬럼이 따로 존재.(예 : nameSpace가 1= 틀:, 2=사용자:) 
- tsv문서와 join하게 될 xml문서에는 title에  nameSpace에 해당하는 타이틀이 붙어있으므로 tsv에서 각 nameSpace에 해당하는 이름을 붙여 완벽한 title로 만들어줌


## Step02. Redirect Remove
- wikipedia에는 검색 시 바로 어떤 문서로 redirect되는 문서들이 존재함
- 이렇게 redirect되는 문서들에는 참조되는 문서나 링크를 가지고 있는 문서가 없다고 판단하여 redirect 되는 문서들을 제거
- Mapreduce를 이용해 redirect 문서의 id와 title이 일치하는 경우 데이터 제거


## Step03. Mapreduce Join
- 앞의 두 단계를 통해 데이터 정제 완료
- 정제된 pagelinks.tsv와 meta.xml을 from_id 와  to_id 컬럼으로 조인
- 각 타이틀에는 고유한 id가 있으므로 타이틀은 제외하고 id 컬럼만 남김
- 따라서 어떠한 to_id와 그 아이디를 참조하고 있는 from_id로 나타낼 수 있음
- 본격적인 pagerank 계산 전, 각 문서의 pagerank 초기값을 1.0으로 설정해줌

## Step04. Pagerank 계산
- RankCalculateMapper : 각 문서에 대응하는 원본 문서와 원본 문서가 가진 링크의 총 갯수 계산
- RankCalculateReducer : key로 원본 문서를 받아옴. pagerank 구하는 공식(damping * sumShareOtherPageRanks + (1 - damping), 이때 damping=0.85)를 사용해 pagerank계산
- RankOrdering : Mapreduce는 key 기준으로 오름차순 정렬. 따라서 key 기준 내림차순 정렬로 나타냄


## Step05. Pagerank 계산 실행
- run 메소드에서 for문을 10번 실행시킴으로써 pagerank 계산식 총 10번 반복(recursion)
- 이 때, 최초의 iter00 데이터는 Step03에서 실행한 join데이터가 될 것이므로 Step03을 실행할 때 결과물의 디렉터리명을 iter00으로 설정해야 함
- for문을 실행하며 iter01~iter10의 output디렉터리가 생성되며 마지막으로 계산된 iter10의 값을  ordering한 결과물은 result 디렉터리에 저장됨 


