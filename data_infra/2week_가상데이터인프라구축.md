배달앱을 운영한다고 가정했습니다.

# raw data
### (1) 주문 테이블
- **주문ID**
- 고객ID
- 주문일시(년/월/일/요일/시간)
- 매장ID
- 메뉴ID
- 포장여부
### (2) 매장 테이블
- **매장ID**
- 매장 이름
- 매장 카테고리 (패스트푸드, 카페 등)
- 메뉴ID
### (3) 메뉴 테이블
- **메뉴ID**
- 메뉴이름
- 메뉴 카테고리 (치킨, 햄버거, 커피, 케이크 등)
- 맛 카테고리 (매움, 달콤, 느끼함, 얼큰함 등)
- 1인 메뉴 여부 (TRUE 또는 FALSE)
### (4) 고객 테이블
- **고객ID**
- 연령
- 성별
- 가입날짜
- ...

# analytics
### 1. 요일/시간대별 주문이 들어온 메뉴 카테고리와 메뉴 맛을 나타내는 테이블
> 사람들이 요일/시간대별로 어떤 메뉴 카테고리와 맛을 선호하는지 파악하기 위한 용도. 
> 금요일에 치킨을 많이 먹는다면, 금요일 치맥이벤트 등을 기획해볼 수 있음.
> 오전에 얼큰함 맛을 많이 먹는다면, 주문이 상대적으로 적은 오전에 얼큰한 메뉴 중심으로 추천 및 이벤트 가능.
- (1), (3) 테이블 사용
### 2. 연령별 1인분 주문 건수 테이블
> 1인분 메뉴 수요 패턴을 파악하기 위한 용도.
- (1), (3), (4) 테이블 사용
### 3. 신규고객의 첫 주문 테이블
> 첫 주문 이벤트의 패턴을 파악하기 위한 용도. 일정한 패턴을 발견하면 이를 활용하여 신규고객 유입방안 모색 가능.
- (1), (4) 테이블 사용
