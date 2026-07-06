CREATE OR REPLACE TABLE midvalues(
  timestamp TIMESTAMP_S,  -- 0
  web STRUCT(
    webPageDetails STRUCT(
      pageViews STRUCT(value INT8)
    )
  ), 
  endUserIDs STRUCT(
    _experience STRUCT(
      mcid STRUCT(
        id VARCHAR, 
        extra1 VARCHAR
      ),
      aaid STRUCT(
        id VARCHAR, 
        extra1 VARCHAR
      )
    )
  )
);

INSERT INTO midvalues VALUES
(
  '2025-01-15 00:00:01',
  {
    webPageDetails : {
      pageViews : {
        value : 100
      }
    }
  }, 
  {
    _experience : {
      mcid : {
        id : 'mcid1', 
        extra1 : 'extram1'
      }, 
      aaid : {
        id : 'mcid1', 
        extra1 : 'extram1'
      }
    }
  }
);

CREATE OR REPLACE TABLE extra_user_data(
  endUserIDs STRUCT(
    _experience STRUCT(
      mcid STRUCT(
        id VARCHAR, 
        name VARCHAR
      ),
      aaid STRUCT(
        id VARCHAR, 
        name VARCHAR
      )
    )
  )
);

INSERT INTO extra_user_data VALUES
(
  {
    _experience : {
      mcid : {
        id : 'mcid1', 
        name : 'name1'
      }, 
      aaid : {
        id : 'mcid1', 
        name : 'name2'
      }
    }
  }
);

COPY midvalues TO 'midvalues.parquet' (FORMAT PARQUET);
COPY extra_user_data TO 'extra_user_data.parquet' (FORMAT PARQUET);

