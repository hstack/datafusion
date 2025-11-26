CREATE OR REPLACE TABLE values(
  _acp_system_metadata STRUCT(
    acp_sourceBatchId VARCHAR, 
    commitBatchId VARCHAR
  ), 
  _id varchar, 
  productListItems STRUCT(
    SKU VARCHAR, 
    quantity INT4, 
    priceTotal FLOAT, 
    _experience STRUCT(
      analytics STRUCT(
        customDimensions STRUCT(
          eVars STRUCT(
            evar1 VARCHAR, 
            evar2 VARCHAR
          )
        ), 
        events STRUCT(
          event1 STRUCT(
            value FLOAT
          ), 
          event2 STRUCT(
            value FLOAT
          )
        )
      )
    )
  )[]
);

INSERT INTO values VALUES
(
  {
    acp_sourceBatchId: 'b1', 
    commitBatchId: 'b1', 
  }, 
  'id1', 
  [
    {
      SKU: 'sku1', quantity: 1, priceTotal: 100, 
      _experience: {
        analytics: {
          customDimensions: {
            eVars: { evar1: 'ev1', evar2: 'ev2' }
          }, 
          events: {
            event1: { value: 1.5  }, 
            event2: { value: 3.2  }
          }
        }
      }
    }
  ]
);

COPY values TO 'values.parquet' (FORMAT PARQUET);

