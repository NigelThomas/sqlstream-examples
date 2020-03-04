-- To demonstrate filtering out "too late" and "too early" records

CREATE OR REPLACE SCHEMA "demos";
CREATE OR REPLACE STREAM "demos"."OrderData" (
    "order_time"   TIMESTAMP,
    "key_order"    BIGINT NOT NULL,    
    "key_user"     BIGINT,    
    "country"      SMALLINT,    
    "key_product"  INTEGER,    
    "quantity"     SMALLINT,    
    "eur"          DECIMAL(19,5),    
    "usd"          DECIMAL(19,5)
);

-- This doesn't promote ORDER_DATE to ROWTIME - which is compulsory
-- Error: From line 4, column 10 to line 4, column 51: For t-Sort, leading order item must be aliased as ROWTIME in the select list (state=,code=0)
-- Too early to do the WITHIN INTERVAL? which should drop too-late rows

-- CREATE OR REPLACE VIEW "demos"."OD_STREAM_VIEW_001" AS
-- SELECT STREAM * 
-- FROM "demos"."OrderData" OD
-- ORDER BY OD."order_time" WITHIN INTERVAL '5' MINUTE;

-- This creates a maxOrderTime including all rows (including too-late and too-early)

CREATE OR REPLACE VIEW "demos"."OD_W_PRIOR_ORDERTIME" AS
SELECT STREAM 
  nth_value("order_time", 2) FROM LAST OVER (ROWS 1 PRECEDING) AS "prior_row_time",
  * 
FROM "demos"."OrderData" AS OD;

-- Here we use a simple comparison with the prior row, allowing ORDER_TIME to go up by 5 minutes max for each row
-- NOTE this is NOT quite the same as using a high water mark + interval
-- We could instead compare with a rolling average (to reduce the effect of outliers, and to give more flexibility when the time saw-tooths)

CREATE OR REPLACE VIEW "demos"."OD_FUTURE_OUTLIERS_FLAGGED" AS
SELECT STREAM 
    CASE WHEN "order_time" > "prior_row_time" + INTERVAL '5' MINUTE THEN true ELSE false END as "too-early"
  , * 
FROM "demos"."OD_W_PRIOR_ORDERTIME" AS OD;

-- exclude the big jumpers
CREATE OR REPLACE VIEW "demos"."OD_NO_FUTURE_OUTLIERS" AS
SELECT STREAM 
    * 
FROM "demos"."OD_FUTURE_OUTLIERS_FLAGGED" AS OD
WHERE NOT "too-early";

CREATE OR REPLACE VIEW "demos"."OD_NO_FUTURE_WITH_HWM" AS
SELECT STREAM 
    MAX("order_time") OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS "maxOrderTime"
  , * 
FROM "demos"."OD_NO_FUTURE_OUTLIERS" AS OD
;

--
-- Now classify the not-too-early rows in two: OK and too-late

CREATE OR REPLACE VIEW "demos"."OD_NOT_TOO_EARLY" AS
SELECT STREAM *
     , ("order_time" + INTERVAL '5' MINUTE < "maxOrderTime") AS "too-late"
FROM   "demos"."OD_NO_FUTURE_WITH_HWM" 
;

-- So we can flow the bad rows WHERE "too-late" 

-- And the ones we should accept:
-- This finally promotes the rowtime, t-sorts and also filters out too-late rows 

CREATE OR REPLACE VIEW "demos"."ORDERDATA_PROMOTED" AS
SELECT STREAM "order_time" as ROWTIME, * 
FROM "demos"."OD_NOT_TOO_EARLY" OD
WHERE NOT "too-late"
ORDER BY OD."order_time" WITHIN INTERVAL '5' MINUTE ;
