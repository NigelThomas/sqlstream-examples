# late-early-rows - handling early and late rows in a streaming pipeline

It is very common that we are processing incoming data which includes an event time.

We want to promote that event time to ROWTIME (so that we can perform time-based analytics with respect to the event time) but:

* the data may not be perfectly monotonic - so we need to execute a partial time sort (T-sort) using `ORDER BY "event_time"`
* we don't want to wait too long for late rows - so we define an interval such as `WITHIN INTERVAL '1' MINUTE`
* we want to filter out anomalous rows that are "too far in the future" so that errors in event time do not poison the T-sort.

## Records that are too late

These are easy to detect. The T-sort rejects any records where ROWTIME + <within interval> < <high watermark time>'. 

If we want to catch those records before they are rejected (to divert them to 'late row processing') we can:

* calculate the running high water mark excluding the current row

```
   MAX("event_time") OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS "max_event_time"
```

* compare the current "event_time" (remember, we haven't promoted "event_time" to ROWTIME yet) to identify rejected rows:

```
   ("event_time" + INTERVAL '1' MINUTE < COALESCE("max_event_time", "event_time") AS "rejected"
```
  Note we need to deal with the special case of the first row in the stream, when "max_event_time" will be NULL.

* create two views on the result, one `WHERE "rejected"` and one `WHERE NOT "rejected"

* Now we can send rejected (too late) rows to a retry path (for example, merging updates into a database) and send the accepted records (not too late) into the next
stage of the 'happy path'.


## Records that are too early (too far in the stream's future)

If there is a risk of data errors in the "event_time", it is easy for new rows to poison a T-sort. If a row arrives that is incorrectly timed too far in the future (perhaps the timestamp is in the wrong TZ) it pushes the 
high watermark of a T-sort forwards much further than expected. The result is that following rows are all rejected.

There are ways to deal with this:

### Set some limit compared to wallclock time (recall that this is generally UTC).

To allow records that are no more than 5 minutes into the future:
```
   WHERE "event_time" < CURRENT_ROW_TIMESTAMP + INTERVAL '5' MINUTES
```
This works well for realtime systems; just be aware that 
* when replaying test data you will be comparing a past time with now. Good data will still pass the test, but now so may some unexpected bad data.
* as a result, repeated test runs are not guaranteed to produce exactly the same results. A row rejected in run 1 may be accepted in run 2 a minute later.

### Set some limit to the step from one row to the next

The idea is to reject a row if it increments the time by too much compared to the prior row.

### Use a UDX to eliminate too-early rows

In a UDX it is very easy to identify problem rows; in the class set up the following:
```
    long hwmTime = 0;
    long limitInterval = 300000; // 5 minutes in msecs - this may be set in the UDX constructor
    long hwmPlusInterval = 0;
    long hwmMinusInterval = 0;
    int eventTimeColIdx = 0;    // set this in the constructor to the column index of the event_time column
```

Now in the main UDX event loop, when the row is read:

```
    long eventTime = in.getTimestamp(eventTimeColIdx).getTime();

    if (hwmTime > 0 && eventTime > hwmPlusInterval) {
        // the event is too early / too far in the future so ignore it
        tracer.warning("Row is too early");
    } elif (hwmTime > 0 && eventTime < hwmMinusInterval )
        // the event is too late
        tracer.warning("Row is too late");
    } else {
        // the event is acceptable - either it's the very first event, or it's within range

        if (eventTime > hwmTime) {
            // this is a new HWM, set thresholds
            hwmTime = eventTime
            hwmPlusInterval = eventTime + limitInterval;
            hwmMinusInterval = eventTime - limitInterval;
        }

        // copy input row to output row
        transferColumns();

        // emit the row
        out.executeUpdate();

    }
```

It is easy to modify this to allow the three way split of early, OK and late rows so that the data can be forked into 3:

```
    Timestamp eventTime_ts = in.getTimestamp(eventTimeColIdx)
    long eventTime = eventTime_ts.getTime();

    if (hwmTime > 0 && eventTime > hwmPlusInterval) {
        // the event is too early / too far in the future so ignore it
        result = "EARLY";
    } elif (hwmTime > 0 && eventTime < hwmMinusInterval )
        // the event is too late
        tracer.warning("Row is too late");
        result = "LATE";
    } else {
        // the event is acceptable - either it's the very first event, or it's within range
        RESULT = "OK"

        if (eventTime > hwmTime) {
            // this is a new HWM, set thresholds
            hwmTime_ts = eventTime_ts;
            hwmTime = eventTime;

            hwmPlusInterval = eventTime + limitInterval;
            hwmMinusInterval = eventTime - limitInterval;
        }
    }

    // copy input row to output row
    transferColumns();

    out.setString(resultColumnIdx, result);
    out.setTimestamp(hwmColumnIdx, hwmTime_ts);

    // emit the row
    out.executeUpdate();

```

