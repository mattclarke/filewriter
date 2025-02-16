# filewriter
Something like the ECDC filewriter but in Rust.

## Design
- Top-level command listener
- On start message, it spawns a job thread
- For each topic it creates a thread that listens to the topic and processes each message
- If the top-level command listener gets a stop message then it passes that to the job thread
- At the top-level, we send status updates.

## Assumptions
- values are written to Kafka with a key, so each value is kept in order
  - if this is not the case, then we need to keep the writers alive until we see a time past the
  stop time + a leeway. non-changing values are written more frequently than every 30 seconds
- if no update during run, then buffered value is written
- each topic+source+schema combination is unique (i.e. we are not trying to write the same data twice in two locations without using links)
