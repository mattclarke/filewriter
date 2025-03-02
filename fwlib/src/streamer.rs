use std::{cell::RefCell, collections::HashMap, time::SystemTime};

trait Time {
    fn now(&self) -> SystemTime;
}

struct StubWallClock {
    times: Vec<SystemTime>,
    index: RefCell<usize>,
}

impl Time for StubWallClock {
    fn now(&self) -> SystemTime {
        let mut index = self.index.borrow_mut();
        if *index >= self.times.len() {
            return *self.times.last().unwrap();
        }
        let result = self.times.get(*index);
        *index += 1;
        *result.unwrap()
    }
}

trait Writer {
    fn is_finished(&self) -> bool;
}

struct Streamer {}

impl Streamer {
    fn process<T: Time>(
        &self,
        source: &mut StubSource,
        writers: &mut HashMap<String, Box<dyn Writer>>,
        start_time: &SystemTime,
        stop_time: Option<SystemTime>,
        wall_clock: &T,
    ) -> bool {
        let Some(message) = source.poll() else {
            if stop_time.is_some() {
                return wall_clock.now() > stop_time.unwrap();
            }
            return false;
        };

        if let Some(writer) = writers.get_mut(&message) {
            let finished = writer.is_finished();
            if finished {
                writers.remove(&message);
            }
        }

        writers.len() == 0
    }
}

struct StubSource {
    data: Vec<String>,
    index: usize,
}

impl StubSource {
    fn poll(&mut self) -> Option<String> {
        let result = self.data.get(self.index);
        if result.is_some() {
            self.index += 1;
        }
        result.cloned()
    }
}

struct StubWriter {
    pub finished: bool,
}

impl Writer for StubWriter {
    fn is_finished(&self) -> bool {
        self.finished
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    fn to_system_time(input: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(input)
    }

    fn generate_wall_clock(times: Vec<u64>) -> StubWallClock {
        let times = times.iter().map(|x| to_system_time(*x)).collect();
        StubWallClock {
            times,
            index: RefCell::new(0),
        }
    }

    #[test]
    fn finished_when_wall_clock_exceeds_stop_time() {
        let wall_clock = generate_wall_clock(vec![2100]);
        let start_time = to_system_time(0);
        let stop_time = Some(to_system_time(2000));
        let streamer = Streamer {};
        let mut source = StubSource {
            data: Vec::new(),
            index: 0,
        };
        let mut writers: HashMap<String, Box<dyn Writer>> = HashMap::new();
        writers.insert(
            String::from("writer1"),
            Box::new(StubWriter { finished: false }),
        );

        let finished = streamer.process(
            &mut source,
            &mut writers,
            &start_time,
            stop_time,
            &wall_clock,
        );

        assert_eq!(finished, true);
    }

    #[test]
    fn not_finished_when_wall_clock_exceeds_stop_time() {
        let wall_clock = generate_wall_clock(vec![1900]);
        let start_time = to_system_time(0);
        let stop_time = Some(to_system_time(2000));
        let streamer = Streamer {};
        let mut source = StubSource {
            data: Vec::new(),
            index: 0,
        };
        let mut writers: HashMap<String, Box<dyn Writer>> = HashMap::new();
        writers.insert(
            String::from("writer1"),
            Box::new(StubWriter { finished: false }),
        );

        let finished = streamer.process(
            &mut source,
            &mut writers,
            &start_time,
            stop_time,
            &wall_clock,
        );

        assert_eq!(finished, false);
    }

    #[test]
    fn not_finished_if_no_stop_time() {
        let wall_clock = generate_wall_clock(vec![1900]);
        let start_time = to_system_time(0);
        let stop_time = None;
        let streamer = Streamer {};
        let mut source = StubSource {
            data: Vec::new(),
            index: 0,
        };
        let mut writers: HashMap<String, Box<dyn Writer>> = HashMap::new();
        writers.insert(
            String::from("writer1"),
            Box::new(StubWriter { finished: false }),
        );

        let finished = streamer.process(
            &mut source,
            &mut writers,
            &start_time,
            stop_time,
            &wall_clock,
        );

        assert_eq!(finished, false);
    }

    #[test]
    fn if_message_then_wall_clock_ignored() {
        let wall_clock = generate_wall_clock(vec![3000]);
        let start_time = to_system_time(0);
        let stop_time = Some(to_system_time(2000));
        let streamer = Streamer {};
        let mut source = StubSource {
            data: vec!["data".to_owned()],
            index: 0,
        };
        let mut writers: HashMap<String, Box<dyn Writer>> = HashMap::new();
        writers.insert(
            String::from("writer1"),
            Box::new(StubWriter { finished: false }),
        );

        let finished = streamer.process(
            &mut source,
            &mut writers,
            &start_time,
            stop_time,
            &wall_clock,
        );

        assert_eq!(finished, false);
    }

    #[test]
    fn is_finished_if_all_writers_are_finished() {
        let wall_clock = generate_wall_clock(vec![1000]);
        let start_time = to_system_time(0);
        let stop_time = Some(to_system_time(2000));
        let streamer = Streamer {};
        let mut source = StubSource {
            data: vec!["writer1".to_owned(), "writer2".to_owned()],
            index: 0,
        };
        let mut writers: HashMap<String, Box<dyn Writer>> = HashMap::new();
        writers.insert(
            String::from("writer1"),
            Box::new(StubWriter { finished: true }),
        );
        writers.insert(
            String::from("writer2"),
            Box::new(StubWriter { finished: true }),
        );

        // Process first message to stop writer 1
        let _ = streamer.process(
            &mut source,
            &mut writers,
            &start_time,
            stop_time,
            &wall_clock,
        );

        // Process second message to stop writer 2
        let finished = streamer.process(
            &mut source,
            &mut writers,
            &start_time,
            stop_time,
            &wall_clock,
        );

        assert_eq!(finished, true);
    }

    #[test]
    fn is_not_finished_if_one_writers_is_not_finished() {
        let wall_clock = generate_wall_clock(vec![1000]);
        let start_time = to_system_time(0);
        let stop_time = Some(to_system_time(2000));
        let streamer = Streamer {};
        let mut source = StubSource {
            data: vec!["writer1".to_owned(), "writer2".to_owned()],
            index: 0,
        };
        let mut writers: HashMap<String, Box<dyn Writer>> = HashMap::new();
        writers.insert(
            String::from("writer1"),
            Box::new(StubWriter { finished: true }),
        );
        writers.insert(
            String::from("writer2"),
            Box::new(StubWriter { finished: false }),
        );

        let finished = streamer.process(
            &mut source,
            &mut writers,
            &start_time,
            stop_time,
            &wall_clock,
        );

        assert_eq!(finished, false);
    }

    // TODO: writers hashmap key should be (source, schema)
    // Replace fake data with ev44 data with appropriate sources
}
