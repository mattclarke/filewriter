extern crate flatbuffers;
use flatbuffers::Vector;

use crate::ev44_events_generated::root_as_event_44_message;

pub struct Ev44<'a> {
    source: String,
    message_id: i64,
    reference_time: Vector<'a, i64>,
}

impl Ev44<'_> {
    pub fn get_source(buf: &[u8]) -> String {
        let root = root_as_event_44_message(buf).unwrap();
        root.source_name().to_string()
    }

    fn new(buf: &[u8]) -> Ev44 {
        let source = Ev44::get_source(&buf);
        let root = root_as_event_44_message(buf).unwrap();
        let message_id = root.message_id();
        let reference_time = root.reference_time();
        dbg!(reference_time);

        Ev44 {
            source,
            message_id,
            reference_time,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ev44_events_generated::{Event44Message, Event44MessageArgs},
        utils::create_ev44_flatbuffer,
    };

    #[test]
    fn can_get_source_from_ev44() {
        let buf = create_ev44_flatbuffer(
            "SomeSource",
            &vec![12345],
            &vec![0],
            &vec![100, 200, 300],
            &vec![1, 2, 3],
        );

        assert_eq!(Ev44::get_source(&buf.as_slice()), "SomeSource");
    }

    #[test]
    fn extract_ev44() {
        let buf = create_ev44_flatbuffer(
            "SomeSource",
            &vec![12345],
            &vec![0],
            &vec![100, 200, 300],
            &vec![1, 2, 3],
        );

        let ev44 = Ev44::new(&buf.as_slice());

        assert_eq!(ev44.source, "SomeSource");
        assert_eq!(ev44.message_id, 123);
        //assert_eq!(ev44.reference_time, vec![123456_i64]);
    }
}
