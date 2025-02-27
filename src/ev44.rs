extern crate flatbuffers;
use flatbuffers::Vector;

use crate::ev44_events_generated::root_as_event_44_message;

struct Ev44<'a> {
    source: String,
    message_id: i64,
    reference_time: Vector<'a, i64>,
}

impl Ev44<'_> {
    fn get_source(buf: &[u8]) -> String {
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
    use crate::ev44_events_generated::{Event44Message, Event44MessageArgs};

    use super::*;

    fn create_flatbuffer() -> Vec<u8> {
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
        let source = builder.create_string("SomeSource");

        let reference_times = vec![123456_i64];
        let reference_time_offset = builder.create_vector(&reference_times);
        let reference_index_offset = builder.create_vector(&vec![0_i32]);

        let tof_offset = builder.create_vector(&vec![100, 200, 300]);
        let pixels_offset = builder.create_vector(&vec![1, 2, 3]);

        let ev44 = Event44Message::create(
            &mut builder,
            &Event44MessageArgs {
                source_name: Some(source),
                message_id: 123,
                reference_time: Some(reference_time_offset),
                reference_time_index: Some(reference_index_offset),
                time_of_flight: Some(tof_offset),
                pixel_id: Some(pixels_offset),
            },
        );
        builder.finish(ev44, Some("ev44"));
        builder.finished_data().to_vec()
    }

    #[test]
    fn can_get_source_from_ev44() {
        let buf = create_flatbuffer();

        assert_eq!(Ev44::get_source(&buf.as_slice()), "SomeSource");
    }

    #[test]
    fn extract_ev44() {
        let buf = create_flatbuffer();

        let ev44 = Ev44::new(&buf.as_slice());

        assert_eq!(ev44.source, "SomeSource");
        assert_eq!(ev44.message_id, 123);
        //assert_eq!(ev44.reference_time, vec![123456_i64]);
    }
}
