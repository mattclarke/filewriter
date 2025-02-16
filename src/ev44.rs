extern crate flatbuffers;
use crate::ev44_events_generated::root_as_event_44_message;

struct Ev44 {}

impl Ev44 {
    fn get_source(buf: &[u8]) -> String {
        let root = root_as_event_44_message(buf).unwrap();
        root.source_name().to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::ev44_events_generated::{root_as_event_44_message, Event44Message, Event44MessageArgs};

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
}
