use crate::ev44_events_generated::{Event44Message, Event44MessageArgs};

pub fn create_ev44_flatbuffer(
    source: &str,
    reference_times: &Vec<i64>,
    reference_index_offset: &Vec<i32>,
    time_of_flight: &Vec<i32>,
    pixel_ids: &Vec<i32>,
) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);

    let source = builder.create_string(source);
    let reference_time_offset = builder.create_vector(&reference_times);
    let reference_index_offset = builder.create_vector(&reference_index_offset);
    let tof_offset = builder.create_vector(&time_of_flight);
    let pixels_offset = builder.create_vector(&pixel_ids);

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
