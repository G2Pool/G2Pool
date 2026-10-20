use super::*;
#[test]
fn control_frames_are_bounded_in_both_directions() {
    let mut input = std::io::Cursor::new(b"{}\nnext");
    assert_eq!(read_frame(&mut input).unwrap(), b"{}\n");
    let mut oversized = std::io::Cursor::new(vec![b' '; MAX_FRAME + 1]);
    assert!(matches!(
        read_frame(&mut oversized),
        Err(crate::Error::Invalid("control frame"))
    ));
}
