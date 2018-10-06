use super::*;

#[test]
fn t_window_end() {
    // 0 1 2 3 4 5 6
    assert_eq!(window_end(0), 7);
    // 3 4 5 6 7 0 1
    assert_eq!(window_end(3), 2);
    // 1 2 3 4 5 6 7
    assert_eq!(window_end(1), 0);
    // 7 0 1 2 3 4 5
    assert_eq!(window_end(7), 6);
}
#[test]
fn window_range() {
    for i in 0..8 {
        for j in 0..8 {
            let ret = seq_in_window(i, j);
            assert!(match j {
                j if j == window_end(i) => !ret,
                _ => ret,
            }, "failed with window starting at {} for seq {}", i, j);
        }
    }
}
#[test]
fn t_slide_amount() {
    // 0 1 2 3 4 5 6
    assert_eq!(slide_amount(0, 2), 3);
    assert_eq!(slide_amount(0, 6), 7);
    assert_eq!(slide_amount(0, 0), 1);

    // 3 4 5 6 7 0 1
    assert_eq!(slide_amount(3, 5), 3);
    assert_eq!(slide_amount(3, 0), 6);
    assert_eq!(slide_amount(3, 1), 7);
    assert_eq!(slide_amount(3, 3), 1);

    // 1 2 3 4 5 6 7
    assert_eq!(slide_amount(1, 7), 7);

    // 7 0 1 2 3 4 5
    assert_eq!(slide_amount(7, 1), 3);
}
#[test]
fn t_next_seq() {
    assert_eq!(next_seq(1), 2);
    assert_eq!(next_seq(7), 0);
    assert_eq!(next_seq(0), 1);
    assert_eq!(next_seq(3), 4);
}

#[test]
fn valid_conn_packet() {
    let valid = vec![b'J', b'Q', b'T', b'T'].into();
    assert!(Packet::validate_connect(&valid).is_ok());

    assert!(Packet::validate_connect(&vec![b'J', b'Q', b'T'].into()).is_err());
    assert!(Packet::validate_connect(&vec![b'M', b'Q', b'T', b'T'].into()).is_err());
    assert!(Packet::validate_connect(&vec![b'J', b'Q', b'T', b'T', b'1'].into()).is_err());
    assert!(Packet::validate_connect(&vec![b'1', b'J', b'Q', b'T', b'T'].into()).is_err());
    assert!(Packet::validate_connect(&vec![b'j', b'q', b't', b't'].into()).is_err());
}
#[test]
fn encode_conn_packet() {
    let connect = Packet::make_connect(vec![0; 4]);
    assert!(Packet::validate_connect(&connect).is_ok());
}

#[inline]
fn test_gbn() -> GoBackN {
    GoBackN::new(UdpSocket::bind("127.0.0.1:0").unwrap())
}
#[test]
fn invalid_packet_type() {
    let packet = vec![(31 << SEQ_BITS)].into();
    assert!(match test_gbn().decode(&packet).unwrap_err() {
        DecodeError::InvalidType(31) => true,
        _ => false,
    })
}
#[test]
fn decode_ack() {
    // packet type 1, seq 7
    let ack = vec![(1 << SEQ_BITS) | 7].into();
    assert!(match test_gbn().decode(&ack).unwrap() {
        Packet::Ack(7) => true,
        _ => false,
    });

    let bad_ack = vec![(1 << SEQ_BITS) | 7, 123].into();
    assert!(match test_gbn().decode(&bad_ack).unwrap_err() {
        DecodeError::Malformed(PacketType::Ack) => true,
        _ => false,
    });
}
