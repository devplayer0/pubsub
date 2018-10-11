use super::*;
use std::str::FromStr;
use std::time::{Duration, Instant};
use std::thread;

use crossbeam_channel as channel;

#[test]
fn timer() {
    let (tx, rx) = crossbeam_channel::unbounded();
    let timers: TimerManager<&str> = TimerManager::new();
    let id = timers.register(&tx);

    let start_b = Instant::now();
    timers.post_message(id, "general kenobi...", start_b + Duration::from_millis(200));
    thread::sleep(Duration::from_millis(50));

    let to_cancel = timers.post_message(id, "it's treason then!", Instant::now() + Duration::from_millis(20));
    assert!(timers.cancel_message(to_cancel));

    let start_a = Instant::now();
    timers.post_message(id, "hello there!", start_a + Duration::from_millis(100));

    assert_eq!(rx.recv().unwrap(), "hello there!");
    println!("hello there @ +{:#?}", Instant::now() - start_a);
    assert_eq!(rx.recv().unwrap(), "general kenobi...");
    println!("general kenobi @ +{:#?}", Instant::now() - start_b);

    timers.stop();
}

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
    let valid = vec![0, b'J', b'Q', b'T', b'T'].into();
    assert!(Packet::validate_connect(&valid).is_ok());

    assert!(Packet::validate_connect(&vec![b'J', b'Q', b'T', b'T'].into()).is_err());
    assert!(Packet::validate_connect(&vec![0, b'J', b'Q', b'T'].into()).is_err());
    assert!(Packet::validate_connect(&vec![b'M', b'Q', b'T', b'T'].into()).is_err());
    assert!(Packet::validate_connect(&vec![b'J', b'Q', b'T', b'T', b'1'].into()).is_err());
    assert!(Packet::validate_connect(&vec![b'1', b'J', b'Q', b'T', b'T'].into()).is_err());
    assert!(Packet::validate_connect(&vec![b'j', b'q', b't', b't'].into()).is_err());
}
#[test]
fn encode_conn_packet() {
    let connect = Packet::make_connect();
    assert!(Packet::validate_connect(&connect).is_ok());
}

#[inline]
fn test_gbn() -> GoBackN {
    let (tx, _) = channel::unbounded();
    GoBackN::new(&TimerManager::new(), SocketAddr::from_str("127.0.0.1:1234").unwrap(), UdpSocket::bind("127.0.0.1:0").unwrap(), &tx, false)
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
    // packet type 2, seq 7
    let ack = vec![(2 << SEQ_BITS) | 7].into();
    assert!(match test_gbn().decode(&ack).unwrap() {
        Packet::Ack(7) => true,
        _ => false,
    });

    let bad_ack = vec![(2 << SEQ_BITS) | 7, 123].into();
    assert!(match test_gbn().decode(&bad_ack).unwrap_err() {
        DecodeError::Malformed(PacketType::Ack) => true,
        _ => false,
    });
}

#[test]
fn decode_subscribe() {
    // packet type 4, seq 0
    let subscribe = vec![(4 << SEQ_BITS) | 0, b'm', b'e', b'm', b'e', b's', b' ', 0xf0, 0x9f, 0xa4, 0x94].into();
    assert!(match &test_gbn().decode(&subscribe) {
        Ok(Packet::Subscribe(msg)) if msg == "memes 🤔" => true,
        Err(e) => {
            println!("{}", e);
            false
        },
        _ => false
    });
}
#[test]
fn encode_subscribe() {
    let topic = Packet::make_subscribe(&mut BytesMut::with_capacity(11), "memes 🤔", 0);
    assert!(test_gbn().decode(&topic).is_ok());
}
