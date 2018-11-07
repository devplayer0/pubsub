use std::cmp;
use std::str::FromStr;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicUsize};
use std::time::{Duration, Instant};
use std::thread;
use std::net::{SocketAddr, UdpSocket};

use bytes::{BufMut, BytesMut};
use crossbeam_channel as channel;

use super::*;
use constants::*;
use util::*;
use timer::*;
use packet::*;
use protocol::*;

const LIPSUM: &'static str = include_str!("lipsum.txt");

#[test]
fn timer() {
    let timers: TimerManager<&str> = TimerManager::new();

    let count = Arc::new(AtomicUsize::new(0));
    let start_b = Instant::now();
    let kenobi = timers.post_new("general kenobi...", { let count = Arc::clone(&count); move |_| {
        println!("general kenobi @ +{:#?}", Instant::now() - start_b);
        count.fetch_add(1, Ordering::Relaxed);
    }}, start_b + Duration::from_millis(200));
    thread::sleep(Duration::from_millis(50));

    let to_cancel = timers.post_new("it's treason then!", |_| panic!("message not cancelled"), Instant::now() + Duration::from_millis(20));
    assert!(timers.cancel(&to_cancel));

    let start_a = Instant::now();
    timers.post_new("hello there!", { let count = Arc::clone(&count); move |_| {
        println!("hello there @ +{:#?}", Instant::now() - start_a);
        count.fetch_add(1, Ordering::Relaxed);
    }}, start_a + Duration::from_millis(100));

    thread::sleep(Duration::from_millis(300));
    assert_eq!(count.load(Ordering::Relaxed), 2);
    kenobi.set_message("sup");
    assert!(timers.reschedule(&kenobi, Instant::now() + Duration::from_millis(50)));
    thread::sleep(Duration::from_millis(100));
    assert_eq!(count.load(Ordering::Relaxed), 3);

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

#[inline]
fn test_gbn() -> Jqtt {
    let (tx, _) = channel::unbounded();
    Jqtt::new(&TimerManager::new(), &BufferProvider::new(IPV6_MAX_PACKET_SIZE, IPV6_MAX_PACKET_SIZE), SocketAddr::from_str("127.0.0.1:1234").unwrap(), UdpSocket::bind("127.0.0.1:0").unwrap(), &tx)
}
#[test]
fn valid_conn_packet() {
    let valid = vec![0, b'J', b'Q', b'T', b'T'].into();
    assert!(test_gbn().decode(&valid).is_ok());

    assert!(test_gbn().decode(&vec![b'J', b'Q', b'T', b'T'].into()).is_err());
    assert!(test_gbn().decode(&vec![0, b'J', b'Q', b'T'].into()).is_err());
    assert!(test_gbn().decode(&vec![b'M', b'Q', b'T', b'T'].into()).is_err());
    assert!(test_gbn().decode(&vec![b'J', b'Q', b'T', b'T', b'1'].into()).is_err());
    assert!(test_gbn().decode(&vec![b'1', b'J', b'Q', b'T', b'T'].into()).is_err());
    assert!(test_gbn().decode(&vec![b'j', b'q', b't', b't'].into()).is_err());
}

#[test]
fn invalid_packet_type() {
    let packet = vec![(31 << SEQ_BITS)].into();
    assert!(match test_gbn().decode(&packet).unwrap_err() {
        Error::InvalidType(31) => true,
        _ => false,
    })
}
/*#[test]
fn decode_ack() {
    // packet type 3, seq 0
    let ack = vec![(3 << SEQ_BITS) | 0].into();
    assert!(match test_gbn().decode(&ack).unwrap() {
        Packet::Ack(0) => true,
        _ => false,
    });

    let bad_ack = vec![(3 << SEQ_BITS) | 7, 123].into();
    assert!(match test_gbn().decode(&bad_ack).unwrap_err() {
        Error::Malformed(PacketType::Ack) => true,
        _ => false,
    });
}*/

#[test]
fn decode_subscribe() {
    // packet type 5, seq 0
    let subscribe = vec![(5 << SEQ_BITS) | 0, b'm', b'e', b'm', b'e', b's', b' ', 0xf0, 0x9f, 0xa4, 0x94].into();
    assert!(match &test_gbn().decode(&subscribe) {
        Ok(Packet::Subscribe(msg)) if msg == &"memes 🤔" => true,
        Ok(Packet::Subscribe(msg)) => {
            println!("{}", msg);
            false
        },
        Err(e) => {
            println!("{}", e);
            false
        },
        _ => false
    });
}

/*#[test]
fn decode_short_message() {
    let topic = "memes";
    let text = "it is wednesday my dudes";
    let mut publish_start = BytesMut::with_capacity(1 + size_of::<u32>() + topic.len());

    // packet type 6 (publish start), seq 0
    publish_start.put_u8((6 << SEQ_BITS) | 0);
    publish_start.put_u32_be(text.len() as u32);
    publish_start.put(topic);

    let mut gbn = test_gbn();
    assert!(match gbn.decode(&publish_start.freeze()) {
        Err(Error::Partial(PacketType::PublishData)) => true,
        Err(e) => {
            println!("{}", e);
            false
        },
        _ => false,
    });

    let mut publish_data = BytesMut::with_capacity(1 + size_of::<u32>() + text.len());

    // packet type 7 (publish data), seq 1
    publish_data.put_u8((7 << SEQ_BITS) | 1);
    publish_data.put(text);
    assert!(match gbn.decode(&publish_data.freeze()) {
        Ok(Packet::Message(msg)) => {
            String::from_utf8(msg.collect()).unwrap() == text
        },
        Err(e) => {
            println!("{}", e);
            false
        },
        _ => false
    });
}
#[test]
fn decode_long_message() {
    let topic = "memes";
    let mut packets = Vec::new();

    {
        let mut buffer = BytesMut::with_capacity(1 + size_of::<u32>() + topic.len());
        // packet type 6 (publish start)
        buffer.put_u8((6 << SEQ_BITS) | 0);
        buffer.put_u32_be(LIPSUM.len() as u32);
        buffer.put(topic);
        packets.push(buffer.freeze());
    }

    let mut i = 0;
    let mut seq = 1;
    while i != LIPSUM.len() {
        // not the exact right amount but who cares...
        let mut buffer = BytesMut::with_capacity(1 + IPV4_MAX_PACKET_SIZE);
        buffer.put_u8((7 << SEQ_BITS) | seq);

        let end = i + cmp::min(IPV4_MAX_PACKET_SIZE, LIPSUM.len() - i);
        buffer.put(&LIPSUM[i..end]);
        packets.push(buffer.freeze());

        seq = next_seq(seq);
        i = end;
    }

    let mut gbn = test_gbn();
    for (i, packet) in packets.iter().enumerate() {
        let result = gbn.decode(packet);
        if i != packets.len() - 1 {
            assert!(match result {
                Err(Error::Partial(PacketType::PublishData)) => true,
                Err(e) => {
                    println!("{}", e);
                    false
                },
                Ok(_) => false,
            });
        } else {
            assert!(match result {
                Ok(Packet::Message(msg)) => {
                    if msg.topic() != topic {
                        false
                    } else {
                        let decoded = String::from_utf8(msg.collect()).unwrap();
                        println!("{}", decoded);
                        decoded == LIPSUM
                    }
                },
                Err(e) => {
                    println!("{}", e);
                    false
                },
                Ok(_) => false,
            });
        }
    }
}*/
