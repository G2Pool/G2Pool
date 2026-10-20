use super::*;

async fn listen(swarm: &mut Swarm<Behaviour>) -> Multiaddr {
    swarm
        .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
        .unwrap();
    loop {
        if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
            return address;
        }
    }
}
#[tokio::test]
async fn profile_bound_noise_rejects_other_network_before_requests() {
    tokio::time::timeout(Duration::from_secs(10),async {
        let mut a=swarm([1;32],Budgets::default(),2,super::super::bans::IpBans::default()).unwrap();let mut b=swarm([2;32],Budgets::default(),2,super::super::bans::IpBans::default()).unwrap();
        let address=listen(&mut b).await;a.dial(address).unwrap();
        loop{tokio::select!{
            e=a.select_next_some()=>match e {SwarmEvent::ConnectionEstablished{..}=>panic!("wrong profile connected"),SwarmEvent::OutgoingConnectionError{..}=>break,_=>{}},
            e=b.select_next_some()=>{assert!(!matches!(e,SwarmEvent::ConnectionEstablished{..}));}
        }}
    }).await.unwrap();
}
#[tokio::test]
async fn real_schema_is_bidirectional_and_payload_ticket_survives_delivery() {
    tokio::time::timeout(Duration::from_secs(45),async {
        let budgets=Budgets::default();let mut a=swarm([1;32],budgets.clone(),2,super::super::bans::IpBans::default()).unwrap();let mut b=swarm([1;32],Budgets::default(),2,super::super::bans::IpBans::default()).unwrap();
        let aa=listen(&mut a).await;let ba=listen(&mut b).await;
        a.dial(libp2p::swarm::dial_opts::DialOpts::unknown_peer_id().address(ba).allocate_new_port().build()).unwrap();
        b.dial(libp2p::swarm::dial_opts::DialOpts::unknown_peer_id().address(aa).allocate_new_port().build()).unwrap();let aid=*a.local_peer_id();let bid=*b.local_peer_id();
        let settle=tokio::time::sleep(Duration::from_millis(300));tokio::pin!(settle);
        loop{tokio::select!{_= &mut settle=>break,_=a.select_next_some()=>{},_=b.select_next_some()=>{}}}
        assert!(a.is_connected(&bid));assert!(b.is_connected(&aid));
        assert_eq!(a.network_info().connection_counters().num_established(),1);assert_eq!(b.network_info().connection_counters().num_established(),1);
        let object=crate::storage::Object::content(&crate::codec::TxOnly{protocol:[1;32],native:crate::codec::Blob(vec![41; 8*1024*1024-128])}).unwrap();
        let key=object.key;let missing=crate::objects::Key::new(crate::objects::Kind::Coupon,[2;32]);
        let get=Message::Get(vec![key,missing]);let reservation=get.response_ceiling().unwrap()*6;
        let ticket=budgets.receive.reserve(reservation).unwrap();
        a.behaviour_mut().bulk.send_request(&bid,Request{message:get,ticket});
        b.behaviour_mut().control.send_request(&aid,Request{message:Message::Heads,ticket:budgets.control.reserve(CONTROL*6).unwrap()});
        let mut response=None;let mut control_done=false;
        while response.is_none()||!control_done{tokio::select!{
            e=a.select_next_some()=> match e {
                SwarmEvent::Behaviour(BehaviourEvent::Bulk(rpc::Event::OutboundFailure{error,..})) | SwarmEvent::Behaviour(BehaviourEvent::Control(rpc::Event::OutboundFailure{error,..}))=>panic!("A RPC failed: {error}"),
                SwarmEvent::ConnectionClosed{cause,..}=>panic!("A lost settled link: {cause:?}"),
                SwarmEvent::Behaviour(BehaviourEvent::Bulk(rpc::Event::InboundFailure{error,..})) | SwarmEvent::Behaviour(BehaviourEvent::Control(rpc::Event::InboundFailure{error,..}))=>panic!("A inbound failed: {error}"),
                SwarmEvent::Behaviour(BehaviourEvent::Control(rpc::Event::Message{message:rpc::Message::Request{channel,request,..},..}))=>{a.behaviour_mut().control.send_response(channel,Packet::new(Message::Tips(vec![[7;32]]),request.ticket)).unwrap();},
                SwarmEvent::Behaviour(BehaviourEvent::Bulk(rpc::Event::Message{message:rpc::Message::Response{response:packet,..},..}))=>response=Some(packet),_=>{}
            },
            e=b.select_next_some()=>match e {
                SwarmEvent::Behaviour(BehaviourEvent::Bulk(rpc::Event::OutboundFailure{error,..})) | SwarmEvent::Behaviour(BehaviourEvent::Control(rpc::Event::OutboundFailure{error,..}))=>panic!("B RPC failed: {error}"),
                SwarmEvent::ConnectionClosed{cause,..}=>panic!("B lost settled link: {cause:?}"),
                SwarmEvent::Behaviour(BehaviourEvent::Bulk(rpc::Event::InboundFailure{error,..})) | SwarmEvent::Behaviour(BehaviourEvent::Control(rpc::Event::InboundFailure{error,..}))=>panic!("B inbound failed: {error}"),
                SwarmEvent::Behaviour(BehaviourEvent::Bulk(rpc::Event::Message{message:rpc::Message::Request{channel,..},..}))=>{
                    let ticket=budgets.serve.reserve(reservation).unwrap();
                    b.behaviour_mut().bulk.send_response(channel,Packet::new(Message::Reply{objects:vec![object.clone()],missing:vec![missing],deferred:vec![]},ticket)).unwrap();
                },
                SwarmEvent::Behaviour(BehaviourEvent::Control(rpc::Event::Message{message:rpc::Message::Response{response,..},..}))=>{assert!(matches!(response.message(),Message::Tips(ids) if ids==vec![[7;32]]));control_done=true;},_=>{}
            }
        }}
        let packet=response.unwrap();
        assert_eq!(budgets.receive.used(),reservation);
        if let (Message::Reply {objects: first,..}, Message::Reply {objects: second,..}) = (packet.message(),packet.message()) {
            assert!(Arc::ptr_eq(&first[0].bytes, &second[0].bytes), "reading a packet twice must share its decoded payload");
        } else { panic!("wrong reply"); }
        match packet.message(){Message::Reply{objects,missing:absent,..}=>{assert_eq!(objects[0].bytes,object.bytes);assert_eq!(absent,vec![missing]);},_=>panic!("wrong reply")}
        drop(packet);assert_eq!(budgets.receive.used(),0);
    }).await.unwrap();
}
#[tokio::test]
async fn codec_rejects_wrong_role_and_oversize_before_body_allocation() {
    use rpc::Codec as _;
    let budgets = Budgets::default();
    for (length, tag) in [(16 * 1024 * 1024, 1), (128, 3), (2051, 1)] {
        let mut codec = Codec {
            budgets: budgets.clone(),
            bulk: false,
            expected: Some(Request {
                message: Message::Heads,
                ticket: budgets.control.reserve(4096).unwrap(),
            }),
        };
        let mut wire = (length as u32).to_be_bytes().to_vec();
        wire.push(tag);
        assert!(codec
            .read_response(
                &StreamProtocol::new("/test"),
                &mut libp2p::futures::io::Cursor::new(wire)
            )
            .await
            .is_err());
        assert_eq!(budgets.control.used(), 0);
    }
    let codec = Codec {
        budgets: budgets.clone(),
        bulk: false,
        expected: Some(Request {
            message: Message::Heads,
            ticket: budgets.control.reserve(4096).unwrap(),
        }),
    };
    assert!(codec.clone().expected.is_none());
    drop(codec);
    assert_eq!(budgets.control.used(), 0);
}
