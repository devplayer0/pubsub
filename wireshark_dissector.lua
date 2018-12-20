packet_types = {
	[0] = "CONNECT",
	[1] = "CONNACK",
	[2] = "HEARTBEAT",
	[3] = "ACK",
	[4] = "DISCONNECT",
	[5] = "SUBSCRIBE",
	[6] = "UNSUBSCRIBE",
	[7] = "PUBLISH_START",
	[8] = "PUBLISH_DATA",
}

jqtt_proto = Proto("JQTT", "Jack QTT")

f_packet_type = ProtoField.uint8("jqtt.type", "Packet type", base.DEC, packet_types)
f_keepalive = ProtoField.bool("jqtt.keepalive", "Keepalive enabled")
f_max_packet_size = ProtoField.uint16("jqtt.max_packet_size", "Max packet size", base.DEC)
f_packet_seq = ProtoField.uint8("jqtt.seq", "Sequence number", base.DEC)
f_topic_name = ProtoField.string("jqtt.topic", "Topic name", base.UNICODE)
f_message_id = ProtoField.uint32("jqtt.message_id", "Message ID", base.DEC)
f_message_size = ProtoField.uint32("jqtt.message_size", "Total message size", base.DEC)
f_message_payload = ProtoField.bytes("jqtt.payload", "Partial message payload")
jqtt_proto.fields = { f_packet_type, f_keepalive, f_max_packet_size, f_packet_seq, f_topic_name, f_message_id, f_message_size, f_message_payload }

function jqtt_proto.dissector(buffer, pinfo, tree)
	pinfo.cols.protocol = "JQTT"

	local subtree = tree:add(jqtt_proto, buffer(), "JQTT")
	if buffer:len() >= 1 then
		local header = buffer(0, 1):uint()
		local p_type = bit.rshift(header, 4)
		local seq = bit.band(header, 0xf)

		local parsed = -1
		if p_type == 0 and buffer:len() == 5 and buffer(0, 1):uint() == 0 and buffer(1, 4):string() == "JQTT" then
			subtree:add(f_packet_type, p_type)
			parsed = 5
		elseif p_type == 1 and buffer:len() == 3 and seq < 2 then
			subtree:add(f_packet_type, p_type)
			subtree:add(f_keepalive, seq)
			subtree:add(f_max_packet_size, buffer(1, 2))
			parsed = 1
		elseif p_type == 2 and buffer:len() == 1 and seq == 0 then
			subtree:add(f_packet_type, p_type)
			parsed = 1
		elseif p_type == 3 and buffer:len() == 1 then
			subtree:add(f_packet_type, p_type)
			subtree:add(f_packet_seq, seq)
			parsed = 1
		elseif p_type == 4 and buffer:len() == 1 then
			subtree:add(f_packet_type, p_type)
			subtree:add(f_packet_seq, seq)
			parsed = 1
		elseif p_type == 5 or p_type == 6 and buffer:len() > 1 then
			subtree:add(f_packet_type, p_type)
			subtree:add(f_packet_seq, seq)
			subtree:add(f_topic_name, buffer(1, buffer:len() - 1))
			parsed = buffer:len()
		elseif p_type == 7 and buffer:len() > 9 then
			subtree:add(f_packet_type, p_type)
			subtree:add(f_packet_seq, seq)
			subtree:add(f_message_id, buffer(1, 4))
			subtree:add(f_message_size, buffer(5, 4))
			subtree:add(f_topic_name, buffer(9, buffer:len() - 9))
			parsed = buffer:len()
		elseif p_type == 8 and buffer:len() > 5 then
			subtree:add(f_packet_type, p_type)
			subtree:add(f_packet_seq, seq)
			subtree:add(f_message_id, buffer(1, 4))
			subtree:add(f_message_payload, buffer(5, buffer:len() - 5))
			parsed = buffer:len()
		end

		if parsed ~= -1 then
			pinfo.cols.info:set(pinfo.src_port .. " -> " .. pinfo.dst_port)
			pinfo.cols.info:append(" ["..packet_types[p_type].."]")
			if p_type >= 3 then
				pinfo.cols.info:append(", seq = "..seq)
			end

			return parsed
		end
	end

	subtree:add("Malformed packet")
	return 0
end

local udp_table = DissectorTable.get("udp.port")
udp_table:add(26999, jqtt_proto)
