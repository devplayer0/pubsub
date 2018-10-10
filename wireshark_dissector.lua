packet_types = {
	[0] = "CONNECT",
	[1] = "HEARTBEAT",
	[2] = "ACK",
	[3] = "DISCONNECT",
	[4] = "SUBSCRIBE"
}

jqtt_proto = Proto("JQTT", "Jack QTT")
f_packet_type = ProtoField.uint8("jqtt.type", "Packet type", base.DEC, packet_types)
f_packet_seq = ProtoField.uint8("jqtt.seq", "Sequence number", base.DEC)
f_topic_name = ProtoField.string("jqtt.topic", "Topic name", base.UNICODE)
jqtt_proto.fields = { f_packet_type, f_packet_seq, f_topic_name }
function jqtt_proto.dissector(buffer, pinfo, tree)
	pinfo.cols.protocol = "JQTT"

	local subtree = tree:add(jqtt_proto, buffer(), "JQTT")
	if buffer:len() >= 1 then
		local header = buffer(0, 1):uint()
		local p_type = bit.rshift(header, 3)
		local seq = bit.band(header, 0x7)

		if p_type == 0 and buffer:len() == 5 and buffer(0, 1):uint() == 0 and buffer(1, 4):string() == "JQTT" then
			subtree:add(f_packet_type, 0)
			return 5
		elseif p_type == 1 and buffer:len() == 1 and seq == 0 then
			subtree:add(f_packet_type, 1)
			return 1
		elseif p_type == 2 and buffer:len() == 1 then
			subtree:add(f_packet_type, 2)
			subtree:add(f_packet_seq, seq)
			return 1
		elseif p_type == 3 and buffer:len() == 1 and seq == 0 then
			subtree:add(f_packet_type, 3)
			return 1
		elseif p_type == 4 and buffer:len() > 1 then
			subtree:add(f_packet_type, 4)
			subtree:add(f_packet_seq, seq)
			subtree:add(f_topic_name, buffer(1, buffer:len() - 1))
			return buffer:len()
		end
	end

	subtree:add("Malformed packet")
	return 0
end

local udp_table = DissectorTable.get("udp.port")
udp_table:add(26999, jqtt_proto)
