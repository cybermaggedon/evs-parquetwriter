package main

// Code for converting a cyberprobe Event object into a FlatEvent which can be
// parquet-serialised.

// FIXME: The set of selected HTTP headers to serialise is mainly arbitrary

import (
	"encoding/base64"
	evs "github.com/cybermaggedon/evs-golang-api"
	"github.com/golang/protobuf/ptypes"
)

// A flattener takes Event objects and outputs FlatEvent objects.  This
// object makes the flattener configurable.
type Flattener struct {
	WritePayloads bool
}

// FlatEvent, similar to the cyberprobe Event, but no structure, useful for
// columnar storage.
type FlatEvent struct {

	// Common fields
	Id     string `parquet:"name=id, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Action string `parquet:"name=action, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Device string `parquet:"name=device, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Time   string `parquet:"name=time, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Origin string `parquet:"name=origin, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// Time in minutes since 1970, and microseconds since 1970.
	TimeMins int32 `parquet:"name=time_mins, type=INT32"`

	// Would like to use TIMESTAMP_MICROS but Spark isn't happy about that.
	TimeMicros int64 `parquet:"name=time_micros, type=TIME_MICROS"`

	Network string  `parquet:"name=network, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Url     string  `parquet:"name=url, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Risk    float64 `parquet:"name=risk, type=DOUBLE"`

	// Addresses
	SrcIpv4  string `parquet:"name=src_ipv4_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	SrcIpv6  string `parquet:"name=src_ipv6_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	SrcTcp   int32  `parquet:"name=src_tcp_0, type=INT32"`
	SrcUdp   int32  `parquet:"name=src_udp_0, type=INT32"`
	DestIpv4 string `parquet:"name=dest_ipv4_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DestIpv6 string `parquet:"name=dest_ipv6_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DestTcp  int32  `parquet:"name=dest_tcp_0, type=INT32"`
	DestUdp  int32  `parquet:"name=dest_udp_0, type=INT32"`

	// DNS
	DnsMessageAnswerName0    string `parquet:"name=dns_message_answer_name_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerName1    string `parquet:"name=dns_message_answer_name_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerName2    string `parquet:"name=dns_message_answer_name_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerName3    string `parquet:"name=dns_message_answer_name_3, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerName4    string `parquet:"name=dns_message_answer_name_4, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerAddress0 string `parquet:"name=dns_message_answer_address_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerAddress1 string `parquet:"name=dns_message_answer_address_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerAddress2 string `parquet:"name=dns_message_answer_address_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerAddress3 string `parquet:"name=dns_message_answer_address_3, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageAnswerAddress4 string `parquet:"name=dns_message_answer_address_4, type=UTF8, encoding=PLAIN_DICTIONARY"`

	DnsMessageQueryName0  string `parquet:"name=dns_message_query_name_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageQueryType0  string `parquet:"name=dns_message_query_type_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DnsMessageQueryClass0 string `parquet:"name=dns_message_query_class_0, type=UTF8, encoding=PLAIN_DICTIONARY"`

	DnsMessageType string `parquet:"name=dns_message_type, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// HTTP header
	HttpHeader_Accept                    string `parquet:"name=http_header_Accept, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Accept_Encoding           string `parquet:"name=http_header_Accept_Encoding, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Accept_Language           string `parquet:"name=http_header_Accept_Language, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Cache_Control             string `parquet:"name=http_header_Cache_Control, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Connection                string `parquet:"name=http_header_Connection, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Host                      string `parquet:"name=http_header_Host, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Metadata_Flavor           string `parquet:"name=http_header_Metadata_Flavor, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Pragma                    string `parquet:"name=http_header_Pragma, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Referer                   string `parquet:"name=http_header_Referer, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Upgrade_Insecure_Requests string `parquet:"name=http_header_Upgrade_Insecure_Requests, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_User_Agent                string `parquet:"name=http_header_User_Agent, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Content_Length            string `parquet:"name=http_header_Content_Length, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Content_Type              string `parquet:"name=http_header_Content_Type, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Date                      string `parquet:"name=http_header_Date, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_ETag                      string `parquet:"name=http_header_ETag, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_Server                    string `parquet:"name=http_header_Server, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_X_Frame_Options           string `parquet:"name=http_header_X_Frame_Options, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpHeader_X_XSS_Protection          string `parquet:"name=http_header_X_XSS_Protection, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// HTTP request
	HttpRequestMethod string `parquet:"name=http_request_method, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// HTTP response
	HttpResponseStatus string `parquet:"name=http_response_status, type=UTF8, encoding=PLAIN_DICTIONARY"`
	HttpResponseCode   int32  `parquet:"name=http_response_code, type=INT32"`

	// Request or response body
	HttpBody string `parquet:"name=http_body, type=BYTE_ARRAY"`

	// ICMP
	IcmpCode    int32  `parquet:"name=icmp_code, type=INT32"`
	IcmpPayload string `parquet:"name=icmp_payload, type=BYTE_ARRAY"`
	IcmpType    int32  `parquet:"name=icmp_type, type=INT32"`

	// Location
	LocationDestAccuracy    int32   `parquet:"name=location_dest_accuracy, type=INT32"`
	LocationDestAsnum       string  `parquet:"name=location_dest_asnum, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationDestAsorg       string  `parquet:"name=location_dest_asorg, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationDestCity        string  `parquet:"name=location_dest_city, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationDestCountry     string  `parquet:"name=location_dest_country, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationDestIso         string  `parquet:"name=location_dest_iso, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationDestLat float32 `parquet:"name=location_dest_lat, type=FLOAT"`
	LocationDestLon float32 `parquet:"name=location_dest_lon, type=FLOAT"`
	LocationDestPostcode    string  `parquet:"name=location_dest_postcode, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationSrcAsnum        string  `parquet:"name=location_src_asnum, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationSrcAsorg        string  `parquet:"name=location_src_asorg, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationSrcCity         string  `parquet:"name=location_src_city, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationSrcCountry      string  `parquet:"name=location_src_country, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationSrcIso          string  `parquet:"name=location_src_iso, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LocationSrcLat  float32 `parquet:"name=location_src_lat, type=FLOAT"`
	LocationSrcLon  float32 `parquet:"name=location_src_lon, type=FLOAT"`
	LocationSrcPostcode     string  `parquet:"name=location_src_postcode, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// NTP
	NtpTimestampMode    int32 `parquet:"name=ntp_timestamp_mode, type=INT32"`
	NtpTimestampVersion int32 `parquet:"name=ntp_timestamp_version, type=INT32"`

	// Unrecognised datagram
	UnrecognisedDatagramPayload       string `parquet:"name=unrecognised_datagram_payload, type=BYTE_ARRAY"`
	UnrecognisedDatagramPayloadLength int64  `parquet:"name=unrecognised_datagram_payload_length, type=INT64"`
	UnrecognisedDatagramPayloadSha1   string `parquet:"name=unrecognised_datagram_payload_sha1, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// Unrecognised stream
	UnrecognisedStreamPayload       string `parquet:"name=unrecognised_stream_payload, type=BYTE_ARRAY"`
	UnrecognisedStreamPayloadLength int64  `parquet:"name=unrecognised_stream_payload_length, type=INT64"`
	UnrecognisedStreamPayloadSha1   string `parquet:"name=unrecognised_stream_payload_sha1, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// Indicator
	IndicatorId0          string `parquet:"name=indicator_id_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorType0        string `parquet:"name=indicator_type_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorValue0       string `parquet:"name=indicator_value_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorDescription0 string `parquet:"name=indicator_description_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorCategory0    string `parquet:"name=indicator_category_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorAuthor0      string `parquet:"name=indicator_author_0, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorSource0      string `parquet:"name=indicator_source_0, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// Indicator
	IndicatorId1          string `parquet:"name=indicator_id_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorType1        string `parquet:"name=indicator_type_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorValue1       string `parquet:"name=indicator_value_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorDescription1 string `parquet:"name=indicator_description_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorCategory1    string `parquet:"name=indicator_category_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorAuthor1      string `parquet:"name=indicator_author_1, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorSource1      string `parquet:"name=indicator_source_1, type=UTF8, encoding=PLAIN_DICTIONARY"`

	// Indicator
	IndicatorId2          string `parquet:"name=indicator_id_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorType2        string `parquet:"name=indicator_type_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorValue2       string `parquet:"name=indicator_value_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorDescription2 string `parquet:"name=indicator_description_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorCategory2    string `parquet:"name=indicator_category_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorAuthor2      string `parquet:"name=indicator_author_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
	IndicatorSource2      string `parquet:"name=indicator_source_2, type=UTF8, encoding=PLAIN_DICTIONARY"`
}

// Decode Base64 string to a string
func Debase64(in string) string {
	enc := base64.StdEncoding
	p, err := enc.DecodeString(in)
	if err == nil {
		return string(p)
	} else {
		return ""
	}

}

// Flatten the source addresses
func (f *Flattener) FlattenSrc(ev *evs.Event, oe *FlatEvent) {
	for _, v := range ev.Src {
		switch v.Protocol {
		case evs.Protocol_ipv4:
			oe.SrcIpv4 = evs.Int32ToIp(v.Address.GetIpv4()).String()
			break
		case evs.Protocol_ipv6:
			oe.SrcIpv6 = evs.BytesToIp(v.Address.GetIpv6()).String()
			break
		case evs.Protocol_tcp:
			oe.SrcTcp = int32(v.Address.GetPort())
			break
		case evs.Protocol_udp:
			oe.SrcUdp = int32(v.Address.GetPort())
			break
		default:
		}
	}
}

// Flatten the destination addresses
func (f *Flattener) FlattenDest(ev *evs.Event, oe *FlatEvent) {
	for _, v := range ev.Dest {
		switch v.Protocol {
		case evs.Protocol_ipv4:
			ip := evs.Int32ToIp(v.Address.GetIpv4())
			oe.DestIpv4 = ip.String()
			break
		case evs.Protocol_ipv6:
			ip := evs.BytesToIp(v.Address.GetIpv6())
			oe.DestIpv6 = ip.String()
			break
		case evs.Protocol_tcp:
			oe.DestTcp = int32(v.Address.GetPort())
			break
		case evs.Protocol_udp:
			oe.DestUdp = int32(v.Address.GetPort())
			break
		default:
		}
	}
}

// Flatten DNS information
func (f *Flattener) FlattenDnsMessage(ev *evs.Event, oe *FlatEvent) {

	msg := ev.GetDnsMessage()

	oe.DnsMessageType = msg.Type.String()
	
	if len(msg.Query) >= 1 {
		qry := msg.Query[0]
		oe.DnsMessageQueryName0 = qry.Name
		oe.DnsMessageQueryType0 = qry.Type
		oe.DnsMessageQueryClass0 = qry.Class
	}

	if len(msg.Answer) >= 1 {
		ans := msg.Answer[0]
		oe.DnsMessageAnswerName0 = ans.Name
		if ans.Address != nil {
			oe.DnsMessageAnswerAddress0 = evs.AddressToString(ans.Address)
		}
	}
	if len(msg.Answer) >= 2 {
		ans := msg.Answer[1]
		oe.DnsMessageAnswerName1 = ans.Name
		if ans.Address != nil {
			oe.DnsMessageAnswerAddress1 = evs.AddressToString(ans.Address)
		}
	}
	if len(msg.Answer) >= 3 {
		ans := msg.Answer[2]
		oe.DnsMessageAnswerName2 = ans.Name
		if ans.Address != nil {
			oe.DnsMessageAnswerAddress2 = evs.AddressToString(ans.Address)
		}
	}
	if len(msg.Answer) >= 4 {
		ans := msg.Answer[3]
		oe.DnsMessageAnswerName3 = ans.Name
		if ans.Address != nil {
			oe.DnsMessageAnswerAddress3 = evs.AddressToString(ans.Address)
		}
	}
	if len(msg.Answer) >= 5 {
		ans := msg.Answer[4]
		oe.DnsMessageAnswerName4 = ans.Name
		if ans.Address != nil {
			oe.DnsMessageAnswerAddress4 = evs.AddressToString(ans.Address)
		}
	}

}

// Flatten HTTP information
func (f *Flattener) FlattenHttpHeader(header map[string]string, oe *FlatEvent) {
	if v, ok := header["Accept"]; ok {
		oe.HttpHeader_Accept = v
	}
	if v, ok := header["Accept-Encoding"]; ok {
		oe.HttpHeader_Accept_Encoding = v
	}
	if v, ok := header["Accept-Language"]; ok {
		oe.HttpHeader_Accept_Language = v
	}
	if v, ok := header["Accept-Cache-Control"]; ok {
		oe.HttpHeader_Cache_Control = v
	}
	if v, ok := header["Connection"]; ok {
		oe.HttpHeader_Connection = v
	}
	if v, ok := header["Host"]; ok {
		oe.HttpHeader_Host = v
	}
	if v, ok := header["Metadata-Flavor"]; ok {
		oe.HttpHeader_Metadata_Flavor = v
	}
	if v, ok := header["Pragma"]; ok {
		oe.HttpHeader_Pragma = v
	}
	if v, ok := header["Referer"]; ok {
		oe.HttpHeader_Referer = v
	}
	if v, ok := header["Upgrade-Insecure-Requests"]; ok {
		oe.HttpHeader_Upgrade_Insecure_Requests = v
	}
	if v, ok := header["User-Agent"]; ok {
		oe.HttpHeader_User_Agent = v
	}
	if v, ok := header["Content-Length"]; ok {
		oe.HttpHeader_Content_Length = v
	}
	if v, ok := header["Content-Type"]; ok {
		oe.HttpHeader_Content_Type = v
	}
	if v, ok := header["Date"]; ok {
		oe.HttpHeader_Date = v
	}
	if v, ok := header["ETag"]; ok {
		oe.HttpHeader_ETag = v
	}
	if v, ok := header["Server"]; ok {
		oe.HttpHeader_Server = v
	}
	if v, ok := header["X-Frame-Options"]; ok {
		oe.HttpHeader_X_Frame_Options = v
	}
	if v, ok := header["X-XSS-Protection"]; ok {
		oe.HttpHeader_X_XSS_Protection = v
	}
}

// Flatten an HTTP request
func (f *Flattener) FlattenHttpRequest(ev *evs.Event, oe *FlatEvent) {
	msg := ev.GetHttpRequest()
	oe.HttpRequestMethod = msg.Method
	if f.WritePayloads {
		oe.HttpBody = string(msg.Body)
	}
	f.FlattenHttpHeader(msg.Header, oe)
}

// Flatten an HTTP response
func (f *Flattener) FlattenHttpResponse(ev *evs.Event, oe *FlatEvent) {
	msg := ev.GetHttpResponse()
	oe.HttpResponseStatus = msg.Status
	oe.HttpResponseCode = int32(msg.Code)
	if f.WritePayloads {
		oe.HttpBody = string(msg.Body)
	}
	f.FlattenHttpHeader(msg.Header, oe)
}

// Flatten ICMP information
func (f *Flattener) FlattenIcmp(ev *evs.Event, oe *FlatEvent) {
	msg := ev.GetIcmp()
	oe.IcmpCode = int32(msg.Code)
	oe.IcmpType = int32(msg.Type)
	if f.WritePayloads {
		oe.IcmpPayload = string(msg.Payload)
	}
}

// Flatten location information
func (f *Flattener) FlattenLocation(ev *evs.Event, oe *FlatEvent) {
	if ev.Location.Src != nil {
		oe.LocationSrcAsnum = ev.Location.Src.Asnum
		oe.LocationSrcAsorg = ev.Location.Src.Asorg
		oe.LocationSrcCity = ev.Location.Src.City
		oe.LocationSrcCountry = ev.Location.Src.Country
		oe.LocationSrcIso = ev.Location.Src.Iso
		oe.LocationSrcLat = ev.Location.Src.Latitude
		oe.LocationSrcLon = ev.Location.Src.Longitude
		oe.LocationSrcPostcode = ev.Location.Src.Postcode
	}
	if ev.Location.Dest != nil {
		oe.LocationDestAsnum = ev.Location.Dest.Asnum
		oe.LocationDestAsorg = ev.Location.Dest.Asorg
		oe.LocationDestCity = ev.Location.Dest.City
		oe.LocationDestCountry = ev.Location.Dest.Country
		oe.LocationDestIso = ev.Location.Dest.Iso
		oe.LocationDestLat = ev.Location.Dest.Latitude
		oe.LocationDestLon = ev.Location.Dest.Longitude
		oe.LocationDestPostcode = ev.Location.Dest.Postcode
	}
}

// Flatten NTP information
func (f *Flattener) FlattenNtpTimestamp(ev *evs.Event, oe *FlatEvent) {
	msg := ev.GetNtpTimestamp()
	oe.NtpTimestampMode = int32(msg.Mode)
	oe.NtpTimestampVersion = int32(msg.Version)
}

// Flatten unrecognised datagram information
func (f *Flattener) FlattenUnrecognisedDatagram(ev *evs.Event, oe *FlatEvent) {

	msg := ev.GetUnrecognisedDatagram()

	if f.WritePayloads {
		oe.UnrecognisedDatagramPayload = string(msg.Payload)
	}
	// FIXME: Schema missing?
//	oe.UnrecognisedDatagramPayloadLength = int64(msg.PayloadLength)
//	oe.UnrecognisedDatagramPayloadSha1 = msg.PayloadHash

}

// Flatten an unrecognised stream
func (f *Flattener) FlattenUnrecognisedStream(ev *evs.Event, oe *FlatEvent) {

	msg := ev.GetUnrecognisedStream()

	if f.WritePayloads {
		oe.UnrecognisedStreamPayload = string(msg.Payload)
	}
	// FIXME: Schema missing?
//	oe.UnrecognisedStreamPayloadLength = int64(msg.PayloadLength)
//	oe.UnrecognisedStreamPayloadSha1 = msg.PayloadHash

}

// Flatten DNS information
func (f *Flattener) FlattenIndicators(ev *evs.Event, oe *FlatEvent) {

	if ev.Indicators != nil {

		if len(ev.Indicators) >= 1 {
			ind := ev.Indicators[0]
			oe.IndicatorId0 = ind.Id
			oe.IndicatorType0 = ind.Type
			oe.IndicatorValue0 = ind.Value
			oe.IndicatorDescription0 = ind.Description
			oe.IndicatorCategory0 = ind.Category
			oe.IndicatorAuthor0 = ind.Author
			oe.IndicatorSource0 = ind.Source
		}
		if len(ev.Indicators) >= 2 {
			ind := ev.Indicators[1]
			oe.IndicatorId1 = ind.Id
			oe.IndicatorType1 = ind.Type
			oe.IndicatorValue1 = ind.Value
			oe.IndicatorDescription1 = ind.Description
			oe.IndicatorCategory1 = ind.Category
			oe.IndicatorAuthor1 = ind.Author
			oe.IndicatorSource1 = ind.Source
		}
		if len(ev.Indicators) >= 3 {
			ind := ev.Indicators[2]
			oe.IndicatorId2 = ind.Id
			oe.IndicatorType2 = ind.Type
			oe.IndicatorValue2 = ind.Value
			oe.IndicatorDescription2 = ind.Description
			oe.IndicatorCategory2 = ind.Category
			oe.IndicatorAuthor2 = ind.Author
			oe.IndicatorSource2 = ind.Source
		}
	}
}

// Flatten an Event, returns a FlatEvent.
func (f *Flattener) Convert(ev *evs.Event) *FlatEvent {

	tm, _ := ptypes.Timestamp(ev.Time)
	
	oe := &FlatEvent{
		Id:      ev.Id,
		Action:  ev.Action.String(),
		Device:  ev.Device,
		Time:    tm.Format("2006-01-02T15:04:05.999Z"),
		Network: ev.Network,
		Url:     ev.Url,
//		Risk:    ev.Risk,
		Origin:  ev.Origin.String(),
	}

	nanos := tm.UnixNano()
	oe.TimeMicros = nanos / 1000
	oe.TimeMins = int32(nanos / 1000000000 / 60)

	f.FlattenSrc(ev, oe)
	f.FlattenDest(ev, oe)

	switch ev.Detail.(type) {
	case *evs.Event_DnsMessage:
		f.FlattenDnsMessage(ev, oe)
		break
	case *evs.Event_HttpRequest:
		f.FlattenHttpRequest(ev, oe)
		break
	case *evs.Event_HttpResponse:
		f.FlattenHttpResponse(ev, oe)
		break
	case *evs.Event_Icmp:
		f.FlattenIcmp(ev, oe)
		break
	case *evs.Event_NtpTimestamp:
		f.FlattenNtpTimestamp(ev, oe)
		break
	case *evs.Event_UnrecognisedDatagram:
		f.FlattenUnrecognisedDatagram(ev, oe)
		break
	case *evs.Event_UnrecognisedStream:
		f.FlattenUnrecognisedStream(ev, oe)
		break
	default:
	}

	if ev.Location != nil {
		f.FlattenLocation(ev, oe)
	}

	if ev.Indicators != nil {
		f.FlattenIndicators(ev, oe)
	}

	return oe

}
