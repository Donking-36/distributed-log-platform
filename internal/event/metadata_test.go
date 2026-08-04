package event

import (
	"strconv"
	"testing"
)

func TestBestEffortTestRunID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
		want    string
	}{
		{
			name:    "可从其他字段无效的事件中恢复",
			payload: nestedFilebeatPayload(`{"test_run_id":"dlq-run-001"}`),
			want:    "dlq-run-001",
		},
		{
			name:    "保留非空值的首尾空白",
			payload: nestedFilebeatPayload(`{"test_run_id":"  dlq-run-002  "}`),
			want:    "  dlq-run-002  ",
		},
		{name: "字段缺失", payload: nestedFilebeatPayload(`{"message":"test_run_id 只是正文"}`)},
		{name: "空字符串", payload: nestedFilebeatPayload(`{"test_run_id":""}`)},
		{name: "全空白", payload: nestedFilebeatPayload(`{"test_run_id":"  "}`)},
		{name: "null", payload: nestedFilebeatPayload(`{"test_run_id":null}`)},
		{name: "非字符串", payload: nestedFilebeatPayload(`{"test_run_id":7}`)},
		{name: "外层损坏", payload: []byte(`not-json`)},
		{name: "message 不是字符串", payload: []byte(`{"message":{"test_run_id":"bait"}}`)},
		{name: "内层损坏", payload: nestedFilebeatPayload(`not-json`)},
		{
			name:    "不读取外层同名诱饵",
			payload: []byte(`{"test_run_id":"outer-bait","message":"{}"}`),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := BestEffortTestRunID(test.payload); got != test.want {
				t.Fatalf("BestEffortTestRunID() = %q，期望 %q", got, test.want)
			}
		})
	}
}

func nestedFilebeatPayload(inner string) []byte {
	return []byte(`{"message":` + strconv.Quote(inner) + `}`)
}
