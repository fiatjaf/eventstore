package betterbinary

import (
	"testing"

	"github.com/mailru/easyjson"
	"github.com/nbd-wtf/go-nostr"
)

func TestTagFiltering(t *testing.T) {
	for _, tc := range []struct {
		json      string
		tagKey    string
		tagValues []string
		matches   bool
	}{
		{
			`{"id":"a9663055164ab8b30d9524656370c4bf93393bb051b7edf4556f40c5298dc0c7","pubkey":"ee11a5dff40c19a555f41fe42b48f00e618c91225622ae37b6c2bb67b76c4e49","created_at":1681778790,"kind":1,"sig":"4dfea1a6f73141d5691e43afc3234dbe73016db0fb207cf247e0127cc2591ee6b4be5b462272030a9bde75882aae810f359682b1b6ce6cbb97201141c576db42","content":"He got snowed in"}`,
			"x",
			[]string{"sadjqw", ""},
			false,
		},
		{
			`{"id":"a9663055164ab8b30d9524656370c4bf93393bb051b7edf4556f40c5298dc0c7","pubkey":"ee11a5dff40c19a555f41fe42b48f00e618c91225622ae37b6c2bb67b76c4e49","created_at":1681778790,"kind":1,"sig":"4dfea1a6f73141d5691e43afc3234dbe73016db0fb207cf247e0127cc2591ee6b4be5b462272030a9bde75882aae810f359682b1b6ce6cbb97201141c576db42","content":"He got snowed in","tags":[["client","gossip"],["p","e2ccf7cf20403f3f2a4a55b328f0de3be38558a7d5f33632fdaaefc726c1c8eb"],["e","2c86abcc98f7fd8a6750aab8df6c1863903f107206cc2d72e8afeb6c38357aed","wss://nostr-pub.wellorder.net/","root"]]}`,
			"e",
			[]string{"2c86abcc98f7fd8a6750aab8df6c1863903f107206cc2d72e8afeb6c38357aed"},
			true,
		},
		{
			`{"id":"3f551da67788c7aae15360d025595dc2d391f10bb7e759ee5d9b2ad7d64392e4","pubkey":"79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798","created_at":1712715433,"kind":1,"tags":[["-"],["askdasds"],["t","spam"],["t","nada"]],"content":"ggsgsgsg","sig":"43431f4cf8bd015305c2d484841e5730d261beeb375a86c57a61df3d26e820ce8d6712d2a3c89e3f2298597f14abf58079954e9e658ba59bfc2d7ce6384f25c7"}`,
			"t",
			[]string{"nothing", "nada"},
			true,
		},
		{
			`{"id":"3f551da67788c7aae15360d025595dc2d391f10bb7e759ee5d9b2ad7d64392e4","pubkey":"79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798","created_at":1712715433,"kind":1,"tags":[["-"],["askdasds"],["t","spam"],["t","nada"]],"content":"ggsgsgsg","sig":"43431f4cf8bd015305c2d484841e5730d261beeb375a86c57a61df3d26e820ce8d6712d2a3c89e3f2298597f14abf58079954e9e658ba59bfc2d7ce6384f25c7"}`,
			"z",
			[]string{"nothing", "nada"},
			false,
		},
	} {
		var evt nostr.Event
		easyjson.Unmarshal([]byte(tc.json), &evt)
		bin := make([]byte, Measure(evt))
		Marshal(evt, bin)

		if res := TagMatches(bin, tc.tagKey, tc.tagValues); res != tc.matches {
			t.Fatalf("matched incorrectly: %v=>%v over %s was %v, expected %v", tc.tagKey, tc.tagValues, tc.json, res, tc.matches)
		}
	}
}
