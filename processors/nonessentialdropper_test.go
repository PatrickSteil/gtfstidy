package processors

import (
	"testing"

	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
)

func TestNonEssentialDropper(t *testing.T) {
	feed := gtfsparser.NewFeed()

	lvl := &gtfs.Level{Id: "l1"}
	feed.Levels["l1"] = lvl
	feed.FareAttributes["f1"] = &gtfs.FareAttribute{Id: "f1"}
	a, b := &gtfs.Stop{Id: "a", Name: "a", Level: lvl}, &gtfs.Stop{Id: "b", Name: "b"}
	feed.Stops["a"], feed.Stops["b"] = a, b
	feed.Pathways["p1"] = &gtfs.Pathway{Id: "p1", From_stop: a, To_stop: b}
	attr := &gtfs.Attribution{}
	feed.Attributions = append(feed.Attributions, attr)
	feed.FeedInfos = append(feed.FeedInfos, &gtfs.FeedInfo{})
	feed.Agencies["ag"] = &gtfs.Agency{Id: "ag", Attributions: []*gtfs.Attribution{attr}}
	feed.Routes["r"] = &gtfs.Route{Id: "r", Attributions: []*gtfs.Attribution{attr}}

	trip := &gtfs.Trip{Id: "t", Attributions: &[]*gtfs.Attribution{attr}}
	trip.Frequencies = &[]*gtfs.Frequency{{}}
	feed.Trips["t"] = trip
	feed.Transfers[gtfs.TransferKey{From_stop: a, To_stop: b}] = gtfs.TransferVal{}

	NonEssentialDropper{}.Run(feed)

	if len(feed.FareAttributes)+len(feed.Levels)+len(feed.Pathways)+len(feed.Attributions)+len(feed.FeedInfos) != 0 {
		t.Error("non-essential collections not emptied")
	}
	if a.Level != nil {
		t.Error("stop level not cleared")
	}
	if feed.Agencies["ag"].Attributions != nil || feed.Routes["r"].Attributions != nil || trip.Attributions != nil {
		t.Error("entity attributions not cleared")
	}
	if len(feed.Stops) != 2 || len(feed.Agencies) != 1 || len(feed.Routes) != 1 || len(feed.Trips) != 1 {
		t.Error("essential entities were touched")
	}
	if len(feed.Transfers) != 1 || trip.Frequencies == nil || len(*trip.Frequencies) != 1 {
		t.Error("transfers or frequencies were touched")
	}
}
