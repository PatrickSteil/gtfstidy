package processors

import (
	"testing"

	"github.com/patrickbr/gtfsparser"
	"github.com/patrickbr/gtfsparser/gtfs"
)

func connService(id string, start, end gtfs.Date) *gtfs.Service {
	s := gtfs.EmptyService()
	s.SetId(id)
	for i := 0; i < 7; i++ {
		s.SetDaymap(i, true)
	}
	s.SetStart_date(start)
	s.SetEnd_date(end)
	return s
}

func connTrip(feed *gtfsparser.Feed, id string, svc *gtfs.Service, route *gtfs.Route, stops ...string) {
	trip := &gtfs.Trip{Id: id, Service: svc, Route: route}
	for i, name := range stops {
		var st gtfs.StopTime
		st.SetStop(feed.Stops[name])
		st.SetSequence(i + 1)
		trip.StopTimes = append(trip.StopTimes, st)
	}
	feed.Trips[id] = trip
}

// two networks {A,B,C} (days 1-3 Jan 2024) and {D,E} (days 10-12 Jan 2024,
// plus a trip on days 1-3 Jan), and a stop F not served by any trip
func connFeed() *gtfsparser.Feed {
	feed := gtfsparser.NewFeed()
	for _, n := range []string{"A", "B", "C", "D", "E", "F"} {
		feed.Stops[n] = &gtfs.Stop{Id: n, Name: n}
	}
	early := connService("early", gtfs.NewDate(1, 1, 2024), gtfs.NewDate(3, 1, 2024))
	late := connService("late", gtfs.NewDate(10, 1, 2024), gtfs.NewDate(12, 1, 2024))
	r1 := &gtfs.Route{Id: "r1"}
	r2 := &gtfs.Route{Id: "r2"}

	connTrip(feed, "t1", early, r1, "A", "B", "C")
	connTrip(feed, "t2", late, r2, "D", "E")
	connTrip(feed, "t3", early, r2, "D", "E")
	return feed
}

func TestConnectivityComponents(t *testing.T) {
	rep := ConnectivityAnalyzer{}.Analyze(connFeed())

	if rep.ServedStops != 5 || rep.UnservedStops != 1 {
		t.Errorf("served/unserved = %d/%d", rep.ServedStops, rep.UnservedStops)
	}
	if len(rep.Components) != 2 {
		t.Fatalf("components = %d", len(rep.Components))
	}
	if c := rep.Components[0]; c.Stops != 3 || c.Routes != 1 || c.Trips != 1 {
		t.Errorf("largest = %+v", c)
	}
	if c := rep.Components[1]; c.Stops != 2 || c.Routes != 1 || c.Trips != 2 {
		t.Errorf("second = %+v", c)
	}
}

func TestConnectivityTransferJoins(t *testing.T) {
	feed := connFeed()
	feed.Transfers[gtfs.TransferKey{From_stop: feed.Stops["C"], To_stop: feed.Stops["D"]}] = gtfs.TransferVal{}
	rep := ConnectivityAnalyzer{}.Analyze(feed)

	if len(rep.Components) != 1 || rep.Components[0].Stops != 5 {
		t.Errorf("components = %+v", rep.Components)
	}
}

func TestConnectivityParentJoins(t *testing.T) {
	feed := connFeed()
	feed.Stops["C"].Parent_station = feed.Stops["F"]
	feed.Stops["D"].Parent_station = feed.Stops["F"]
	rep := ConnectivityAnalyzer{}.Analyze(feed)

	if len(rep.Components) != 1 || rep.Components[0].Stops != 5 {
		t.Errorf("components = %+v", rep.Components)
	}
}

func TestConnectivityCalendarGaps(t *testing.T) {
	rep := ConnectivityAnalyzer{}.Analyze(connFeed())

	if rep.FirstDay != gtfs.NewDate(1, 1, 2024) || rep.LastDay != gtfs.NewDate(12, 1, 2024) {
		t.Errorf("range = %v - %v", rep.FirstDay, rep.LastDay)
	}
	if len(rep.FeedGaps) != 1 {
		t.Fatalf("feed gaps = %+v", rep.FeedGaps)
	}
	g := rep.FeedGaps[0]
	if g.Start != gtfs.NewDate(4, 1, 2024) || g.End != gtfs.NewDate(9, 1, 2024) || g.Days != 6 {
		t.Errorf("gap = %+v", g)
	}

	// A, B, C are served on consecutive days only, D and E have a gap
	if c := rep.Components[0]; c.MaxStopGap != 0 || c.StopsWithGap != 0 {
		t.Errorf("A,B,C = %+v", c)
	}
	if c := rep.Components[1]; c.MaxStopGap != 6 || c.StopsWithGap != 2 || c.MedianStopGap != 6 {
		t.Errorf("D,E = %+v", c)
	}
}

func TestConnectivityEmptyFeed(t *testing.T) {
	rep := ConnectivityAnalyzer{}.Analyze(gtfsparser.NewFeed())
	if len(rep.Components) != 0 || !rep.FirstDay.IsEmpty() || len(rep.FeedGaps) != 0 {
		t.Errorf("rep = %+v", rep)
	}
}
