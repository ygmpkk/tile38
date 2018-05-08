package index

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func init() {
	seed := time.Now().UnixNano()
	fmt.Printf("seed: %d\n", seed)
	rand.Seed(seed)
}

func randf(min, max float64) float64 {
	return rand.Float64()*(max-min) + min
}

func randRect() (minX, minY, maxX, maxY float64) {
	minX, minY = rand.Float64()*360-180, rand.Float64()*180-90
	maxX, maxY = rand.Float64()*360-180, rand.Float64()*180-90
	if minX > maxX {
		minX, maxX = maxX, minX
	}
	if minY > maxY {
		minY, maxY = maxY, minY
	}
	return
}

func wp(minX, minY, maxX, maxY float64) *FlexItem {
	return &FlexItem{
		MinX: minX,
		MinY: minY,
		MaxX: maxX,
		MaxY: maxY,
	}
}

func TestRandomInserts(t *testing.T) {

	l := 100000
	tr := New()
	i := 0
	var gitems []*FlexItem
	var nitems []*FlexItem

	start := time.Now()
	for ; i < l; i++ {
		item := wp(randRect())
		tr.Insert(item)
		if item.MinX >= -180 && item.MinY >= -90 && item.MaxX <= 180 && item.MaxY <= 90 {
			gitems = append(gitems, item)
		} else {
			nitems = append(nitems, item)
		}
	}
	insrdur := time.Now().Sub(start)
	count := 0

	count = tr.Count()
	if count != l {
		t.Fatalf("count == %d, expect %d", count, l)
	}
	count = 0
	items := make([]Item, 0, l)
	tr.Search(-180, -90, +180, +90, func(item interface{}) bool {
		count++
		items = append(items, item.(Item))
		return true
	})

	if count != len(gitems) {
		t.Fatalf("count == %d, expect %d", count, len(gitems))
	}
	start = time.Now()
	count1 := 0
	tr.Search(33, -115, 34, -114, func(item interface{}) bool {
		count1++
		return true
	})
	searchdur1 := time.Now().Sub(start)

	start = time.Now()
	count2 := 0

	tr.Search(33-180, -115-360, 34-180, -114-360, func(item interface{}) bool {
		count2++
		return true
	})
	searchdur2 := time.Now().Sub(start)

	start = time.Now()
	count3 := 0
	tr.Search(-10, 170, 20, 200, func(item interface{}) bool {
		count3++
		return true
	})
	searchdur3 := time.Now().Sub(start)

	fmt.Printf("Randomly inserted %d rects in %s.\n", l, insrdur.String())
	fmt.Printf("Searched %d items in %s.\n", count1, searchdur1.String())
	fmt.Printf("Searched %d items in %s.\n", count2, searchdur2.String())
	fmt.Printf("Searched %d items in %s.\n", count3, searchdur3.String())

	tr.Search(-10, 170, 20, 200, func(item interface{}) bool {
		lat1, lon1, lat2, lon2 := item.(Item).Rect()
		if lat1 == lat2 && lon1 == lon2 {
			return false
		}
		return true
	})

	tr.Search(-10, 170, 20, 200, func(item interface{}) bool {
		lat1, lon1, lat2, lon2 := item.(Item).Rect()
		if lat1 != lat2 || lon1 != lon2 {
			return false
		}
		return true
	})

	// Remove all of the elements
	for _, item := range items {
		tr.Remove(item)
	}

	count = tr.Count()
	if count != 0 {
		t.Fatalf("count == %d, expect %d", count, 0)
	}

	tr.RemoveAll()
	/*	if tr.getQTreeItem(nil) != nil {
			t.Fatal("getQTreeItem(nil) should return nil")
		}
	*/
	// if tr.getRTreeItem(nil) != nil {
	// 	t.Fatal("getRTreeItem(nil) should return nil")
	// }
}

func TestMemory(t *testing.T) {
	rand.Seed(0)
	l := 100000
	tr := New()
	for i := 0; i < l; i++ {
		swLat, swLon, neLat, neLon := randRect()
		if rand.Int()%2 == 0 { // one in three chance that the rect is actually a point.
			neLat, neLon = swLat, swLon
		}
		tr.Insert(wp(swLat, swLon, neLat, neLon))
	}
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	const PtrSize = 32 << uintptr(uint64(^uintptr(0))>>63)
	fmt.Printf("Memory consumption is %d bytes/object. Pointers are %d bytes.\n", int(m.HeapAlloc)/tr.Count(), PtrSize/8)
}

func TestInsertVarious(t *testing.T) {
	var count int
	tr := New()
	item := wp(33, -115, 33, -115)
	tr.Insert(item)
	count = tr.Count()
	if count != 1 {
		t.Fatalf("count = %d, expect 1", count)
	}
	tr.Remove(item)
	count = tr.Count()
	if count != 0 {
		t.Fatalf("count = %d, expect 0", count)
	}
	tr.Insert(item)
	count = tr.Count()
	if count != 1 {
		t.Fatalf("count = %d, expect 1", count)
	}
	found := false
	tr.Search(-90, -180, 90, 180, func(item2 interface{}) bool {
		if item2.(Item) == item {
			found = true
		}
		return true
	})
	if !found {
		t.Fatal("did not find item")
	}
}

func BenchmarkInsertRect(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	tr := New()
	for i := 0; i < b.N; i++ {
		swLat, swLon, neLat, neLon := randRect()
		tr.Insert(wp(swLat, swLon, neLat, neLon))
	}
}

func BenchmarkInsertPoint(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	tr := New()
	for i := 0; i < b.N; i++ {
		swLat, swLon, _, _ := randRect()
		tr.Insert(wp(swLat, swLon, swLat, swLon))
	}
}

func BenchmarkInsertEither(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	tr := New()
	for i := 0; i < b.N; i++ {
		swLat, swLon, neLat, neLon := randRect()
		if rand.Int()%3 == 0 { // one in three chance that the rect is actually a point.
			neLat, neLon = swLat, swLon
		}
		tr.Insert(wp(swLat, swLon, neLat, neLon))
	}
}

// func BenchmarkSearchRect(b *testing.B) {
// 	rand.Seed(time.Now().UnixNano())
// 	tr := New()
// 	for i := 0; i < 100000; i++ {
// 		swLat, swLon, neLat, neLon := randRect()
// 		tr.Insert(swLat, swLon, neLat, neLon)
// 	}
// 	b.ResetTimer()
// 	count := 0
// 	//for i := 0; i < b.N; i++ {
// 	tr.Search(0, -180, 90, 180, func(id int) bool {
// 		count++
// 		return true
// 	})
// 	//}
// 	println(count)
// }
