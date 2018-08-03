package index

import (
	"github.com/tidwall/boxtree/d2"
)

// Index is a geospatial index
type Index struct {
	r d2.BoxTree
}

// New create a new index
func New() *Index {
	return &Index{}
}

// Item represents an index item.
type Item interface {
	Point() (x, y float64)
	Rect() (minX, minY, maxX, maxY float64)
}

// FlexItem can represent a point or a rectangle
type FlexItem struct {
	MinX, MinY, MaxX, MaxY float64
}

// Rect returns the rectangle
func (item *FlexItem) Rect() (minX, minY, maxX, maxY float64) {
	return item.MinX, item.MinY, item.MaxX, item.MaxY
}

// Point returns the point
func (item *FlexItem) Point() (x, y float64) {
	return item.MinX, item.MinY
}

// Insert inserts an item into the index
func (ix *Index) Insert(item Item) {
	minX, minY, maxX, maxY := item.Rect()
	ix.r.Insert([]float64{minX, minY}, []float64{maxX, maxY}, item)
}

// Remove removed an item from the index
func (ix *Index) Remove(item Item) {
	minX, minY, maxX, maxY := item.Rect()
	ix.r.Delete([]float64{minX, minY}, []float64{maxX, maxY}, item)
}

// Count counts all items in the index.
func (ix *Index) Count() int {
	return ix.r.Count()
}

// Bounds returns the minimum bounding rectangle of all items in the index.
func (ix *Index) Bounds() (MinX, MinY, MaxX, MaxY float64) {
	min, max := ix.r.Bounds()
	return min[0], min[1], max[0], max[1]

}

// RemoveAll removes all items from the index.
func (ix *Index) RemoveAll() {
	ix.r = d2.BoxTree{}
}

// KNN returns the nearsest neighbors
func (ix *Index) KNN(x, y float64, iterator func(item interface{}) bool) bool {
	res := true
	ix.r.Nearby([]float64{x, y}, []float64{x, y},
		func(_, _ []float64, item interface{}) bool {
			if !iterator(item) {
				res = false
				return false
			}
			return true
		})
	return res
}

// Search returns all items that intersect the bounding box.
func (ix *Index) Search(minX, minY, maxX, maxY float64,
	iterator func(item interface{}) bool,
) bool {
	res := true
	ix.r.Search([]float64{minX, minY}, []float64{maxX, maxY},
		func(_, _ []float64, item interface{}) bool {
			if !iterator(item) {
				res = false
				return false
			}
			return true
		})
	return res
}
