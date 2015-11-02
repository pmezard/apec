package main

import (
	"image"
	"image/color"
	"image/png"
	"os"
	"sort"

	"github.com/blevesearch/bleve"
)

func hueToRgb(p, q, t float64) float64 {
	if t < 0 {
		t += 1
	}
	if t > 1 {
		t -= 1
	}
	switch {
	case t < 1/6.:
		return p + (q-p)*6*t
	case t < 1/2.:
		return q
	case t < 2/3.:
		return p + (q-p)*(2/3.-t)*6
	}
	return p
}

func hslToRgb(h, s, l float64) (r, g, b float64) {
	if s == 0 {
		r = l
		g = l
		b = l
	} else {
		q := l * (1 + s)
		if l >= 0.5 {
			q = l + s - l*s
		}
		p := 2*l - q
		r = hueToRgb(p, q, h+1/3.)
		g = hueToRgb(p, q, h)
		b = hueToRgb(p, q, h-1/3.)
	}
	return
}

// Turns values in [0, 1] into RGBA colors.
func getColor(v float64) color.RGBA {
	// Color paths are easily described in HSL space.
	h := (1 - v)
	s := 1.0
	l := 0.5 * v
	r, g, b := hslToRgb(h, s, l)
	return color.RGBA{uint8(255*r + 0.5), uint8(255*g + 0.5), uint8(255*b + 0.5), 255}
}

type Point struct {
	Lat float64
	Lon float64
}

// listPoints returns the location of offers satisfying specified full-text
// query. If query is empty, it returns all locations. If not nil, spatial is
// exploited as a cache to fetch indexed offers and their locations, which
// avoid store lookups.
func listPoints(store *Store, index bleve.Index, spatial *SpatialIndex,
	query string) ([]Point, error) {

	var ids []string
	if query == "" {
		if spatial != nil {
			ids = spatial.List()
		} else {
			list, err := store.List()
			if err != nil {
				return nil, err
			}
			ids = list
		}
	} else {
		q := makeSearchQuery(query, nil)
		rq := bleve.NewSearchRequest(q)
		rq.Size = 20000
		res, err := index.Search(rq)
		if err != nil {
			return nil, err
		}
		for _, doc := range res.Hits {
			ids = append(ids, doc.ID)
		}
	}
	points := make([]Point, 0, len(ids))
	for _, id := range ids {
		var p *Point
		if spatial != nil {
			offer := spatial.Get(id)
			if offer != nil {
				p = &offer.Point
			}
		}
		if p == nil {
			loc, _, err := store.GetLocation(id)
			if err != nil {
				return nil, err
			}
			if loc == nil {
				continue
			}
			p = &Point{
				Lat: loc.Lat,
				Lon: loc.Lon,
			}
		}
		points = append(points, *p)
	}
	return points, nil
}

type Grid struct {
	Width  int
	Height int
	Values []int
}

func NewGrid(w, h int) *Grid {
	return &Grid{
		Width:  w,
		Height: h,
		Values: make([]int, w*h),
	}
}

func (g *Grid) Add(i, j int) {
	g.Values[j*g.Width+i]++
}

func (g *Grid) Get(i, j int) int {
	return g.Values[j*g.Width+i]
}

func (g *Grid) Set(i, j, v int) {
	g.Values[j*g.Width+i] = v
}

func makeMapGrid(points []Point, w, h int) *Grid {
	// France bounding box
	minX, maxX := -5.1406, 9.55932
	minY, maxY := 41.33374, 51.089062
	kept := []Point{}
	for _, p := range points {
		if p.Lat < minY || p.Lat > maxY || p.Lon < minX || p.Lon > maxX {
			continue
		}
		kept = append(kept, p)
	}
	points = kept
	cX := 0.5 * (minX + maxX)
	cY := 0.5 * (minY + maxY)
	width := 1.1 * (maxX - minX)
	height := 1.1 * (maxY - minY)
	minX = cX - 0.5*width
	maxX = cX + 0.5*width
	minY = cY - 0.5*height
	maxY = cY + 0.5*height

	cellWidth := width / float64(w)
	cellHeight := height / float64(h)
	grid := NewGrid(w, h)
	for _, p := range points {
		i := int((p.Lon - minX) / cellWidth)
		j := int((p.Lat - minY) / cellHeight)
		if i >= grid.Width {
			i = grid.Width - 1
		}
		if j >= grid.Height {
			j = grid.Height - 1
		}
		grid.Add(i, j)
	}
	return grid
}

func convolveGrid(grid *Grid) *Grid {
	// France is roughly 1000x1000km, this kernel radius around 10/20km.
	kw, kh := 21, 21
	cx, cy := kw/2, kh/2
	ker := make([]float64, kw*kh)
	dmax := float64(cx * cx)
	for j := 0; j < kh; j++ {
		for i := 0; i < kw; i++ {
			dx := float64(i - cx)
			dy := float64(j - cy)
			d := dx*dx + dy*dy
			w := (dmax - d) / dmax
			if w < 0 {
				w = 0
			}
			ker[j*kw+i] = w * w
		}
	}
	output := NewGrid(grid.Width, grid.Height)
	for j := 0; j < grid.Height; j++ {
		for i := 0; i < grid.Width; i++ {
			total := 0.
			for jj := 0; jj < kh; jj++ {
				for ii := 0; ii < kw; ii++ {
					x := i + ii - cx
					y := j + jj - cy
					if x < 0 || x >= grid.Width || y < 0 || y >= grid.Height {
						continue
					}
					total += ker[jj*kw+ii] * float64(grid.Get(x, y))
				}
			}
			output.Set(i, j, int(total))
		}
	}
	return output
}

func drawGrid(grid *Grid) *image.RGBA {
	rect := image.Rect(0, 0, grid.Width, grid.Height)
	img := image.NewRGBA(rect)
	counts := map[int]int{}
	values := []int{}
	for j := 0; j < grid.Height; j++ {
		for i := 0; i < grid.Width; i++ {
			n := grid.Get(i, j)
			if n <= 0 {
				continue
			}
			if counts[n] == 0 {
				values = append(values, n)
			}
			counts[n]++
		}
	}
	total := 0
	sort.Ints(values)
	for _, v := range values {
		total += counts[v]
		counts[v] = total
	}
	for j := 0; j < grid.Height; j++ {
		for i := 0; i < grid.Width; i++ {
			v := float64(counts[grid.Get(i, j)]) / float64(total)
			img.Set(i, grid.Height-j-1, getColor(v))
		}
	}
	return img
}

func writeImage(img image.Image, path string) error {
	fp, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fp.Close()
	err = png.Encode(fp, img)
	if err != nil {
		return err
	}
	return fp.Close()
}

var (
	densityCmd   = app.Command("density", "generate density map")
	densityFile  = densityCmd.Arg("file", "output image file").Required().String()
	densityQuery = densityCmd.Arg("query", "query string").String()
)

func densityFn(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	defer store.Close()

	index, err := OpenOfferIndex(cfg.Index())
	if err != nil {
		return err
	}

	points, err := listPoints(store, index, nil, *densityQuery)
	if err != nil {
		return err
	}
	grid := makeMapGrid(points, 1000, 1000)
	grid = convolveGrid(grid)
	img := drawGrid(grid)
	return writeImage(img, *densityFile)
}
