package shpdraw

import (
	"fmt"
	"image"
	"image/color"

	"github.com/jonas-p/go-shp"
	"github.com/llgcode/draw2d"
	"github.com/llgcode/draw2d/draw2dimg"
)

func intersect(b1 shp.Box, b2 shp.Box) bool {
	return !(b1.MinX > b2.MaxX || b1.MaxX < b2.MinX ||
		b1.MinY > b2.MaxY || b1.MaxY < b2.MinY)
}

func LoadAndFilterShapes(path string, box shp.Box) ([]shp.Shape, error) {
	reader, err := shp.Open(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	kept := []shp.Shape{}
	for reader.Next() {
		_, p := reader.Shape()
		if !intersect(box, p.BBox()) {
			continue
		}
		kept = append(kept, p)
	}
	return kept, nil
}

func Draw(img *image.RGBA, col color.RGBA, box shp.Box, shape shp.Shape) error {
	poly, ok := shape.(*shp.Polygon)
	if !ok {
		return fmt.Errorf("cannot draw non-polygon shape")
	}

	gc := draw2dimg.NewGraphicContext(img)
	gc.SetStrokeColor(col)
	gc.SetLineWidth(1)

	rect := img.Bounds()
	dx := float64(rect.Max.X-rect.Min.X) / (box.MaxX - box.MinX)
	dy := float64(rect.Max.Y-rect.Min.Y) / (box.MaxY - box.MinY)

	for i, start := range poly.Parts {
		end := len(poly.Points)
		if i+1 < len(poly.Parts) {
			end = int(poly.Parts[i+1])
		}
		part := poly.Points[start:end]

		path := draw2d.Path{}
		for j, p := range part {
			x := ((p.X - box.MinX) * dx)
			y := ((box.MaxY - p.Y) * dy)
			if j == 0 {
				path.MoveTo(x, y)
			} else {
				path.LineTo(x, y)
			}
		}
		path.Close()
		gc.Stroke(&path)
	}
	return nil
}
