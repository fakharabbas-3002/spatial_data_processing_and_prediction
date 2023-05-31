from shapely.geometry import Point, Polygon, LineString, MultiPolygon, shape
from shapely.ops import transform, split
import shapefile
import pyproj



def ret_index(i, nx, ny):
    return (ny - int(i / nx)) - 1, int(i % nx)

def grid_index(grids, point):
    for i in range(len(grids)):
        if point.within(grids[i]):
            return i
    return -1

def transform(x ,nx, ny):
    
    rec = [(x[0], x[1]), (x[0], x[3]), (x[2], x[3]), (x[2], x[1])]
    #print(rec)
    polygon = Polygon(rec)

    # compute splitter
    minx, miny, maxx, maxy = polygon.bounds
    dx = (maxx - minx) / nx  # width of a small part
    dy = (maxy - miny) / ny  # height of a small part
    horizontal_splitters = [LineString([(minx, miny + i * dy), (maxx, miny + i * dy)]) for i in range(ny)]
    vertical_splitters = [LineString([(minx + i * dx, miny), (minx + i * dx, maxy)]) for i in range(nx)]
    splitters = horizontal_splitters + vertical_splitters

    # split
    result = polygon
    for splitter in splitters:
        result = MultiPolygon(split(result, splitter))
    grids = list(result.geoms)

    loc_geom_dict = {}
    for i in range(len(grids)):
        loc_geom_dict[ret_index(i, nx, ny)] = grids[i]
    
    return grids, loc_geom_dict
