import sys
from configuration import ConfigDict
from collections import namedtuple

Point = namedtuple('Point', ('x', 'y'))

def identity(i): return i

def getminmax(coords):
    """
    Given an iterable of (x, y), return minx, miny, maxx, maxy.
    """
    i = iter(coords)
    try:
        x, y = i.next()
        minx = maxx = x
        miny = maxy = y
    except StopIteration:
        return None, None, None, None

    for x, y in i:
        if x < minx:
            minx = x
        if x > maxx:
            maxx = x
        if y < miny:
            miny = y
        if y > maxy:
            maxy = y

    return minx, miny, maxx, maxy

class Plot(object):
    def __init__(self, drawing, width, height,
                 minx, miny, maxx, maxy,
                 **kwargs):
        self.d = drawing
        self.width = width
        self.height = height
        self.minx = minx
        self.miny = miny
        self.maxx = maxx
        self.maxy = maxy

        self.config  = ConfigDict(subconfigs=('xaxis', 'yaxis'),
                                  margin=4,
                                  fontSize=10,
                                  lineScale=1.3,
                                  tickLength=4)
        self.config.xaxis.transform = identity
        self.config.yaxis.transform = identity
        self.config.update(kwargs)

        self.root = self.d.g()

    def fontHeight(self):
        return self.config.fontSize * self.config.lineScale

    def innerBounds(self):
        """Return l, t, r, b"""
        leftTickLabelSize = self.config.yaxis.get('labelDepth', self.fontHeight)
        bottomTickLabelSize = self.config.xaxis.get('labelDepth', self.fontHeight);

        left = self.config.margin * 2 + self.fontHeight() + leftTickLabelSize + self.config.tickLength
        right = self.width - self.config.margin
        top = self.config.margin
        bottom = self.height - self.config.margin - bottomTickLabelSize - self.fontHeight() * 2 - self.config.tickLength

        return left, top, right, bottom

    def transform(self, x, y):
        x = self.config.xaxis.transform(x)
        y = self.config.yaxis.transform(y)

        l, t, r, b = self.innerBounds()

        minx = self.config.xaxis.transform(self.minx)
        maxx = self.config.xaxis.transform(self.maxx)

        miny = self.config.yaxis.transform(self.miny)
        maxy = self.config.yaxis.transform(self.maxy)

        x2 = l + float(r - l) / (maxx - minx) * (x - minx)
        y2 = b - (float(b - t) / (maxy - miny) * (y - miny))

        return x2, y2

    def drawAxes(self):
        l, t, r, b = self.innerBounds()

        self.root.add(self.d.path(('M', l, t,
                                   'L', l, b,
                                   'L', r, b), class_="border"))

    def printTicks(self, axis, ticks):
        """
        Print the ticks for an axis.

        axis: "x" or "y"
        ticks: iterable of (value, string)
        """

        def printXTick(v, label):
            x, y = self.transform(v, self.miny)
            self.root.add(self.d.line((x, y), (x, y + self.config.tickLength),
                                      class_='tick'))
            if len(label):
                pos = (x, y + self.config.tickLength + self.config.margin + self.fontHeight())
                t = self.root.add(self.d.text(label, pos,
                                              class_='tickLabel xaxis'))
                rotate = self.config.xaxis.get('labelRotate', 0)
                t.rotate(rotate, pos)

        def printYTick(v, label):
            x, y = self.transform(self.minx, v)
            self.root.add(self.d.line((x, y), (x - self.config.tickLength, y),
                                      class_='tick'))

            maxx, oy = self.transform(self.maxx, v)
            self.root.add(self.d.line((x, y), (maxx, y), class_='crossTick'))

            if len(label):
                pos = (x - self.config.tickLength - self.config.margin, y)
                t = self.root.add(self.d.text(label, pos,
                                              class_='tickLabel yaxis'))
                rotate = self.config.yaxis.get('labelRotate', -90)
                t.rotate(rotate, pos)

        fn = {'x': printXTick,
              'y': printYTick}[axis]
        for v, label in ticks:
            fn(v, label)

    def printXAxisLabel(self, label):
        self.root.add(self.d.text(label,
                                  (self.width / 2, self.height - self.config.margin),
                                  class_="axisLabel xaxis"))

    def printYAxisLabel(self, label):
        pos = (self.config.margin + self.fontHeight(), self.height / 2)
        t = self.root.add(self.d.text(label, pos, class_="axisLabel yaxis"))
        t.rotate(-90, pos)

def indexPosition(v):
    return v[0], v[1]

def xyPosition(v):
    return v.x, v.y
