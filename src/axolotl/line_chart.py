from typing import NamedTuple, Optional
from typing import List
from typing import Literal
from typing import Optional
from typing import Tuple
from typing import Generator
from typing import Any
from datetime import date
from datetime import datetime
import itertools
import re
import math


class Plot(NamedTuple):
    values: List[int|float|datetime|date|None]
    color: Optional[str]
    side: Literal["left", "right"]
    label_end: bool|str = False
    bar_like: bool = False  # display as bars dropping to 0

class Chars:
    LeftEnd='╶'
    RightEnd='╴'
    HLine='─'
    EndBottom='╰'
    EndTop='╭'
    StartTop='╮'
    StartBottom='╯'
    VLine='│'
    # EndCircle='○'
    EndCircle='●'
    BarBottom='╵'

class Chart:
    def __init__(
        self,
        *,
        include_zero=False,  # sets both left and right if true
        include_zero_left=False,
        include_zero_right=False,
        x_labels=[],
        axis_title='',  # sets only left if provided
        left_axis_title='',
        right_axis_title='',
    ):
        self.plots = []
        self.include_zero_left = include_zero or include_zero_left
        self.include_zero_right = include_zero or include_zero_right
        self.x_labels = x_labels
        self.left_axis_title = left_axis_title or axis_title
        self.right_axis_title = right_axis_title

    def add_plot(
        self,
        values,
        *,
        color='',
        side='left',
        label_end=False,
        bar_like=False,
    ):
        if side != 'left' and label_end:
            raise TypeError('Cannot use label_end with side=right')
        self.plots.append(Plot(
            values=values,
            color=color,
            side=side,
            label_end=label_end,
            bar_like=bar_like,
        ))

    def render(self, height=10) -> str:
        """ render the plot, using height as the _inner_ height """
        inner_height = height
        inner_width = max(len(plot.values) for plot in self.plots) + 1

        left_axis = self.compute_axis(
            [p for p in self.plots if p.side == 'left'],
            self.include_zero_left,
            inner_height,
        )
        right_axis = self.compute_axis(
            [p for p in self.plots if p.side == 'right'],
            self.include_zero_right,
            inner_height,
        )

        if(
            (not left_axis or left_axis.is_constant)
            and (not right_axis or right_axis.is_constant)
            and height > 1
        ):
            return self.render(height=1)

        if not left_axis and not right_axis:
            return (
                "  ╢ [empty chart] \n"
                "  ╚═══════════════"
            )

        inner_grid = [
            [' '] * inner_width
            for _ in range(0, inner_height)
        ]

        for plot in self.plots:
            self.draw_plot(
                inner_grid,
                plot,
                left_axis if plot.side == 'left' else right_axis
            )

        if left_axis:
            left_axis_grid = ["      \033[2m║\033[0m"] * inner_height
            for y, value in left_axis.get_labels():
                left_axis_grid[y] = self.format_label(value).rjust(5) + ' \033[2m╢\033[0m'
        else:
            left_axis_grid = [''] * inner_height

        if right_axis:
            right_axis_grid = ["\033[2m║\033[0m      "] * inner_height
            for y, value in right_axis.get_labels():
                right_axis_grid[y] = "\033[2m╟\033[0m " + self.format_label(value).ljust(5)
        else:
            right_axis_grid = [''] * inner_height

        left_axis_width = 7 if left_axis else 0
        right_axis_width = 7 if right_axis else 0
        outer_width = inner_width + left_axis_width + right_axis_width

        result_lines = []
        if self.left_axis_title or self.right_axis_title:
            num_spaces = max(0, outer_width - len(self.left_axis_title) - len(self.right_axis_title))
            result_lines.append(
                f"{self.left_axis_title}{' ' * num_spaces}{self.right_axis_title}"
            )

        for l, inner, r in reversed(list(zip(left_axis_grid, inner_grid, right_axis_grid))):
            result_lines.append(l + ''.join(inner) + r)

        result_lines.append(
            "\033[2m"
            + ' ' * (left_axis_width - 1)
            + ('╚' if left_axis else ' ')
            + '═' * inner_width
            + ('╝' if right_axis else ' ')
            + ' ' * (right_axis_width - 1)
            + "\033[0m"
        )

        return '\n'.join(result_lines)

    def compute_axis(self, plots, include_zero, height):
        if not plots:
            return None

        max_value = max((v for plot in plots for v in plot.values if v is not None), default=0.0) or 0.0
        min_value = min((v for plot in plots for v in plot.values if v is not None), default=0.0) or 0.0

        if include_zero:
            max_value = max(max_value, 0.0)
            min_value = min(min_value, 0.0)

        if abs(max_value - min_value) < 1e-12:
            class NoHeightAxis:
                is_constant = True
                def get_y(self, value) -> int:
                    return height // 2

                def get_labels(self) -> Generator[Tuple[int, Any]]:
                    yield (height // 2, max_value)

            return NoHeightAxis()

        class Axis:
            is_constant = False
            def get_y(self, value) -> int:
                return round((value - min_value) / (max_value - min_value) * (height - 1))

            def get_labels(self) -> Generator[Tuple[int, Any]]:
                mark_values = [min_value, max_value]

                if include_zero:
                    mark_values += [0.0]

                delta = max_value - min_value
                order = math.floor(math.log10(delta))
                step = 10 ** order
                start = round(min_value, -order)
                if start < min_value:
                    start += step

                mark_values += [
                    mark
                    for mark
                    in frange(start, max_value - 0.5 * step, step)
                ]

                for v in reversed(mark_values):
                    yield (self.get_y(v), v)

        return Axis()

    def draw_plot(self, grid, plot, axis):
        def color(c: str) -> str:
            return plot.color + c + '\033[0m'
        for x, (a, b) in enumerate(itertools.pairwise([None, *plot.values, None])):
            ay = None if a is None else axis.get_y(a)
            by = None if b is None else axis.get_y(b)
            if a is None and b is None:
                pass
            elif a is None:
                if plot.bar_like and b > 0:
                    zy = max(0, axis.get_y(0.0))
                    for y in range(zy + 1, by):
                        grid[y][x] = color(Chars.VLine)
                    if by > zy:
                        grid[zy][x] = color(Chars.BarBottom)
                        grid[by][x] = color(Chars.EndTop)
                    else:
                        grid[by][x] = color(Chars.LeftEnd)
                else:
                    grid[by][x] = color(Chars.LeftEnd)
            elif b is None:
                if plot.bar_like and a > 0:
                    zy = max(0, axis.get_y(0.0))
                    for y in range(zy + 1, ay):
                        grid[y][x] = color(Chars.VLine)
                    if ay > zy:
                        grid[zy][x] = color(Chars.BarBottom)
                        grid[ay][x] = color(Chars.StartTop)
                    else:
                        grid[ay][x] = color(Chars.RightEnd)
                else:
                    grid[ay][x] = color(Chars.RightEnd)
            elif ay == by:
                grid[ay][x] = color(Chars.HLine)
            elif ay < by:
                # going up
                grid[ay][x] = color(Chars.StartBottom)
                for y in range(ay + 1, by):
                    grid[y][x] = color(Chars.VLine)
                grid[by][x] = color(Chars.EndTop)
            elif ay > by:
                # going down
                grid[ay][x] = color(Chars.StartTop)
                for y in range(by + 1, ay):
                    grid[y][x] = color(Chars.VLine)
                grid[by][x] = color(Chars.EndBottom)
            else:
                raise Exception('unexpected plot error')

            if ay is not None and x == len(plot.values):
                if not plot.bar_like:
                    grid[ay][x] = color(Chars.EndCircle)
                if plot.label_end:
                    if plot.label_end is True:
                        label = self.format_label(a, 7)
                    else:
                        label = plot.label_end
                    grid[ay].append(f" {color(label)}")

    def format_label(self, v, max_len=5):
        suffix = ""
        value = v

        if abs(v) >= 1e12:
            value, suffix = v / 1e12, "T"
        elif abs(v) >= 1e9:
            value, suffix = v / 1e9, "B"
        elif abs(v) >= 1e6:
            value, suffix = v / 1e6, "M"
        elif abs(v) >= 1e3:
            value, suffix = v / 1e3, "k"

        # try decreasing precision until it fits
        for p in range(max_len, -1, -1):
            s = re.sub(
                r"\.0+$",
                "",
                str(round(value, p)),
            ) + suffix
            # s = f"{value:.{p}f}{suffix}"
            if len(s) <= max_len:
                return s
        return f"{round(value)}{suffix}"

def frange(start, end, step):
    assert step > 0
    n = 0
    # adding step directly to start causes precision issues when start >> step
    # so instead we use n
    while start + step * n < end:
        yield start + step * n
        n += 1

def arr_to_dots(arr):
    def normalized_bottom_to_unicode(a, b):
        # dot bit indices:
        # 0 3
        # 1 4
        # 2 5
        # 6 7
        # ex: 2^0 lights up the NW dot
        #     2^3 lights up the NE dot
        #     etc.
        val = sum([
            2 ** 6 if a >= 1 else 0,
            2 ** 2 if a >= 2 else 0,
            2 ** 1 if a >= 3 else 0,
            2 ** 0 if a >= 4 else 0,
            2 ** 7 if b >= 1 else 0,
            2 ** 5 if b >= 2 else 0,
            2 ** 4 if b >= 3 else 0,
            2 ** 3 if b >= 4 else 0,
        ])
        return chr(0x2800 + val)
    result = ''
    for pair in itertools.batched(arr, 2):
        a = pair[0]
        b = pair[1] if len(pair) == 2 else 0
        result += normalized_bottom_to_unicode(a, b)
    return result

