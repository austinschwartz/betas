from bokeh.plotting import figure
from bokeh.palettes import Spectral11, inferno, linear_palette
from bokeh.embed import components


def chart(df):
    """
    Builds a Bokeh plot for given stock betas
    :param df: Pandas DataFrame containing stock betas
    :return: Bokeh script, div components for rendering plot
    """
    tooltips = [("Beta", "$y")]

    p = figure(width=800,
               height=400,
               x_axis_type='datetime',
               tools='wheel_zoom,pan,box_zoom,reset',
               tooltips=tooltips)

    col_len = len(df.columns)

    # Spectral palette is really nice, but only has 11 colors
    if col_len <= 11:
        palette = linear_palette(Spectral11, col_len)
    else:
        palette = inferno(col_len)

    for i in range(col_len):
        p.line(df.index,
               df[df.columns[i]],
               color=palette[i],
               line_width=2,
               legend=df.columns[i],
               alpha=0.8,
               muted_alpha=0.2,
               muted_color=palette[i])

    p.legend.location = "top_left"
    p.legend.click_policy = "mute"

    return components(p)
