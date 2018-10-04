#################################################################################
# Spark ML Pipeline Display Function
# This function was originally created by Peter Knight from GE Aviaiton. 
# It is designed to create an interactive Bokeh plot of Spark ML Pipelines. 
# It has an on hover display that shows the Parameters and values for each stage.
#################################################################################

# these imports are for display in Databricks notebook
from bokeh.embed import file_html
from bokeh.resources import CDN

# these imports are for display in Zeppelin notebook
from bokeh.plotting import output_notebook
import bkzep
output_notebook(notebook_type='zeppelin')

# these imports are for the plotting function
from bokeh.plotting import figure, ColumnDataSource
from bokeh.models import Arrow, VeeHead, LabelSet, HoverTool, Label
from pyspark.ml import Pipeline, PipelineModel, Estimator, Transformer
import pandas as pd
import textwrap

def display_pipeline(pipeline, plot_width = 800):
    p = pipeline
    if isinstance(p, Pipeline):
        stages = p.getStages()
    elif isinstance(p, PipelineModel):
        stages = p.stages
        
    arrow_len = 20
    box_height = 30
    box_width = int(plot_width/2)
    num_boxes = len(stages)
    plot_height = int(300 + num_boxes * (box_height + arrow_len))
    plot_width = int(2*box_width)
    bottom_pos = plot_height - box_height - 200
    left_pos = box_width/3
    
    # see https://bokeh.pydata.org/en/latest/docs/user_guide/tools.html#custom-tooltip
    hover = HoverTool(tooltips='<html><head><style> \
        table { \
            font-family: arial, sans-serif; \
            border-collapse: collapse; \
            table-layout: fixed; \
            width: ' + str(box_width-20) + '; \
        } \
        td, th { \
            border: 1px solid #dddddd; font-size:10px; \
            text-align: left; \
            padding: 1px; \
        } \
        tr:nth-child(even) { \
            background-color: #dddddd; font-size:10px; \
        } \
        </style></head><div style="width:' + str(box_width-20) + '"><p style="font-size:10px"><font color="#26AAE1">Stage: </font>@name <br> \
        <font color="#26aae1">Type: </font>@type <br> \
        @desc{safe}</p></div>', formatters={"desc": "printf"})
        
    plot = figure(plot_width=plot_width, plot_height=plot_height, tools=[hover], y_range=[0,plot_height], x_range=[0,plot_width])
    plot.xgrid.visible = False
    plot.ygrid.visible = False
    plot.axis.visible = False
    
    # define colours for each type
    d = {'Estimator': "#FF6969", #red
        'Transformer': "#26AAE1", #blue
        'Pipeline': "#B3DE69", #green
        'PipelineModel': "#FFFF69", #yellow
        } 
    bottom = plot_height - box_height - 20
    
    # plot the legend
    for type, box_color in d.items():
        plot.quad(top=bottom+box_height, bottom=bottom, left=20, right=120, color=box_color, line_color='black')
        plot.add_layout(Label(x=25, y=bottom + 5, 
                     text=type, render_mode='css', text_font_size='10pt',
                     border_line_color='black', border_line_alpha=0,
                     background_fill_color='#FFFFFF', background_fill_alpha=0))
        bottom -= box_height + 5
    
    first_box = True
    rows_list = [] # this will be a list of the rows in the final table - each row being a dictionary
    for stage in stages:
        
        if isinstance(stage, Pipeline):
            stage_type = "Pipeline"
        elif isinstance(stage, PipelineModel):
            stage_type = "PipelineModel"
        elif isinstance(stage, Estimator):
            stage_type = "Estimator"
        elif isinstance(stage, Transformer):
            stage_type = "Transformer"
        
        box_color = d[stage_type]
        
        #set up the table to display on hover with the Params and their descriptions
        values_html = '<table><tr><th>Param</th><th>Value</th></tr>' 
        if stage_type == "PipelineModel":
            values_html += '<tr><td>stages</td><td>%s</td>' % stage.stages
        else:
            for param in stage.params:
                values_html += '<tr><td>' + param.name + '</td>'
                try:
                    value_str = "%s" % stage.getOrDefault(param.name)
                except:
                    value_str = "None"
                wrapped_value_str = "<br>".join(textwrap.wrap(value_str))
                values_html += '<td>' + wrapped_value_str + '</td></tr>'
        values_html += '</table>'
        
        # create the row in the plot table with box positions and hover information
        plot_dd={
            'top':bottom_pos+box_height, 'bottom':bottom_pos, 'left':left_pos, 'right':left_pos+box_width,
            'color':box_color,
            'type':stage_type,
            'name':stage.uid,
            'text_x':left_pos+5, 'text_y':bottom_pos + 5,
            'x_start':left_pos+(box_width/2), 'y_start':bottom_pos+box_height+arrow_len, 'x_end':left_pos+(box_width/2), 'y_end':bottom_pos+box_height,
            'desc':values_html}
        if first_box:
            # we don't want an arrow (None seems to plot at 0,0 so plotting off the screen at -100)
            plot_dd['x_start']=-100
            plot_dd['y_start']=-100
            plot_dd['x_end']=-100
            plot_dd['y_end']=-100
        rows_list.append(plot_dd)
        bottom_pos = bottom_pos - box_height - arrow_len
        first_box = False    
    
    # now we can create the plot items - box, text and arrows
    plot_data = pd.DataFrame(rows_list) 
    source = ColumnDataSource(data=plot_data)
    boxes = plot.quad(source=source,top='top', bottom='bottom', left='left', right='right', color='color', line_color='black')
    plot.add_layout(LabelSet(source=source,x='text_x', y='text_y', text='name', render_mode='css', text_font_size='10pt', border_line_alpha=0, background_fill_alpha=0))
    plot.add_layout(Arrow(end=VeeHead(size=15), source=source, x_start='x_start', y_start='y_start', x_end='x_end', y_end='y_end'))
    
    # this is for showing in a Databricks Notebook - create HTML then show it
    html = file_html(plot, CDN, "Pipeline plot")
    displayHTML(html)
    
    # this is for showing in Zeppelin Notebook
    show(plot)
	
    #return the plot object
    return plot


