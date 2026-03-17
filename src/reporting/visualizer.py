# src/reporting/visualizer.py
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Optional, List, Dict, Any
import plotly.io as pio

pio.renderers.default = "iframe"

def create_visualization(
    data: List[Dict[str, Any]],
    chart_type: str,
    x_axis: Optional[str] = None,
    y_axis: Optional[str] = None,
    title: str = "Query Results"
) -> Optional[Dict[str, Any]]:
    """
    Generate professional Plotly visualization based on data and chart type.
    Returns chart JSON for frontend rendering with clear labels and formatting.
    """
    
    if not data or chart_type == "none":
        return {"chart_type": "none"}
    
    try:
        df = pd.DataFrame(data)
        
        # Format title for display
        display_title = title[:60] if len(title) > 60 else title
        
        # --- BAR CHART ---
        if chart_type == "bar" and x_axis and y_axis:
            # Sort by y_axis descending for better visualization
            if y_axis in df.columns and pd.api.types.is_numeric_dtype(df[y_axis]):
                df_sorted = df.sort_values(y_axis, ascending=False)
            else:
                df_sorted = df
            
            fig = go.Figure(data=[
                go.Bar(
                    x=df_sorted[x_axis],
                    y=df_sorted[y_axis],
                    text=df_sorted[y_axis],
                    textposition="outside",  # Show values on top of bars
                    hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br><b>{y_axis.replace('_', ' ').title()}</b>: %{{y}}<extra></extra>",
                    marker=dict(
                        color=df_sorted[y_axis],
                        colorscale="Blues",
                        showscale=len(df) > 5,
                        colorbar=dict(title=y_axis.replace("_", " ").title()) if len(df) > 5 else None
                    )
                )
            ])
            
            fig.update_layout(
                title=dict(text=display_title, font=dict(size=18, color="#1f77b4")),
                xaxis_title=x_axis.replace("_", " ").title(),
                yaxis_title=y_axis.replace("_", " ").title(),
                height=450,
                template="plotly_white",
                hovermode="x unified",
                showlegend=False,
                margin=dict(l=50, r=50, t=80, b=50)
            )
            
            return {
                "chart_type": "bar",
                "figure": fig,
                "data": data
            }
        
        # --- LINE CHART (Time series) ---
        elif chart_type == "line" and x_axis and y_axis:
            fig = go.Figure(data=[
                go.Scatter(
                    x=df[x_axis],
                    y=df[y_axis],
                    mode='lines+markers',
                    line=dict(color='#1f77b4', width=3),
                    marker=dict(size=8, color='#ff7f0e'),
                    fill='tozeroy',
                    fillcolor='rgba(31, 119, 180, 0.2)',
                    hovertemplate=f"<b>{x_axis.replace('_', ' ').title()}</b>: %{{x}}<br><b>{y_axis.replace('_', ' ').title()}</b>: %{{y}}<extra></extra>"
                )
            ])
            
            fig.update_layout(
                title=dict(text=display_title, font=dict(size=18, color="#1f77b4")),
                xaxis_title=x_axis.replace("_", " ").title(),
                yaxis_title=y_axis.replace("_", " ").title(),
                height=450,
                template="plotly_white",
                hovermode="x unified",
                showlegend=False,
                margin=dict(l=50, r=50, t=80, b=50)
            )
            
            return {
                "chart_type": "line",
                "figure": fig,
                "data": data
            }
        
        # --- PIE CHART (Distributions) ---
        elif chart_type == "pie" and x_axis and y_axis:
            # Sort by value descending for pie consistency
            if y_axis in df.columns and pd.api.types.is_numeric_dtype(df[y_axis]):
                df_sorted = df.sort_values(y_axis, ascending=False)
            else:
                df_sorted = df
            
            fig = go.Figure(data=[
                go.Pie(
                    labels=df_sorted[x_axis],
                    values=df_sorted[y_axis],
                    textposition="inside",
                    textinfo="label+percent",
                    hovertemplate="<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>",
                    marker=dict(line=dict(color="black", width=2))
                )
            ])
            
            fig.update_layout(
                title=dict(text=display_title, font=dict(size=18, color="#1f77b4")),
                height=450,
                showlegend=True,
                margin=dict(l=20, r=20, t=80, b=20)
            )
            
            return {
                "chart_type": "pie",
                "figure": fig,
                "data": data
            }
        
        # --- TABLE CHART (Fallback for complex data) ---
        elif chart_type == "table":
            fig = go.Figure(data=[go.Table(
                header=dict(
                    values=[f"<b>{col.replace('_', ' ').title()}</b>" for col in df.columns],
                    fill_color="#1f77b4",
                    font=dict(color="white", size=12),
                    align="left",
                    height=30
                ),
                cells=dict(
                    values=[df[col].astype(str) for col in df.columns],
                    fill_color=[["#f0f0f0" if i % 2 == 0 else "white" for i in range(len(df))] for _ in df.columns],
                    align="left",
                    height=25,
                    font=dict(color="black", size=11)
                )
            )])
            fig.update_layout(
                title=dict(text=display_title, font=dict(size=18, color="#1f77b4")),
                height=max(400, min(len(df) * 30 + 100, 800)),
                margin=dict(l=20, r=20, t=80, b=20)
            )
            return {
                "chart_type": "table",
                "figure": fig,
                "spec": pio.to_json(fig),
                "data": data
            }
        
        return {"chart_type": "none"}
    
    except Exception as e:
        print(f"❌ Visualization error: {e}")
        return {"chart_type": "none"}


def create_visualizer():
    """Legacy function for compatibility - returns visualization function."""
    return create_visualization