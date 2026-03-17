import React, { useState } from 'react';
import ReactMarkdown from 'react-markdown';
import Plot from 'react-plotly.js';
import { Database, Bot, User } from 'lucide-react';

const ChatBox = ({ message, showSql }) => {
  const isUser = message.role === 'user';
  const [expanded, setExpanded] = useState(false);

  // Detect "Top N: item1, item2, ... (and X more)||MORE||item6, item7, ..." pattern and make it expandable
  const renderSummary = (text) => {
    if (!text) return text;

    // Split on our backend-embedded marker
    const markerIdx = text.indexOf('||MORE||');
    if (markerIdx === -1) return text; // No hidden content, render as-is

    const visiblePart = text.slice(0, markerIdx);   // e.g. "Found 8 table_name. Top 5: a, b, c (and 3 more)"
    const hiddenPart = text.slice(markerIdx + 8);   // e.g. "f, g, h"

    if (expanded) {
      // Show everything: visible + hidden, no "(and N more)" button
      const fullText = visiblePart.replace(/\(and \d+ more\)/, '') + ', ' + hiddenPart;
      return fullText;
    }

    // Show visible part, replace "(and N more)" with clickable span
    const parts = visiblePart.split(/(\(and \d+ more\))/);
    return parts.map((part, idx) => {
      if (/^\(and \d+ more\)$/.test(part)) {
        return (
          <span
            key={idx}
            onClick={() => setExpanded(true)}
            style={{
              color: '#60a5fa',
              cursor: 'pointer',
              textDecoration: 'underline',
              fontWeight: 600,
            }}
            title="Click to show all"
          >
            {part}
          </span>
        );
      }
      return part;
    });
  };

  // Helper to parse the Plotly JSON safely
  const renderChart = (chartData) => {
    if (!chartData || chartData.chart_type === 'none' || !chartData.spec) return null;

    try {
      // Parse the JSON string sent from backend
      let plotlyFig;
      if (typeof chartData.spec === 'string') {
        plotlyFig = JSON.parse(chartData.spec);
      } else {
        plotlyFig = chartData.spec;
      }

      // plotlyFig has data, layout, config properties
      return (
        <div className="bg-slate-800/50 rounded-xl p-4 mt-4 shadow-inner border border-slate-700">
          <Plot
            data={plotlyFig.data || []}
            layout={{
              ...plotlyFig.layout,
              autosize: true,
              paper_bgcolor: 'rgba(15, 23, 42, 0.5)',
              plot_bgcolor: 'rgba(30, 41, 59, 0)',
              margin: { t: 40, b: 40, l: 40, r: 40 },
              font: { color: '#cbd5e1', family: 'Inter, system-ui' }
            }}
            useResizeHandler={true}
            style={{ width: "100%", height: "400px" }}
            config={{ responsive: true, displayModeBar: false }}
          />
        </div>
      );
    } catch (e) {
      console.error("Failed to render chart:", e, chartData);
      return <p className="text-red-400 text-xs mt-2 italic">📊 Visualization unavailable</p>;
    }
  };

  return (
    <div className={`flex gap-4 ${isUser ? 'flex-row-reverse' : 'flex-row'}`}>
      {/* Avatar Icons */}
      <div className={`w-8 h-8 rounded-full flex items-center justify-center shrink-0 shadow-lg ${
        isUser ? 'bg-blue-600' : 'bg-slate-700'
      }`}>
        {isUser ? <User size={18} /> : <Bot size={18} />}
      </div>

      {/* Message Content */}
      <div className={`flex flex-col max-w-[85%] ${isUser ? 'items-end' : 'items-start'}`}>
        <div className={`p-4 rounded-2xl shadow-sm ${
          isUser 
            ? 'bg-blue-600 text-white rounded-tr-none' 
            : 'bg-slate-800 border border-slate-700 text-slate-200 rounded-tl-none'
        }`}>
          {isUser ? (
            <p className="whitespace-pre-wrap">{message.content}</p>
          ) : (
            <div className="space-y-3">
              {/* Natural Language Summary */}
              <div className="prose prose-invert prose-sm max-w-none">
                {message.summary && message.summary.includes('||MORE||') ? (
                  // Render with clickable "(and N more)" span or expanded view
                  <p className="whitespace-pre-wrap text-sm leading-relaxed">
                    {renderSummary(message.summary)}
                  </p>
                ) : (
                  <ReactMarkdown>{message.summary}</ReactMarkdown>
                )}
              </div>

              {/* Conditional SQL Logic (The "How I found this" part) */}
              {showSql && message.sql && (
                <div className="mt-4 border-t border-slate-700 pt-3">
                  <div className="flex items-center gap-2 text-xs font-semibold text-slate-400 mb-2 uppercase tracking-wider">
                    <Database size={12} /> Generated SQL Query
                  </div>
                  <pre className="bg-black/40 p-3 rounded-lg text-[11px] font-mono text-emerald-400 overflow-x-auto border border-emerald-900/30">
                    {message.sql}
                  </pre>
                </div>
              )}

              {/* Data Visualization */}
              {renderChart(message.chart)}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default ChatBox;