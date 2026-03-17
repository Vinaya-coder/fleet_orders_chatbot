import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import ChatMessage from './components/ChatBox';
import { Send, Trash2, History, Plus, LayoutPanelLeft, Database, Pencil, Check, Loader2, Square } from 'lucide-react';

function App() {
  const [query, setQuery] = useState('');
  const [messages, setMessages] = useState(() => {
    const saved = localStorage.getItem("chat_messages")
    return saved ? JSON.parse(saved) : []
  });
  const [loading, setLoading] = useState(false);
  const [showSql, setShowSql] = useState(false);
  const [chatHistory, setChatHistory] = useState([]);
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  const [editingHistoryId, setEditingHistoryId] = useState(null);
  const [tempTitle, setTempTitle] = useState('');
  const scrollRef = useRef(null);
  const abortControllerRef = useRef(null);

  useEffect(() => {
    scrollRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, loading]);

  useEffect(() => {
    const saved = JSON.parse(localStorage.getItem('fleetChatHistory') || '[]');
    setChatHistory(saved);
  }, []);

  useEffect(() => {
    localStorage.setItem("chat_messages", JSON.stringify(messages))
  }, [messages]);

  const saveToHistory = (newMessages) => {
    if (newMessages.length < 2) return;
    const firstUserMsg = newMessages.find(m => m.role === 'user')?.content || "New Chat";
    setChatHistory(prev => {
      const existingIdx = prev.findIndex(h => h.messages[0]?.content === newMessages[0]?.content);
      let updated;
      if (existingIdx !== -1) {
        updated = [...prev];
        updated[existingIdx] = { ...updated[existingIdx], messages: newMessages };
      } else {
        const newItem = {
          id: Date.now(),
          title: firstUserMsg.slice(0, 35) + (firstUserMsg.length > 35 ? '...' : ''),
          messages: newMessages,
          timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
        };
        updated = [newItem, ...prev].slice(0, 15);
      }
      localStorage.setItem('fleetChatHistory', JSON.stringify(updated));
      return updated;
    });
  };

  const handleStop = () => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
    }
  };

  const handleSend = async (overrideQuery = null) => {
    const activeQuery = overrideQuery || query;
    if (!activeQuery.trim() || loading) return;

    const controller = new AbortController();
    abortControllerRef.current = controller;

    const userMsg = { role: 'user', content: activeQuery };
    const updatedMessages = [...messages, userMsg];
    setMessages(updatedMessages);
    setQuery('');
    setLoading(true);

    try {
      // Build clean context from last Q&A pair only
      const lastUserMsg = messages.filter(m => m.role === 'user').slice(-1)[0];
      const lastAssistantMsg = messages.filter(m => m.role === 'assistant').slice(-1)[0];

      // Cancel words clear the context
      const cancelWords = ["leave it", "okay leave it", "nevermind", "forget it", "stop", "cancel"];
      const isCancelMessage = cancelWords.some(w => activeQuery.toLowerCase().includes(w));

      const cleanContext = (!isCancelMessage && lastUserMsg && lastAssistantMsg) ? {
        prev_question: lastUserMsg.content,
        prev_answer: lastAssistantMsg.summary || ''
      } : null;

      const { data } = await axios.post('http://127.0.0.1:8000/chat', {
        query: activeQuery,
        messages: [],
        context: cleanContext
      }, { signal: controller.signal });

      const aiMsg = {
        role: 'assistant',
        summary: data.summary,
        sql: data.sql || '',
        chart: data.chart || { chart_type: 'none' },
        context: data.context
      };

      const finalMessages = [...updatedMessages, aiMsg];
      setMessages(finalMessages);
      saveToHistory(finalMessages);
    } catch (err) {
      if (axios.isCancel(err) || err.name === 'CanceledError' || err.name === 'AbortError') {
        setMessages(updatedMessages.slice(0, -1));
        setQuery(activeQuery);
      } else {
        setMessages(prev => [...prev, {
          role: 'assistant',
          summary: "❌ Error: Failed to retrieve fleet data.",
          chart: { chart_type: 'none' }
        }]);
      }
    } finally {
      setLoading(false);
      abortControllerRef.current = null;
    }
  };

  const handleEditLastMessage = () => {
    const lastUserIndex = [...messages].reverse().findIndex(m => m.role === 'user');
    if (lastUserIndex === -1) return;
    const actualIndex = messages.length - 1 - lastUserIndex;
    setQuery(messages[actualIndex].content);
    setMessages(messages.slice(0, actualIndex));
  };

  const deleteHistoryItem = (id, e) => {
    e.stopPropagation();
    const updated = chatHistory.filter(h => h.id !== id);
    setChatHistory(updated);
    localStorage.setItem('fleetChatHistory', JSON.stringify(updated));
  };

  const clearAllHistory = () => {
    setChatHistory([]);
    setMessages([]);
    localStorage.removeItem('fleetChatHistory');
    localStorage.removeItem('chat_messages');
  };

  return (
    <div className="flex h-screen bg-[#0f172a] text-slate-200 overflow-hidden font-sans">
      {/* SIDEBAR */}
      <aside className={`${isSidebarOpen ? 'w-64' : 'w-20'} bg-[#1e293b] border-r border-slate-800 flex flex-col transition-all duration-300 shadow-2xl z-10`}>
        <div className="p-5 flex items-center justify-between">
          {isSidebarOpen && <h1 className="text-sm font-bold text-blue-400 flex items-center gap-2 truncate">🚛 Fleet Ops</h1>}
          <button onClick={() => setIsSidebarOpen(!isSidebarOpen)} className="p-2 hover:bg-slate-700 rounded-lg mx-auto">
            <LayoutPanelLeft size={18} className="text-slate-400" />
          </button>
        </div>

        <button
          onClick={() => { setMessages([]); localStorage.removeItem('chat_messages'); }}
          className="mx-4 mb-4 flex items-center justify-center gap-2 bg-blue-600 hover:bg-blue-500 text-white py-2.5 rounded-xl transition-all shadow-lg"
        >
          <Plus size={18} />
          {isSidebarOpen && <span className="text-sm font-medium">New Chat</span>}
        </button>

        {/* CHAT HISTORY SECTION */}
        <div className="flex-1 overflow-y-auto px-3 space-y-1 custom-scrollbar">
          {chatHistory.map((item) => (
            <div
              key={item.id}
              onClick={() => setMessages(item.messages)}
              className="group flex items-center gap-3 p-3 hover:bg-slate-800/80 rounded-xl cursor-pointer transition-all relative"
            >
              <History size={16} className="text-slate-500 shrink-0" />
              {isSidebarOpen && (
                <div className="flex-1 flex items-center min-w-0">
                  {editingHistoryId === item.id ? (
                    <div className="flex items-center gap-1 w-full" onClick={e => e.stopPropagation()}>
                      <input
                        className="bg-slate-900 border border-blue-500 text-[11px] px-2 py-1 rounded w-full outline-none"
                        value={tempTitle}
                        autoFocus
                        onChange={(e) => setTempTitle(e.target.value)}
                        onKeyDown={e => e.key === 'Enter' && (() => {
                          const updated = chatHistory.map(h => h.id === editingHistoryId ? { ...h, title: tempTitle } : h);
                          setChatHistory(updated);
                          localStorage.setItem('fleetChatHistory', JSON.stringify(updated));
                          setEditingHistoryId(null);
                        })()}
                      />
                      <Check size={14} className="text-green-500 cursor-pointer" onClick={(e) => {
                        const updated = chatHistory.map(h => h.id === editingHistoryId ? { ...h, title: tempTitle } : h);
                        setChatHistory(updated);
                        localStorage.setItem('fleetChatHistory', JSON.stringify(updated));
                        setEditingHistoryId(null);
                      }} />
                    </div>
                  ) : (
                    <span className="text-xs font-medium text-slate-300 truncate flex-1">{item.title}</span>
                  )}

                  {!editingHistoryId && (
                    <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                      <Pencil size={12} className="text-slate-500 hover:text-white" onClick={(e) => {
                        e.stopPropagation();
                        setEditingHistoryId(item.id);
                        setTempTitle(item.title);
                      }} />
                      <Trash2 size={12} className="text-slate-500 hover:text-red-400" onClick={(e) => deleteHistoryItem(item.id, e)} />
                    </div>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>

        {/* BOTTOM CONTROLS */}
        <div className="p-4 border-t border-slate-800 space-y-2 bg-[#1e293b]">
          <label className="flex items-center gap-3 cursor-pointer p-2 hover:bg-slate-700 rounded-lg transition text-xs text-slate-400">
            <input
              type="checkbox"
              checked={showSql}
              onChange={() => setShowSql(!showSql)}
              className="w-3.5 h-3.5 accent-blue-500"
            />
            {isSidebarOpen && <span>Show SQL Query</span>}
          </label>
          <button
            onClick={clearAllHistory}
            className="w-full flex items-center justify-center gap-2 p-2 text-[11px] text-red-400 border border-red-900/30 hover:bg-red-900/10 rounded-lg transition"
          >
            <Trash2 size={14} />
            {isSidebarOpen && <span>Clear All</span>}
          </button>
        </div>
      </aside>

      {/* MAIN CONTENT */}
      <main className="flex-1 flex flex-col min-w-0 bg-[#0f172a]">
        <div className="flex-1 overflow-y-auto p-6 scroll-smooth">
          <div className="max-w-4xl mx-auto space-y-8">
            {messages.length === 0 ? (
              <div className="flex flex-col items-center justify-center mt-40 opacity-20">
                <Database size={64} />
                <p className="mt-4 font-medium tracking-tight">Fleet Data Intelligence</p>
              </div>
            ) : (
              <>
                {messages.map((msg, i) => (
                  <ChatMessage key={i} message={msg} showSql={showSql} />
                ))}

                {loading && (
                  <div className="flex gap-3 items-center text-blue-400 animate-pulse ml-2">
                    <Loader2 size={16} className="animate-spin" />
                    <span className="text-xs font-bold uppercase tracking-widest">Analyzing fleet records...</span>
                  </div>
                )}

                {!loading && (
                  <div className="flex justify-center mt-4">
                    <button onClick={handleEditLastMessage} className="flex items-center gap-2 text-[10px] text-slate-500 hover:text-blue-400 bg-slate-800/40 px-3 py-1.5 rounded-full border border-slate-700">
                      <Pencil size={10} /> Edit last query
                    </button>
                  </div>
                )}
              </>
            )}
            <div ref={scrollRef} />
          </div>
        </div>

        {/* INPUT DOCK */}
        <div className="p-6 bg-gradient-to-t from-[#0f172a] via-[#0f172a] to-transparent">
          <div className="max-w-4xl mx-auto relative flex items-center bg-[#1e293b] border border-slate-700 rounded-2xl p-2 shadow-2xl focus-within:ring-1 focus-within:ring-blue-500/50 transition-all">
            <input
              className="flex-1 bg-transparent px-4 py-2 text-sm text-slate-200 outline-none placeholder-slate-500"
              placeholder="Ask about orders, vehicle status, or trends..."
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleSend()}
            />
            {loading ? (
              <button
                onClick={handleStop}
                title="Stop generating"
                className="p-3 bg-red-600 hover:bg-red-500 text-white rounded-xl transition-all shadow-lg animate-pulse"
              >
                <Square size={18} fill="white" />
              </button>
            ) : (
              <button
                onClick={() => handleSend()}
                disabled={loading}
                className="p-3 bg-blue-600 hover:bg-blue-500 disabled:bg-slate-800 text-white rounded-xl transition-all shadow-lg"
              >
                <Send size={18} />
              </button>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;