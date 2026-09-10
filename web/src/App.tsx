import { useEffect, useRef, useState } from 'react';
import './App.css';

interface LogEntry {
  Term: number;
  Index: number;
  Command: string; // base64-encoded JSON tick
}

interface NodeView {
  ID: number;
  Role: string;
  Term: number;
  LeaderID: number | null;
  CommitIndex: number;
  Logs: LogEntry[];
}

interface Tick {
  symbol: string;
  price: number;
  seq: number;
}

interface TimelineEvent {
  id: number;
  time: string;
  message: string;
}

function decodeTick(command: string): Tick | null {
  try {
    return JSON.parse(atob(command)) as Tick;
  } catch {
    return null;
  }
}

// The leader's log is the reference feed; while no leader is known yet
// (e.g. mid-election), fall back to whichever node has replicated the most.
function pickReferenceNode(nodes: NodeView[]): NodeView | undefined {
  return (
    nodes.find((n) => n.Role === 'leader') ??
    nodes.slice().sort((a, b) => b.Logs.length - a.Logs.length)[0]
  );
}

function diffEvents(prev: Map<number, NodeView>, next: NodeView[]): string[] {
  const messages: string[] = [];
  for (const node of next) {
    const before = prev.get(node.ID);
    if (!before) continue; // first snapshot for this node, nothing to compare
    if (before.Role !== 'leader' && node.Role === 'leader') {
      messages.push(`Node ${node.ID} became leader (term ${node.Term})`);
    }
    if (before.Role !== 'dead' && node.Role === 'dead') {
      messages.push(`Node ${node.ID} killed`);
    }
    if (before.Role === 'dead' && node.Role !== 'dead') {
      messages.push(`Node ${node.ID} revived`);
    }
  }
  return messages;
}

// Same-origin: the Go server serves this app's static build alongside the
// API, so these resolve correctly whether running locally or deployed.
const WS_PROTOCOL = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const WS_URL = `${WS_PROTOCOL}//${window.location.host}/ws`;
const API_BASE = '';
const MAX_TIMELINE_EVENTS = 20;
const MAX_TICKER_ENTRIES = 15;

async function killNode(id: number) {
  await fetch(`${API_BASE}/nodes/${id}/kill`, { method: 'POST' });
}

async function reviveNode(id: number) {
  await fetch(`${API_BASE}/nodes/${id}/revive`, { method: 'POST' });
}

function App() {
  const [nodes, setNodes] = useState<NodeView[]>([]);
  const [connected, setConnected] = useState(false);
  const [events, setEvents] = useState<TimelineEvent[]>([]);
  const prevNodesRef = useRef<Map<number, NodeView>>(new Map());
  const nextEventId = useRef(0);

  useEffect(() => {
    const ws = new WebSocket(WS_URL);
    ws.onopen = () => setConnected(true);
    ws.onclose = () => setConnected(false);
    ws.onmessage = (event) => {
      const views = JSON.parse(event.data) as NodeView[];

      const messages = diffEvents(prevNodesRef.current, views);
      if (messages.length > 0) {
        const time = new Date().toLocaleTimeString();
        setEvents((prevEvents) =>
          [
            ...messages.map((message) => ({ id: nextEventId.current++, time, message })),
            ...prevEvents,
          ].slice(0, MAX_TIMELINE_EVENTS),
        );
      }
      prevNodesRef.current = new Map(views.map((n) => [n.ID, n]));

      setNodes(views);
    };
    return () => ws.close();
  }, []);

  const referenceNode = pickReferenceNode(nodes);
  const tickerEntries = referenceNode
    ? referenceNode.Logs.slice(-MAX_TICKER_ENTRIES).map((entry) => decodeTick(entry.Command))
    : [];

  return (
    <div className="app">
      <h1>Raft Market Feed</h1>
      <p className={`status ${connected ? 'connected' : 'disconnected'}`}>
        {connected ? 'connected to cluster' : 'disconnected'}
      </p>
      <div className="nodes">
        {nodes.map((node) => {
          const lastEntry = node.Logs.at(-1);
          const lastTick = lastEntry ? decodeTick(lastEntry.Command) : null;
          return (
            <div key={node.ID} className={`node-card role-${node.Role}`}>
              <h2>Node {node.ID}</h2>
              <dl>
                <dt>Role</dt>
                <dd>{node.Role}</dd>
                <dt>Term</dt>
                <dd>{node.Term}</dd>
                <dt>Leader</dt>
                <dd>{node.LeaderID ?? '-'}</dd>
                <dt>Commit Index</dt>
                <dd>{node.CommitIndex}</dd>
                <dt>Log Length</dt>
                <dd>{node.Logs.length}</dd>
                <dt>Last Tick</dt>
                <dd>
                  {lastTick
                    ? `${lastTick.symbol} $${lastTick.price.toFixed(2)} (#${lastTick.seq})`
                    : '-'}
                </dd>
              </dl>
              {node.Role === 'dead' ? (
                <button
                  type="button"
                  className="node-action revive"
                  onClick={() => reviveNode(node.ID)}
                >
                  Revive
                </button>
              ) : (
                <button
                  type="button"
                  className="node-action kill"
                  onClick={() => killNode(node.ID)}
                >
                  Kill
                </button>
              )}
            </div>
          );
        })}
      </div>

      <div className="panels">
        <div className="panel ticker-panel">
          <h3>Market Feed{referenceNode ? ` — from Node ${referenceNode.ID}` : ''}</h3>
          <div className="ticker-tape">
            {tickerEntries.length === 0 && <span className="ticker-empty">waiting for ticks…</span>}
            {tickerEntries.map((tick, i) => {
              const prevTick = i > 0 ? tickerEntries[i - 1] : null;
              const direction =
                tick && prevTick ? (tick.price >= prevTick.price ? 'up' : 'down') : 'flat';
              return tick ? (
                <span key={tick.seq} className={`ticker-entry ${direction}`}>
                  {tick.symbol} ${tick.price.toFixed(2)}
                </span>
              ) : null;
            })}
          </div>
        </div>

        <div className="panel timeline-panel">
          <h3>Event Timeline</h3>
          <ul className="timeline">
            {events.length === 0 && <li className="timeline-empty">no events yet</li>}
            {events.map((event) => (
              <li key={event.id}>
                <span className="timeline-time">{event.time}</span> {event.message}
              </li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
}

export default App;
