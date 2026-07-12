export function SignalFlow() {
  return (
    <div className="signal-card" aria-label="Signalynx dispatch and messaging flow">
      <div className="signal-card__top"><span>LIVE ARCHITECTURE</span><span className="status-dot">READY</span></div>
      <div className="flow-row">
        <div className="flow-node flow-node--muted">Command<br /><small>or Query</small></div>
        <div className="flow-line"><i /></div>
        <div className="flow-node flow-node--core"><span className="mark">S</span>Signalynx</div>
        <div className="flow-line"><i /></div>
        <div className="flow-node flow-node--success">Handler<br /><small>ValueTask</small></div>
      </div>
      <div className="branch"><span /></div>
      <div className="flow-row flow-row--small">
        <div className="flow-node">Outbox</div><div className="flow-line"><i /></div>
        <div className="flow-node">Transport</div><div className="flow-line"><i /></div>
        <div className="flow-node">Consumer</div>
      </div>
      <div className="signal-stats">
        <span><b>01</b> typed dispatch</span><span><b>02</b> durable delivery</span><span><b>03</b> observable</span>
      </div>
    </div>
  )
}
