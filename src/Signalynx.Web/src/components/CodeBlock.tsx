import { useState } from 'react'

export function CodeBlock({ code, label }: { code: string; label: string }) {
  const [copied, setCopied] = useState(false)
  const copy = async () => {
    await navigator.clipboard.writeText(code)
    setCopied(true)
    window.setTimeout(() => setCopied(false), 1800)
  }

  return (
    <div className="code-block">
      <div className="code-toolbar">
        <span><i /> {label}</span>
        <button type="button" onClick={copy}>{copied ? 'Copied ✓' : 'Copy'}</button>
      </div>
      <pre><code>{code}</code></pre>
      <span className="sr-only" aria-live="polite">{copied ? `${label} copied` : ''}</span>
    </div>
  )
}
