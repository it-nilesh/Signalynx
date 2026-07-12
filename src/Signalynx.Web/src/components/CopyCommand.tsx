import { useEffect, useState } from 'react'

type Props = { value: string; compact?: boolean; label?: string }

export function CopyCommand({ value, compact = false, label = 'Copy' }: Props) {
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    if (!copied) return
    const timer = window.setTimeout(() => setCopied(false), 1800)
    return () => window.clearTimeout(timer)
  }, [copied])

  const copy = async () => {
    await navigator.clipboard.writeText(value)
    setCopied(true)
  }

  return (
    <div className={`copy-command ${compact ? 'copy-command--compact' : ''}`}>
      <code>{value}</code>
      <button type="button" onClick={copy} aria-label={`${label}: ${value}`}>
        <span aria-hidden="true">{copied ? '✓' : '⧉'}</span>
        {compact ? '' : copied ? 'Copied' : label}
      </button>
      <span className="sr-only" aria-live="polite">{copied ? 'Copied to clipboard' : ''}</span>
    </div>
  )
}
