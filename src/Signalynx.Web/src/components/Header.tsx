import { useEffect, useState } from 'react'
import { site } from '../data/site'

const links = ['overview', 'architecture', 'app-types', 'packages', 'quick-start', 'messaging']

export function Header() {
  const [open, setOpen] = useState(false)
  const [active, setActive] = useState('overview')

  useEffect(() => {
    const observer = new IntersectionObserver(
      entries => entries.forEach(entry => entry.isIntersecting && setActive(entry.target.id)),
      { rootMargin: '-35% 0px -55%' },
    )
    links.forEach(id => { const node = document.getElementById(id); if (node) observer.observe(node) })
    return () => observer.disconnect()
  }, [])

  return (
    <header className="header">
      <a className="brand" href="#top" aria-label="Signalynx home"><span className="brand-mark"><i /><b>S</b><i /></span>Signalynx</a>
      <button className="menu-button" type="button" aria-expanded={open} aria-controls="main-nav" onClick={() => setOpen(!open)}>
        <span /><span /><span /><span className="sr-only">Toggle navigation</span>
      </button>
      <nav id="main-nav" className={open ? 'nav nav--open' : 'nav'} aria-label="Main navigation">
        {links.map(id => <a key={id} className={active === id ? 'active' : ''} href={`#${id}`} onClick={() => setOpen(false)}>{id.replace('-', ' ')}</a>)}
        <a className="repo-link" href={site.githubUrl} target="_blank" rel="noreferrer" aria-label="View Signalynx source code on GitHub">GitHub <span aria-hidden="true">↗</span></a>
        <a className="button button--small" href="#install" onClick={() => setOpen(false)}>Install <span aria-hidden="true">↗</span></a>
      </nav>
    </header>
  )
}
