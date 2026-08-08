import { useState } from 'react'
import { CodeBlock } from './components/CodeBlock'
import { CopyCommand } from './components/CopyCommand'
import { Header } from './components/Header'
import { SignalFlow } from './components/SignalFlow'
import { codeExamples, packageGroups, site } from './data/site'

const benefits = [
  ['↗', 'Strongly typed', 'Commands, queries, requests, notifications, and events with compile-time contracts.'],
  ['⚡', 'Lean hot path', 'ValueTask-based handlers with no MethodInfo.Invoke on the dispatch hot path.'],
  ['◇', 'Built to compose', 'Keep the core small, then add validation, logging, messaging, stores, or transports.'],
  ['⌁', 'NativeAOT ready', 'Optional source generation replaces runtime assembly scanning for trimmed applications.'],
]

const productionFeatures = [
  {
    index: '01',
    icon: '◎',
    title: 'Validate and observe every request',
    text: 'Pipeline behaviors wrap handlers in registration order, keeping validation, logging, authorization, metrics, and transactions outside business logic.',
    package: 'Signalynx.Validation · Signalynx.Logging',
    code: 'options.AddOpenBehavior(typeof(ValidationBehavior<,>));',
  },
  {
    index: '02',
    icon: '⌁',
    title: 'Export standard .NET telemetry',
    text: 'Signalynx emits activities, counters, failures, and duration measurements for dispatch, publish, message handling, retries, and dead letters.',
    package: 'Signalynx.Core · Signalynx.Messaging',
    code: 'options.EnableDiagnostics = true;',
  },
  {
    index: '03',
    icon: '◇',
    title: 'Generate registrations for NativeAOT',
    text: 'Compile-time registration avoids reflection-based assembly scanning and produces handler descriptors and a static dispatch map.',
    package: 'Signalynx.SourceGeneration',
    code: 'services.AddSignalynxGenerated(options => { ... });',
  },
]

const appArchitectures = [
  {
    icon: '▱',
    title: 'Web APIs and modular monoliths',
    label: 'IN-PROCESS APPLICATIONS',
    text: 'Dispatch commands and queries to application handlers. Use notifications and domain events when several local modules need to react.',
    packages: ['DependencyInjection', 'Validation'],
    flow: ['Endpoint', 'Dispatch', 'Handler'],
  },
  {
    icon: '◴',
    title: 'Worker services',
    label: 'BACKGROUND WORK',
    text: 'Consume durable messages in hosted workers with retries, scheduling, inbox deduplication, and dead-letter recovery.',
    packages: ['Messaging', 'Store', 'Transport'],
    flow: ['Queue', 'Receiver', 'Worker'],
  },
  {
    icon: '⌘',
    title: 'Microservices and event-driven systems',
    label: 'SERVICE BOUNDARIES',
    text: 'Use local dispatch inside each service and durable integration messages across services, retries, delays, and broker boundaries.',
    packages: ['DependencyInjection', 'Messaging', 'Transport'],
    flow: ['Service A', 'Broker', 'Service B'],
  },
  {
    icon: '△',
    title: 'NativeAOT and containers',
    label: 'LEAN DEPLOYMENT',
    text: 'Replace runtime handler scanning with generated registrations for trimmed executables, startup-sensitive workloads, and compact containers.',
    packages: ['SourceGeneration', 'Core', 'Abstractions'],
    flow: ['Compile', 'Generate', 'Run'],
  },
]

function SectionHeading({ eyebrow, title, text }: { eyebrow: string; title: string; text: string }) {
  return <div className="section-heading"><span className="eyebrow">{eyebrow}</span><h2>{title}</h2><p>{text}</p></div>
}

function App() {
  const [tab, setTab] = useState<'command' | 'query' | 'notification'>('command')

  return (
    <>
      <a className="skip-link" href="#main">Skip to content</a>
      <div className="ambient" aria-hidden="true" />
      <div className="signal-rail signal-rail--left" aria-hidden="true"><i /><i /><i /></div>
      <div className="signal-rail signal-rail--right" aria-hidden="true"><i /><i /></div>
      <Header />
      <main id="main">
        <section className="hero" id="top">
          <div className="hero-copy">
            <div className="system-tag"><span>SLX / CORE</span><b>NETWORK ONLINE</b></div>
            <span className="eyebrow"><i /> {site.eyebrow}</span>
            <h1>{site.headline}</h1>
            <p className="hero-lead">{site.description}</p>
            <div className="badges"><span>.NET 8 · 9 · 10</span><span>MIT</span><span>Async only</span></div>
            <div className="hero-actions"><a className="button" href="#quick-start">Get started <span>→</span></a><a className="button button--ghost" href={site.githubUrl} target="_blank" rel="noreferrer">View source <span className="github-symbol" aria-hidden="true">⌘</span></a><a className="button button--ghost" href={site.nugetUrl} target="_blank" rel="noreferrer">NuGet ↗</a></div>
            <div id="install"><CopyCommand value={site.diInstall} /></div>
          </div>
          <SignalFlow />
        </section>

        <section className="section" id="overview">
          <SectionHeading eyebrow="01 · What is Signalynx?" title="One toolkit for application dispatch and reliable messaging." text="Signalynx is a strongly typed .NET toolkit. It handles work inside your application with a mediator and moves work safely across time or service boundaries with durable messaging." />
          <div className="problem-statement">
            <span>THE PROBLEM IT SOLVES</span>
            <h3>Application code needs two very different kinds of communication.</h3>
            <p>Some work needs an immediate result in the current process. Other work must survive retries, restarts, delays, or delivery to another service. Signalynx supports both without forcing broker infrastructure into ordinary local dispatch.</p>
          </div>
          <div className="path-grid">
            <article className="path-card path-card--cyan"><span className="path-number">01</span><div className="path-icon">↯</div><h3>In-process mediator</h3><p>Immediate, typed dispatch for commands, queries, requests, notifications, and domain events.</p><ul><li>Pipeline behaviors</li><li>Sequential or parallel publishers</li><li>Bulk processing</li><li>Diagnostics</li></ul></article>
            <article className="path-card path-card--violet"><span className="path-number">02</span><div className="path-icon">⌁</div><h3>Durable messaging</h3><p>Asynchronous delivery for work that must survive delay, retries, restarts, or service boundaries.</p><ul><li>Inbox and outbox</li><li>Retries and scheduling</li><li>Dead letters and replay</li><li>Pluggable providers</li></ul></article>
          </div>
          <div className="benefit-grid">{benefits.map(([icon, title, text]) => <article key={title}><span>{icon}</span><h3>{title}</h3><p>{text}</p></article>)}</div>
        </section>

        <section className="section architecture" id="architecture">
          <SectionHeading eyebrow="02 · Which API should I use?" title="Choose by what the work needs." text="You do not need durable messaging for every operation. Select the smallest execution model that provides the delivery behavior you need." />
          <div className="choice-grid">
            <article><span>Need a result now?</span><h3>Dispatch</h3><p>Use a command, query, or request for one typed handler in the current process.</p><code>DispatchAsync()</code></article>
            <article><span>Multiple local reactions?</span><h3>Publish</h3><p>Use a notification or domain event when several in-process handlers should react.</p><code>PublishAsync()</code></article>
            <article><span>Retries or another service?</span><h3>Enqueue</h3><p>Use durable messaging when work may be delayed, retried, or delivered elsewhere.</p><code>EnqueueAsync()</code></article>
            <article><span>Many local items?</span><h3>Bulk API</h3><p>Use bounded sequential or parallel processing when per-item mediator semantics add no value.</p><code>ProcessParallelAsync()</code></article>
          </div>
          <h3 className="flow-heading">How the two execution paths work</h3>
          <div className="architecture-board">
            <div className="architecture-lane"><span className="lane-label">IMMEDIATE</span>{['Application', 'ISignalynx', 'Pipeline', 'Handler'].map((item, i) => <div className="architecture-step" key={item}><b>{item}</b><small>{['Command / Query', 'Typed dispatch', 'Validation · Logging', 'ValueTask result'][i]}</small></div>)}</div>
            <div className="architecture-lane architecture-lane--durable"><span className="lane-label">DURABLE</span>{['Application', 'Message Bus', 'Outbox', 'Transport', 'Inbox', 'Handler'].map((item, i) => <div className="architecture-step" key={item}><b>{item}</b><small>{['Enqueue', 'Envelope', 'Persist', 'Deliver', 'Deduplicate', 'Process'][i]}</small></div>)}</div>
            <div className="providers"><span>SQL Server</span><span>PostgreSQL</span><span>RabbitMQ</span><span>Azure Service Bus</span><span>Amazon SQS</span><span>Kafka</span></div>
          </div>
          <p className="rule"><span>Rule of thumb</span> Need an immediate result? Dispatch in-process. Need delay, retry, durability, or cross-service delivery? Use messaging.</p>
        </section>

        <section className="section app-types" id="app-types">
          <SectionHeading eyebrow="03 · Does it support my application?" title="From Web APIs to event-driven systems." text="Signalynx works across common .NET application styles. Start with typed in-process dispatch, then add durability only at boundaries that need it." />
          <div className="architecture-fit-grid">
            {appArchitectures.map((item, index) => <article className="architecture-fit-card" key={item.title}>
              <div className="fit-card-top"><span className="fit-icon">{item.icon}</span><small>{String(index + 1).padStart(2, '0')} / 04</small></div>
              <span className="fit-label">{item.label}</span>
              <h3>{item.title}</h3>
              <p>{item.text}</p>
              <div className="mini-flow" aria-label={`${item.title} flow`}>{item.flow.map((step, stepIndex) => <span key={step}>{step}{stepIndex < item.flow.length - 1 && <i aria-hidden="true">→</i>}</span>)}</div>
              <div className="fit-packages">{item.packages.map(pkg => <span key={pkg}>{pkg}</span>)}</div>
            </article>)}
          </div>
          <div className="architecture-principle">
            <span className="principle-mark">S</span>
            <div><span className="eyebrow">The Signalynx principle</span><h3>Local when possible. Durable when necessary.</h3></div>
            <p>A broker is not required for ordinary application dispatch. ASP.NET Core is not required by the libraries. Infrastructure remains an explicit choice at the composition root.</p>
          </div>
        </section>

        <section className="section" id="packages">
          <SectionHeading
            eyebrow="04 · Which packages should I install?"
            title="Choose packages based on where the code runs."
            text="Use Signalynx.DependencyInjection in Microsoft DI host applications, Signalynx.Abstractions in shared class libraries, or Signalynx.Core directly when you want the mediator runtime without the DI integration. Add validation, messaging, stores, and transports only where required."
          />
          <div className="install-paths">
            <article><span>MAIN RUNTIME</span><h3>Use Signalynx directly</h3><p>Install Core when you want the mediator runtime and will compose handlers yourself.</p><CopyCommand value={site.install} /></article>
            <div className="path-or">OR</div>
            <article><span>MICROSOFT DI APPLICATION</span><h3>Use AddSignalynx registration</h3><p>Install the DI integration. NuGet automatically includes Core and Abstractions.</p><CopyCommand value={site.diInstall} /></article>
          </div>
          <div className="package-grid">{packageGroups.map(group => <article className="package-group" key={group.name}><div><span className="package-kicker">{group.name}</span><h3>{group.description}</h3></div>{group.packages.map(pkg => <div className="package" key={pkg.name}><div><div className="package-title"><h4>{pkg.name}</h4>{pkg.badge && <span>{pkg.badge}</span>}</div><p>{pkg.purpose}</p>{pkg.note && <span className="warning">{pkg.note}</span>}</div><CopyCommand value={`dotnet add package ${pkg.name}`} compact label={`Copy ${pkg.name} install command`} /></div>)}</article>)}</div>
        </section>

        <section className="section quick-start" id="quick-start">
          <SectionHeading eyebrow="05 · How do I start?" title="From command to result in three steps." text="This example uses Microsoft DI. Install the integration package, which automatically includes the Core runtime, then define, register, and dispatch your command." />
          <div className="start-install">
            <div><span>INSTALL FOR THIS EXAMPLE</span><small>Includes Signalynx.Core automatically</small></div>
            <CopyCommand value={site.diInstall} />
          </div>
          <div className="step"><span>01</span><div><h3>Define the command and handler</h3><p>Handlers are strongly typed, asynchronous, and cancellation-aware.</p></div></div><CodeBlock label="CreateOrder.cs" code={codeExamples.command} />
          <div className="step"><span>02</span><div><h3>Register Signalynx</h3><p>Discover handlers once and compose optional pipeline behaviors.</p></div></div><CodeBlock label="Program.cs" code={codeExamples.registration} />
          <div className="step"><span>03</span><div><h3>Dispatch</h3><p>Choose the API that expresses the message intent.</p></div></div>
          <div className="tabs" role="tablist" aria-label="Dispatch examples">{(['command', 'query', 'notification'] as const).map(name => <button role="tab" aria-selected={tab === name} key={name} onClick={() => setTab(name)}>{name}</button>)}</div>
          <CodeBlock label={`${tab}.cs`} code={codeExamples[tab === 'command' ? 'dispatch' : tab]} />
        </section>

        <section className="section production" id="production">
          <SectionHeading eyebrow="Production readiness" title="Make behavior observable, predictable, and recoverable." text="Production readiness is more than dispatch speed. Apply policies consistently, export operational signals, choose deployment-safe registration, and design asynchronous work for failure." />
          <div className="production-grid">{productionFeatures.map(feature => <article key={feature.title}><div className="feature-top"><span>{feature.icon}</span><small>{feature.index}</small></div><h3>{feature.title}</h3><p>{feature.text}</p><div className="feature-package"><b>PACKAGE</b><span>{feature.package}</span></div><code>{feature.code}</code></article>)}</div>
          <div className="production-checklist">
            <div><span className="eyebrow">Before production</span><h3>Signalynx provides the runtime. Your deployment completes the reliability story.</h3></div>
            <ul>
              <li><span>01</span>Use persistent inbox, outbox, and dead-letter stores.</li>
              <li><span>02</span>Enlist business data and outbox writes in the same database connection and transaction.</li>
              <li><span>03</span>Make message handlers idempotent and use stable wire names.</li>
              <li><span>04</span>Alert on failures, retries, dead-letter growth, and handler latency.</li>
            </ul>
          </div>
        </section>

        <section className="section messaging" id="messaging">
          <div className="messaging-copy"><span className="eyebrow">06 · How does production messaging work?</span><h2>Durability without dragging a broker into your mediator.</h2><p>Enqueue or schedule work through the message bus. Hosted workers move messages from an outbox to your selected transport, then protect consumers with inbox deduplication, bounded retries, and dead letters.</p><div className="production-note"><b>Production boundary</b><p>The in-memory provider loses messages when the process exits. Production deployments require persistent stores, a real transport, transaction enlistment for a true outbox, and idempotent handlers.</p></div></div>
          <CodeBlock label="MessagingSetup.cs" code={codeExamples.messaging} />
        </section>

        <section className="final-cta">
          <span className="brand-mark brand-mark--large"><i /><b>S</b><i /></span><p className="eyebrow">START WHERE YOU ARE</p><h2>Start with the mediator.<br />Add durability when the boundary demands it.</h2><CopyCommand value={site.diInstall} /><div className="cta-links"><a href={site.githubUrl} target="_blank" rel="noreferrer">Explore the repository <span>↗</span></a><a href={site.nugetUrl} target="_blank" rel="noreferrer">View on NuGet <span>↗</span></a></div>
        </section>
      </main>
      <footer><a className="brand" href="#top"><span className="brand-mark"><i /><b>S</b><i /></span>Signalynx</a><p>Typed mediator and durable messaging for .NET 8–10.</p><div><span>MIT License</span><a href={site.githubUrl} target="_blank" rel="noreferrer">GitHub ↗</a><a href={site.nugetUrl} target="_blank" rel="noreferrer">NuGet ↗</a></div></footer>
    </>
  )
}

export default App